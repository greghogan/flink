/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.library.paths.unweighted;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.result.BinaryResultBase;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.paths.unweighted.AllPairsDistance.Result;
import org.apache.flink.graph.library.paths.unweighted.Functions.Convergence;
import org.apache.flink.graph.utils.GraphUtils.NonForwardingIdentityMapper;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Compute the distance between all pairs of vertices in the Graph, which is
 * the length of the shortest path. No result is returned if no path exists
 * between the source and target vertices.
 *
 * <p>In an undirected graph, for each result (u, v, distance) there exists a
 * matching result (v, u, distance). This is not guaranteed in a directed
 * graph as (v) may be reachable from (u) but not (u) from (v).
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class AllPairsDistance<K extends Value & Comparable<K>, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Result<K>> {

	private static final String NEW_LONGEST_DISTANCES = "new longest distances";

	@Override
	public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// u, v, 1
		DataSet<Tuple3<K, K, IntValue>> edges = input
			.getEdges()
			.map(new InitializeEdges<K, EV>())
				.name("Initialize edges");

		// u, v, d
		IterativeDataSet<Tuple3<K, K, IntValue>> iterative = edges
			.iterate(Integer.MAX_VALUE);

		// u, v, d where d is the maximum possible distance for this iteration
		DataSet<Tuple3<K, K, IntValue>> maxDistances = iterative
			.filter(new MaxDistances<K>())
				.name("Max distances");

		// u, v, d for all new and old paths
		DataSet<Tuple3<K, K, IntValue>> distances = iterative
			.join(maxDistances)
			.where(1)
			.equalTo(0)
			.with(new ConnectDistances<K>())
				.name("Connect distances")
			.union(iterative)
				.name("All paths");

		// u, v, d where d is the minimum distance between u and v
		DataSet<Tuple3<K, K, IntValue>> shortestDistances = distances
			.map(new NonForwardingIdentityMapper<Tuple3<K, K, IntValue>>())
				.name("Identity")
			.groupBy(0, 1)
			.reduce(new ShortestDistances<K>())
			.setCombineHint(CombineHint.HASH)
				.name("Shortest distances")
			.filter(new CountMaxDistances<K>())
				.name("Count max distances");

		iterative.registerAggregationConvergenceCriterion(NEW_LONGEST_DISTANCES, new LongSumAggregator(), new Convergence());

		return iterative
			.closeWith(shortestDistances)
			.map(new TranslateResult<K>())
				.name("Result");
	}

	/**
	 * Convert edges to tuples and initialize with a distance of '1'.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	@ForwardedFields("0; 1")
	private static final class InitializeEdges<T extends Value, ET>
	implements MapFunction<Edge<T, ET>, Tuple3<T, T, IntValue>> {
		private Tuple3<T, T, IntValue> output = new Tuple3<>(null, null, new IntValue(1));

		@Override
		public Tuple3<T, T, IntValue> map(Edge<T, ET> edge)
				throws Exception {
			output.f0 = edge.f0;
			output.f1 = edge.f1;
			return output;
		}
	}

	/**
	 * Pass through when the distance is the largest possible for this
	 * iteration. This maximum distance grows exponentially, starting at '1'
	 * for the initial edges and doubling each superstep.
	 *
	 * @param <T> ID type
	 */
	private static final class MaxDistances<T>
	extends RichFilterFunction<Tuple3<T, T, IntValue>> {
		private int lengthFilter;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			int superstep = getIterationRuntimeContext().getSuperstepNumber();
			lengthFilter = 1 << (superstep - 1);
		}

		@Override
		public boolean filter(Tuple3<T, T, IntValue> path)
				throws Exception {
			return path.f2.getValue() == lengthFilter;
		}
	}

	/**
	 * Connect two paths and sum the distances.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFieldsFirst("0")
	@ForwardedFieldsSecond("1")
	private class ConnectDistances<T extends Comparable<T>>
	implements FlatJoinFunction<Tuple3<T, T, IntValue>, Tuple3<T, T, IntValue>, Tuple3<T, T, IntValue>> {
		private Tuple3<T, T, IntValue> output = new Tuple3<>(null, null, new IntValue());

		@Override
		public void join(Tuple3<T, T, IntValue> first, Tuple3<T, T, IntValue> second, Collector<Tuple3<T, T, IntValue>> out)
				throws Exception {
			// ignore loops
			if (first.f0.compareTo(second.f1) != 0) {
				output.f0 = first.f0;
				output.f1 = second.f1;
				output.f2.setValue(first.f2.getValue() + second.f2.getValue());
				out.collect(output);
			}
		}
	}

	/**
	 * Reduce to the small distance between vertices.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0; 1")
	private class ShortestDistances<T>
	implements ReduceFunction<Tuple3<T, T, IntValue>> {
		@Override
		public Tuple3<T, T, IntValue> reduce(Tuple3<T, T, IntValue> value1, Tuple3<T, T, IntValue> value2)
				throws Exception {
			return (value1.f2.getValue() <= value2.f2.getValue()) ? value1 : value2;
		}
	}

	/**
	 *
	 * @param <T> ID type
	 */
	private static class CountMaxDistances<T>
	extends RichFilterFunction<Tuple3<T, T, IntValue>> {
		private int maxLength;

		private int maxLengthCount;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			int superstep = getIterationRuntimeContext().getSuperstepNumber();

			// each iteration doubles the length of the largest path
			// and the first iteration processes edges joined with edges
			maxLength = 1 << superstep;
			maxLengthCount = 0;
		}

		@Override
		public void close()
			throws Exception {
			super.close();

			LongSumAggregator agg = getIterationRuntimeContext().getIterationAggregator(NEW_LONGEST_DISTANCES);
			agg.aggregate(maxLengthCount);
		}

		@Override
		public boolean filter(Tuple3<T, T, IntValue> value)
				throws Exception {
			if (value.f2.getValue() == maxLength) {
				maxLengthCount++;
			}

			return true;
		}
	}

	/**
	 * Translate to the result type.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0->vertexId0; 1->vertexId1; 2->distance")
	private static class TranslateResult<T>
	implements MapFunction<Tuple3<T, T, IntValue>, Result<T>> {
		private Result<T> output = new Result<>();

		@Override
		public Result<T> map(Tuple3<T, T, IntValue> value)
				throws Exception {
			output.setVertexId0(value.f0);
			output.setVertexId1(value.f1);
			output.setDistance(value.f2);
			return output;
		}
	}

	/**
	 * Wraps the results from the AllPairsDistance algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends BinaryResultBase<T>
	implements PrintableResult {
		private IntValue distance;

		/**
		 * Get the distance between vertices.
		 *
		 * @return distance between vertices
		 */
		public IntValue getDistance() {
			return distance;
		}

		/**
		 * Set the distance between vertices.
		 *
		 * @param distance the distance between vertices
		 */
		public void setDistance(IntValue distance) {
			this.distance = distance;
		}

		@Override
		public String toString() {
			return "(" + getVertexId0()
				+ "," + getVertexId1()
				+ "," + getDistance()
				+ ")";
		}

		@Override
		public String toPrintableString() {
			return "first vertex: " + getVertexId0()
				+ ", last vertex: " + getVertexId1()
				+ ", distance: " + getDistance();
		}

		// ----------------------------------------------------------------------------------------

		public static final int HASH_SEED = 0x1c9acb71;

		private transient MurmurHash hasher;

		@Override
		public int hashCode() {
			if (hasher == null) {
				hasher = new MurmurHash(HASH_SEED);
			}

			return hasher.reset()
				.hash(getVertexId0().hashCode())
				.hash(getVertexId1().hashCode())
				.hash(distance.getValue())
				.hash();
		}
	}
}
