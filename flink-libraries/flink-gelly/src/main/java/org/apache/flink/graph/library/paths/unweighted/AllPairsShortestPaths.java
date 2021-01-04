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

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.result.BinaryResultBase;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.asm.result.TranslatableResult;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.library.paths.unweighted.AllPairsShortestPaths.Result;
import org.apache.flink.graph.types.valuearray.ValueArray;
import org.apache.flink.graph.types.valuearray.ValueArrayFactory;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Produce all shortest-paths between every pair of vertices.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class AllPairsShortestPaths<K extends Value & Comparable<K>, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Result<K>> {

	private static final String NEW_LONGEST_PATHS = "new longest paths";

	@Override
	public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// u, v, 1, ValueArray(Ø)
		DataSet<Tuple4<K, K, IntValue, ValueArray<K>>> edges = input
			.getEdges()
			.map(new InitializeEdges<K, EV>())
				.name("Initialize edges");

		// u, v, d, path
		IterativeDataSet<Tuple4<K, K, IntValue, ValueArray<K>>> iterative = edges
			.iterate(Integer.MAX_VALUE);

		// u, v, d
		DataSet<Tuple2<K, K>> shortestPathDistances = iterative
			.<Tuple2<K, K>>project(0, 1)
			.distinct(0, 1)
				.name("Distinct distances");

		// u, v, d, path where d is the maximum possible distance for this iteration
		DataSet<Tuple4<K, K, IntValue, ValueArray<K>>> maxLengthPaths = iterative
			.filter(new MaxLengthPaths<K>())
				.name("Longest paths");

		// u, v, d for all new paths
		DataSet<Tuple4<K, K, IntValue, ValueArray<K>>> connectedPaths = iterative
			.join(maxLengthPaths)
			.where(1)
			.equalTo(0)
			.with(new JoinPaths<K>())
				.name("Connected paths");

		// u, v, d, shortest path
		DataSet<Tuple4<K, K, IntValue, ValueArray<K>>> newPaths = connectedPaths
			.leftOuterJoin(shortestPathDistances, JoinHint.REPARTITION_HASH_SECOND)
			.where(0, 1)
			.equalTo(0, 1)
			.with(new NewShortestPaths<K>())
				.name("New paths");

		//
		DataSet<Tuple4<K, K, IntValue, ValueArray<K>>> newShortestPaths = newPaths
			.groupBy(0, 1)
			.sortGroup(2, Order.ASCENDING)
			.reduceGroup(new FindShortestPaths<K>())
				.name("New shortest paths");

		//
		DataSet<Tuple4<K,  K, IntValue, ValueArray<K>>> allShortestPaths = iterative
			.union(newShortestPaths)
				.name("All paths");

		iterative.registerAggregationConvergenceCriterion(NEW_LONGEST_PATHS, new LongSumAggregator(), new Convergence());

		return iterative
			.closeWith(allShortestPaths)
			.map(new TranslateResult<K>())
				.name("Map result");
	}

	/**
	 * Convert edges to tuples with a distance of '1' with an empty path array.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	@ForwardedFields("0; 1")
	private static final class InitializeEdges<T extends Value, ET>
	implements MapFunction<Edge<T, ET>, Tuple4<T, T, IntValue, ValueArray<T>>> {
		private Tuple4<T, T, IntValue, ValueArray<T>> output = new Tuple4<>(null, null, new IntValue(1), null);

		@Override
		public Tuple4<T, T, IntValue, ValueArray<T>> map(Edge<T, ET> edge)
				throws Exception {
			if (output.f3 == null) {
				output.f3 = ValueArrayFactory.createValueArray(edge.f0.getClass());
			}

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
	private static final class MaxLengthPaths<T>
	extends RichFilterFunction<Tuple4<T, T, IntValue, ValueArray<T>>> {
		private int pathLengthFilter;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			int superstep = getIterationRuntimeContext().getSuperstepNumber();

			// each iteration doubles the length of the largest path
			// and the first iteration processes edges
			pathLengthFilter = 1 << (superstep - 1);
		}

		@Override
		public boolean filter(Tuple4<T, T, IntValue, ValueArray<T>> path)
				throws Exception {
			return path.f2.getValue() == pathLengthFilter;
		}
	}

	/**
	 *
	 * @param <T> ID type
	 */
	@ForwardedFieldsFirst("0")
	@ForwardedFieldsSecond("1")
	private class JoinPaths<T extends Comparable<T>>
	implements FlatJoinFunction<Tuple4<T, T, IntValue, ValueArray<T>>, Tuple4<T, T, IntValue, ValueArray<T>>, Tuple4<T, T, IntValue, ValueArray<T>>> {
		private Tuple4<T, T, IntValue, ValueArray<T>> output = new Tuple4<>(null, null, new IntValue(), null);

		@Override
		public void join(Tuple4<T, T, IntValue, ValueArray<T>> first, Tuple4<T, T, IntValue, ValueArray<T>> second, Collector<Tuple4<T, T, IntValue, ValueArray<T>>> out)
				throws Exception {
			if (first.f0.compareTo(second.f1) != 0) {
				output.f0 = first.f0;
				output.f1 = second.f1;
				output.f2.setValue(first.f2.getValue() + second.f2.getValue());

				first.f3.mark();

				output.f3 = first.f3;
				output.f3.add(second.f0);
				output.f3.addAll(second.f3);
				out.collect(output);

				first.f3.reset();
			}
		}
	}

	/**
	 *
	 * @param <T> ID type
	 */
	private static final class NewShortestPaths<T>
	implements FlatJoinFunction<Tuple4<T, T, IntValue, ValueArray<T>>, Tuple2<T, T>, Tuple4<T, T, IntValue, ValueArray<T>>> {
		@Override
		public void join(Tuple4<T, T, IntValue, ValueArray<T>> first, Tuple2<T, T> second, Collector<Tuple4<T, T, IntValue, ValueArray<T>>> out)
				throws Exception {
			if (second == null) {
				out.collect(first);
			}
		}
	}

	/**
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("*")
	private class FindShortestPaths<T>
	extends RichGroupReduceFunction<Tuple4<T, T, IntValue, ValueArray<T>>, Tuple4<T, T, IntValue, ValueArray<T>>> {
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

			LongSumAggregator agg = getIterationRuntimeContext().getIterationAggregator(NEW_LONGEST_PATHS);
			agg.aggregate(maxLengthCount);
		}

		@Override
		public void reduce(Iterable<Tuple4<T, T, IntValue, ValueArray<T>>> paths, Collector<Tuple4<T, T, IntValue, ValueArray<T>>> out)
				throws Exception {
			int shortestPathLength = Integer.MAX_VALUE;
			int shortestPathCount = 0;

			for (Tuple4<T, T, IntValue, ValueArray<T>> path : paths) {
				int pathLength = path.f2.getValue();

				if (pathLength > shortestPathLength) {
					break;
				}

				shortestPathLength = pathLength;
				out.collect(path);
				shortestPathCount++;
			}

			if (shortestPathLength == maxLength) {
				maxLengthCount += shortestPathCount;
			}
		}
	}

	/**
	 * A shortest path of length {@code n} contains two shortest paths of
	 * length {@code n-1}. Terminate when no new shortest paths are possible.
	 */
	private static class Convergence
	implements ConvergenceCriterion<LongValue> {
		@Override
		public boolean isConverged(int iteration, LongValue value) {
			return value.getValue() < 2;
		}
	}

	/**
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0->vertexId0; 1->vertexId1; 2->pathLength; 3->intermediatePathVertices")
	private static class TranslateResult<T>
	implements MapFunction<Tuple4<T, T, IntValue, ValueArray<T>>, Result<T>> {
		private Result<T> output = new Result<>();

		@Override
		public Result<T> map(Tuple4<T, T, IntValue, ValueArray<T>> value)
				throws Exception {
			output.setVertexId0(value.f0);
			output.setVertexId1(value.f1);
			output.setPathLength(value.f2);
			output.setIntermediatePathVertices(value.f3);
			return output;
		}
	}

	/**
	 * Wraps the results from the AllPairsShortestPaths algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends BinaryResultBase<T>
	implements PrintableResult {
		private IntValue pathLength;

		private ValueArray<T> intermediatePathVertices;

		/**
		 * Get the path length.
		 *
		 * @return path length
		 */
		public IntValue getPathLength() {
			return pathLength;
		}

		/**
		 * Set the path length.
		 *
		 * @param pathLength the path length
		 */
		public void setPathLength(IntValue pathLength) {
			this.pathLength = pathLength;
		}

		/**
		 * Get the intermediate path vertices.
		 *
		 * @return intermediate path vertices
		 */
		public ValueArray<T> getIntermediatePathVertices() {
			return intermediatePathVertices;
		}

		/**
		 * Get the intermediate path vertices.
		 *
		 * @param intermediatePathVertices intermediate path vertices
		 */
		public void setIntermediatePathVertices(ValueArray<T> intermediatePathVertices) {
			this.intermediatePathVertices = intermediatePathVertices;
		}

		@Override
		public String toString() {
			return "(" + getVertexId0()
				+ "," + getVertexId1()
				+ "," + getPathLength()
				+ "," + getIntermediatePathVertices()
				+ ")";
		}

		@Override
		public String toPrintableString() {
			StringBuilder builder = new StringBuilder()
				.append(getVertexId0())
				.append("->");

			for (T vertex : getIntermediatePathVertices()) {
				builder
					.append(vertex)
					.append("->");
			}

			return builder
				.append(getVertexId1())
				.toString();
		}

		// ----------------------------------------------------------------------------------------

		public static final int HASH_SEED = 0xa3a03a40;

		private transient MurmurHash hasher;

		@Override
		public int hashCode() {
			if (hasher == null) {
				hasher = new MurmurHash(HASH_SEED);
			}

			return hasher.reset()
				.hash(getVertexId0().hashCode())
				.hash(getVertexId1().hashCode())
				.hash(pathLength.getValue())
				.hash(intermediatePathVertices.hashCode())
				.hash();
		}

		// ----------------------------------------------------------------------------------------

		@Override
		public <U> TranslatableResult<U> translate(TranslateFunction<T, U> translator, TranslatableResult<U> reuse, Collector<TranslatableResult<U>> out)
				throws Exception {
			if (reuse == null) {
				reuse = new Result<>();
			}

			Result<U> translated = (Result<U>) reuse;

			translated.setVertexId0(translator.translate(this.getVertexId0(), translated.getVertexId0()));

			if (translated.getIntermediatePathVertices() == null) {
				Class<? extends Value> cls = (Class<? extends Value>) translated.getVertexId0().getClass();
				ValueArray<U> valueArray = ValueArrayFactory.createValueArray(cls);
				translated.setIntermediatePathVertices(valueArray);
			}

			ValueArray<U> intermediatePathVertices = translated.getIntermediatePathVertices();
			intermediatePathVertices.clear();

			U translatedVertex = translated.getVertexId1();
			for (T intermediateVertex : this.getIntermediatePathVertices()) {
				translatedVertex = translator.translate(intermediateVertex, translatedVertex);
				intermediatePathVertices.add(translatedVertex);
			}

			translated.setVertexId1(translator.translate(this.getVertexId1(), translated.getVertexId1()));
			translated.setPathLength(this.getPathLength());

			out.collect(translated);

			return reuse;
		}
	}
}
