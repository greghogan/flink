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

package org.apache.flink.graph.library.centrality;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.centrality.BetweennessCentrality.Result;
import org.apache.flink.graph.library.paths.AllPairsShortestPaths;
import org.apache.flink.graph.utils.Murmur3_32;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;

/**
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class BetweennessCentrality<K extends Comparable<K>, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Result<K>> {

	@Override
	protected String getAlgorithmName() {
		return BetweennessCentrality.class.getName();
	}

	@Override
	protected boolean mergeConfiguration(GraphAlgorithmWrappingDataSet other) {
		Preconditions.checkNotNull(other);

		if (! BetweennessCentrality.class.isAssignableFrom(other.getClass())) {
			return false;
		}

		return true;
	}

	@Override
	public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		DataSet<AllPairsShortestPaths.Result<K>> apsp = input
			.run(new AllPairsShortestPaths<K, VV, EV>()
				.setFilterPathsByOrder(true));

		DataSet<Tuple3<K, K, LongValue>> pathCount = apsp
			.groupBy(0, 1)
			.reduceGroup(new CountPaths<K>())
				.name("Count paths");

		return apsp
			.join(pathCount)
			.where(0, 1)
			.equalTo(0, 1)
			.with(new EmitScores<K>())
				.name("Emit scores")
			.groupBy(0)
			.reduce(new SumScores<K>())
			.setCombineHint(CombineHint.HASH)
				.name("Sum scores");
	}

	@ForwardedFields("0; 1")
	private static final class CountPaths<T>
	implements GroupReduceFunction<AllPairsShortestPaths.Result<T>, Tuple3<T, T, LongValue>> {
		private Tuple3<T, T, LongValue> output = new Tuple3<>(null, null, new LongValue());

		@Override
		public void reduce(Iterable<AllPairsShortestPaths.Result<T>> values, Collector<Tuple3<T, T, LongValue>> out)
				throws Exception {
			Iterator<AllPairsShortestPaths.Result<T>> iter = values.iterator();

			AllPairsShortestPaths.Result<T> path = iter.next();
			long count = 1;

			while (iter.hasNext()) {
				path = iter.next();
				count++;
			}

			output.f0 = path.f0;
			output.f1 = path.f1;
			output.f2.setValue(count);

			out.collect(output);
		}
	}

	private static final class EmitScores<T>
	implements FlatJoinFunction<AllPairsShortestPaths.Result<T>, Tuple3<T, T, LongValue>, Result<T>> {
		private Result<T> output = new Result<>(new LongValue(1), new DoubleValue());

		@Override
		public void join(AllPairsShortestPaths.Result<T> path, Tuple3<T, T, LongValue> count, Collector<Result<T>> out)
				throws Exception {
			output.f2.setValue(1.0d / count.f2.getValue());

			for (T vertex : path.getIntermediatePathVertices()) {
				output.f0 = vertex;
				out.collect(output);
			}
		}
	}

	@ForwardedFields("0")
	private static final class SumScores<T>
	implements ReduceFunction<Result<T>> {
		@Override
		public Result<T> reduce(Result<T> value1, Result<T> value2)
				throws Exception {
			value1.f1.setValue(value1.f1.getValue() + value2.f1.getValue());
			value1.f2.setValue(value1.f2.getValue() + value2.f2.getValue());
			return value1;
		}
	}

	/**
	 * Wraps the results from the BetweennessCentrality algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends Tuple3<T, LongValue, DoubleValue> {
		public static final int HASH_SEED = 0xb85441a9;

		private Murmur3_32 hasher = new Murmur3_32(HASH_SEED);

		/**
		 * No-arg constructor.
		 */
		public Result() {
			this(new LongValue(), new DoubleValue());
		}

		/**
		 *
		 * @param count
		 * @param score
		 */
		public Result(LongValue count, DoubleValue score) {
			this.f1 = count;
			this.f2 = score;
		}

		/**
		 * Get the vertex ID.
		 *
		 * @return vertex ID
		 */
		public T getVertexId() {
			return f0;
		}

		/**
		 * Get the path count, the number of shortest paths on which this vertex
		 * is in the intermediate path.
		 *
		 * @return path count
		 */
		public LongValue getPathCount() {
			return f1;
		}

		/**
		 * Get the betweenness centrality, the number of shortest paths from
		 * all vertices to all others that pass through this vertex. The score
		 * for each vertex path is divided by the number of paths.
		 *
		 * @return betweenness centrality
		 */
		public DoubleValue getBetweennessCentrality() {
			return f2;
		}

		@Override
		public int hashCode() {
			return hasher.reset()
				.hash(f0.hashCode())
				.hash(f1.getValue())
				.hash(f2.getValue())
				.hash();
		}
	}
}
