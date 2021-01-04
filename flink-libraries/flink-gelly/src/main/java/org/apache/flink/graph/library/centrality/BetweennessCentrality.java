/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.asm.result.UnaryResultBase;
import org.apache.flink.graph.library.paths.unweighted.AllPairsShortestPaths;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class BetweennessCentrality<K extends Value & Comparable<K>, VV, EV>
implements GraphAlgorithm<K, VV, EV, DataSet<BetweennessCentrality.Result<K>>> {

	@Override
	public DataSet<Result<K>> run(Graph<K, VV, EV> input)
			throws Exception {
		DataSet<AllPairsShortestPaths.Result<K>> apsp = input
			.run(new AllPairsShortestPaths<K, VV, EV>());

		DataSet<Tuple3<K, K, LongValue>> pathCount = apsp
			.groupBy("vertexId0", "vertexId1")
			.reduceGroup(new CountPaths<K>())
				.name("Count paths");

		return apsp
			.join(pathCount)
			.where("vertexId0", "vertexId1")
			.equalTo(0, 1)
			.with(new EmitScores<K>())
				.name("Emit scores")
			.groupBy("vertexId0")
			.reduce(new SumScores<K>())
			.setCombineHint(CombineHint.HASH)
				.name("Sum scores");
	}

	@ForwardedFields("vertexId0->0; vertexId1->1")
	private static final class CountPaths<T>
	implements GroupReduceFunction<AllPairsShortestPaths.Result<T>, Tuple3<T, T, LongValue>> {
		private Tuple3<T, T, LongValue> output = new Tuple3<>(null, null, new LongValue());

		@Override
		public void reduce(Iterable<AllPairsShortestPaths.Result<T>> values, Collector<Tuple3<T, T, LongValue>> out)
				throws Exception {
			Iterator<AllPairsShortestPaths.Result<T>> iter = values.iterator();

			AllPairsShortestPaths.Result<T> path = iter.next();
			if (path.getPathLength().getValue() > 0) {
				long count = 1;

				while (iter.hasNext()) {
					path = iter.next();
					count++;
				}

				output.f0 = path.getVertexId0();
				output.f1 = path.getVertexId1();
				output.f2.setValue(count);

				out.collect(output);
			}
		}
	}

	private static final class EmitScores<T>
	implements FlatJoinFunction<AllPairsShortestPaths.Result<T>, Tuple3<T, T, LongValue>, Result<T>> {
		private Result<T> output = new Result<>();

		@Override
		public void join(AllPairsShortestPaths.Result<T> path, Tuple3<T, T, LongValue> count, Collector<Result<T>> out)
				throws Exception {
			output.getScore().setValue(1.0d / count.f2.getValue());

			for (T vertex : path.getIntermediatePathVertices()) {
				output.setVertexId0(vertex);
				out.collect(output);
			}
		}
	}

	@ForwardedFields("vertexId0")
	private static final class SumScores<T>
	implements ReduceFunction<Result<T>> {
		@Override
		public Result<T> reduce(Result<T> value1, Result<T> value2)
				throws Exception {
			value1.setPathCount(value1.getPathCount().getValue() + value2.getPathCount().getValue());
			value1.setScore(value1.getScore().getValue() + value2.getScore().getValue());
			return value1;
		}
	}

	/**
	 * Wraps the results from the BetweennessCentrality algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends UnaryResultBase<T>
	implements PrintableResult {
		private LongValue pathCount = new LongValue(1);

		private DoubleValue score = new DoubleValue();

		/**
		 * Get the path count, the number of shortest paths on which this vertex
		 * is in the intermediate path.
		 *
		 * @return path count
		 */
		public LongValue getPathCount() {
			return pathCount;
		}

		/**
		 * Set the path count, the number of shortest paths on which this vertex
		 * is in the intermediate path.
		 *
		 * @param pathCount path count
		 */
		public void setPathCount(LongValue pathCount) {
			this.pathCount = pathCount;
		}

		/**
		 * Set the path count, the number of shortest paths on which this vertex
		 * is in the intermediate path.
		 *
		 * @param pathCount path count
		 */
		public void setPathCount(long pathCount) {
			this.pathCount.setValue(pathCount);
		}

		/**
		 * Get the betweenness centrality, the number of shortest paths from
		 * all vertices to all others that pass through this vertex. The score
		 * for each vertex path is divided by the number of paths.
		 *
		 * @return betweenness centrality score
		 */
		public DoubleValue getScore() {
			return score;
		}

		/**
		 * Set the betweenness centrality, the number of shortest paths from
		 * all vertices to all others that pass through this vertex. The score
		 * for each vertex path is divided by the number of paths.
		 *
		 * @param score betweenness centrality score
		 */
		public void setScore(DoubleValue score) {
			this.score = score;
		}

		/**
		 * Set the betweenness centrality, the number of shortest paths from
		 * all vertices to all others that pass through this vertex. The score
		 * for each vertex path is divided by the number of paths.
		 *
		 * @param score betweenness centrality score
		 */
		public void setScore(double score) {
			this.score.setValue(score);
		}

		@Override
		public String toString() {
			return "(" + getVertexId0()
				+ "," + getPathCount()
				+ "," + getScore()
				+ ")";
		}

		@Override
		public String toPrintableString() {
			return "Vertex ID: " + getVertexId0()
				+ ", path count: " + getPathCount()
				+ ", betweenness centrality score: " + getScore();
		}

		// ----------------------------------------------------------------------------------------

		public static final int HASH_SEED = 0xb85441a9;

		private transient MurmurHash hasher;

		@Override
		public int hashCode() {
			if (hasher == null) {
				hasher = new MurmurHash(HASH_SEED);
			}

			return hasher.reset()
				.hash(getVertexId0().hashCode())
				.hash(pathCount.getValue())
				.hash(score.getValue())
				.hash();
		}
	}
}
