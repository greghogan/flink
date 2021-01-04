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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.asm.result.UnaryResult;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 *
 */
public class KatzCentrality<K, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Tuple2<K, DoubleValue>> {

	private int maxIterations;

	private double alpha;

	public KatzCentrality(double alpha, int maxIterations) {
		this.alpha = alpha;
		this.maxIterations = maxIterations;
	}

	@Override
	protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
		if (!super.canMergeConfigurationWith(other)) {
			return false;
		}

		KatzCentrality rhs = (KatzCentrality) other;

		return alpha == rhs.alpha;
	}

	@Override
	protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
		super.mergeConfiguration(other);

		KatzCentrality rhs = (KatzCentrality) other;

		maxIterations = Math.max(maxIterations, rhs.maxIterations);
	}

	@Override
	protected DataSet<Tuple2<K, DoubleValue>> runInternal(Graph<K, VV, EV> input) throws Exception {
		DataSet<Tuple2<K, DoubleValue>> scores = input
			.getEdges()
			.<Tuple1<K>>project(1)
			.map(new InitializeScores<K>())
				.name("Initialize scores");

		DataSet<Tuple3<K, K, DoubleValue>> paths = input
			.getEdges()
			.map(new InitializePaths<K, EV>())
				.name("Initialize paths");

		DeltaIteration<Tuple2<K, DoubleValue>, Tuple3<K, K, DoubleValue>> iteration = scores
			.iterateDelta(paths, maxIterations, 0);

		DataSet<Tuple3<K, K, DoubleValue>> sumOfIncidentScores = iteration.getWorkset()
			.groupBy(0, 1)
			.reduceGroup(new SumIncidentScores<K>(alpha))
				.name("Sum incident scores");

		DataSet<Tuple2<K, DoubleValue>> nextSolutionSet = sumOfIncidentScores
			.<Tuple2<K, DoubleValue>>project(1, 2)
			.groupBy(0)
			.sum(1)
			.join(iteration.getSolutionSet(), JoinHint.REPARTITION_SORT_MERGE)
			.where(0)
			.equalTo(0)
			.with(new JoinScores<K>())
				.name("Join scores");

		DataSet<Tuple3<K, K, DoubleValue>> nextWorkSet = sumOfIncidentScores
			.join(input.getEdges())
			.where(1)
			.equalTo(0)
			.projectFirst(0)
			.projectSecond(1)
			.projectFirst(2);

		return iteration.closeWith(nextSolutionSet, nextWorkSet);
	}

	@ForwardedFields("0")
	private static class InitializeScores<T>
	implements MapFunction<Tuple1<T>, Tuple2<T, DoubleValue>> {
		private Tuple2<T, DoubleValue> output = new Tuple2<>(null, new DoubleValue(0));

		@Override
		public Tuple2<T, DoubleValue> map(Tuple1<T> value) throws Exception {
			output.f0 = value.f0;

			return output;
		}
	}

	@ForwardedFields("0; 1")
	private static class InitializePaths<T, ET>
	implements MapFunction<Edge<T, ET>, Tuple3<T, T, DoubleValue>> {
		private Tuple3<T, T, DoubleValue> output = new Tuple3<>(null, null, new DoubleValue(1));

		@Override
		public Tuple3<T, T, DoubleValue> map(Edge<T, ET> value)
				throws Exception {
			output.f0 = value.f0;
			output.f1 = value.f1;

			return output;
		}
	}

	@ForwardedFields("0; 1")
	private static class SumIncidentScores<T>
	implements GroupReduceFunction<Tuple3<T, T, DoubleValue>, Tuple3<T, T, DoubleValue>> {
		private double alpha;

		private Tuple3<T, T, DoubleValue> output = new Tuple3<>(null, null, new DoubleValue());

		public SumIncidentScores(double alpha) {
			this.alpha = alpha;
		}

		@Override
		public void reduce(Iterable<Tuple3<T, T, DoubleValue>> values, Collector<Tuple3<T, T, DoubleValue>> out)
				throws Exception {
			double sum = 0;
			boolean first = true;

			for (Tuple3<T, T, DoubleValue> value : values) {
				if (first) {
					output.f0 = value.f0;
					output.f1 = value.f1;
					first = false;
				}

				sum += value.f2.getValue();
			}

			output.f2.setValue(alpha * sum);

			out.collect(output);
		}
	}

	@ForwardedFieldsFirst("0")
	@ForwardedFieldsSecond("0")
	private static class JoinScores<T>
	implements FlatJoinFunction<Tuple2<T, DoubleValue>, Tuple2<T, DoubleValue>, Tuple2<T, DoubleValue>> {
		private Tuple2<T, DoubleValue> output = new Tuple2<>(null, new DoubleValue());

		@Override
		public void join(Tuple2<T, DoubleValue> first, Tuple2<T, DoubleValue> second, Collector<Tuple2<T, DoubleValue>> out)
			throws Exception {
			output.f0 = first.f0;
			output.f1.setValue(first.f1.getValue() + second.f1.getValue());

			out.collect(output);
		}
	}

	/**
	 * Wraps the {@link Tuple2} to encapsulate results from the Katz Centrality algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends Tuple2<T, DoubleValue>
	implements PrintableResult, UnaryResult<T> {
		public static final int HASH_SEED = 0xa40a0deb;

		private MurmurHash hasher = new MurmurHash(HASH_SEED);

		@Override
		public T getVertexId0() {
			return f0;
		}

		@Override
		public void setVertexId0(T value) {
			f0 = value;
		}

		/**
		 * Get the Katz Centrality score.
		 *
		 * @return the Katz Centrality score
		 */
		public DoubleValue getKatzCentralityScore() {
			return f1;
		}

		public String toPrintableString() {
			return "Vertex ID: " + getVertexId0()
				+ ", PageRank score: " + getKatzCentralityScore();
		}

		@Override
		public int hashCode() {
			return hasher.reset()
				.hash(f0.hashCode())
				.hash(f1.getValue())
				.hash();
		}
	}
}
