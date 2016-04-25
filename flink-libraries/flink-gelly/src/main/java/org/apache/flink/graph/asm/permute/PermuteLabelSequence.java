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

package org.apache.flink.graph.asm.permute;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.generator.random.BlockInfo;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.util.List;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

public class PermuteLabelSequence<K extends RandomGenerator, VV, EV>
implements GraphAlgorithm<LongValue, VV, EV, Graph<LongValue,VV,EV>> {

	// Required configuration
	private final RandomGenerableFactory<K> randomGeneratorFactory;

	private long vertexCount;

	// Optional configuration
	private int parallelism = PARALLELISM_DEFAULT;

	public PermuteLabelSequence(RandomGenerableFactory<K> randomGeneratorFactory, long vertexCount) {
		if (vertexCount < 0) {
			throw new RuntimeException("Vertex count must be non-negative");
		}

		this.randomGeneratorFactory = randomGeneratorFactory;
		this.vertexCount = vertexCount;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public PermuteLabelSequence<K,VV,EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	public Graph<LongValue,VV,EV> run(Graph<LongValue,VV,EV> input)
			throws Exception {
		ExecutionEnvironment env = input.getContext();

		int cyclesPerElement = 2;

		List<BlockInfo<K>> blocks = randomGeneratorFactory
			.getRandomGenerables(vertexCount, cyclesPerElement);

		// group, key, label
		DataSet<Tuple3<IntValue,LongValue,LongValue>> ordering = env
			.fromCollection(blocks)
				.name("Random generators")
			.rebalance()
				.setParallelism(parallelism)
				.name("Rebalance")
			.flatMap(new GenerateRandomOrdering<K>())
				.setParallelism(parallelism)
				.name("Generate random ordering");

		// old label, new label
		DataSet<Tuple2<LongValue,LongValue>> permutedLabels = ordering
			.groupBy(0)
			.sortGroup(1, Order.ASCENDING)
			.reduceGroup(new PermuteLabelGroups<IntValue,LongValue,LongValue>())
				.setParallelism(parallelism)
				.name("Permute label groups");

		// Vertices
		DataSet<Vertex<LongValue,VV>> permutedVertices = input
			.getVertices()
			.join(permutedLabels, JoinHint.REPARTITION_HASH_SECOND)
			.where(0)
			.equalTo(0)
			.with(new ProjectVertexLabel<LongValue, VV>())
				.setParallelism(parallelism)
				.name("Permute vertex labels");

		// Edges
		DataSet<Edge<LongValue,EV>> permutedSource = input
			.getEdges()
			.join(permutedLabels, JoinHint.REPARTITION_HASH_SECOND)
			.where(0)
			.equalTo(0)
			.with(new ProjectEdgeSourceLabel<LongValue,EV>())
				.setParallelism(parallelism)
				.name("Permute edge source labels");

		DataSet<Edge<LongValue,EV>> permutedEdges = permutedSource
			.join(permutedLabels, JoinHint.REPARTITION_HASH_SECOND)
			.where(1)
			.equalTo(0)
			.with(new ProjectEdgeTargetLabel<LongValue,EV>())
				.setParallelism(parallelism)
				.name("Permute edge target labels");

		// Graph
		return Graph.fromDataSet(permutedVertices, permutedEdges, env);
	}

	private static class GenerateRandomOrdering<T extends RandomGenerator>
	implements FlatMapFunction<BlockInfo<T>, Tuple3<IntValue,LongValue,LongValue>> {
		private Tuple3<IntValue,LongValue,LongValue> output = new Tuple3<>(new IntValue(), new LongValue(), new LongValue());

		@Override
		public void flatMap(BlockInfo<T> value, Collector<Tuple3<IntValue,LongValue,LongValue>> out)
				throws Exception {
			RandomGenerator r = value.getRandomGenerable().generator();
			long elementIndex = value.getFirstElement();
			long elementCount = value.getElementCount();

			output.f0.setValue(value.getBlockIndex());

			for (long i = 0 ; i < elementCount ; i++) {
				output.f1.setValue(r.nextLong());
				output.f2.setValue(elementIndex++);

				out.collect(output);
			}
		}
	}

	private static class PermuteLabelGroups<X, Y, T extends CopyableValue<T>>
	implements GroupReduceFunction<Tuple3<X,Y,T>, Tuple2<T,T>> {
		Tuple2<T,T> output = new Tuple2<>();

		@Override
		public void reduce(Iterable<Tuple3<X,Y,T>> values, Collector<Tuple2<T,T>> out)
				throws Exception {
			boolean initialized = false;

			T first = null;

			for (Tuple3<X,Y,T> value : values) {
				if (! initialized) {
					initialized = true;
					first = value.f2.copy();
					output.f1 = value.f2.copy();
				} else {
					output.f0 = value.f2;

					out.collect(output);

					value.f2.copyTo(output.f1);
				}
			}

			output.f0 = first;
			out.collect(output);
		}
	}

	private static class ProjectVertexLabel<_K, _VV>
	implements JoinFunction<Vertex<_K,_VV>, Tuple2<_K,_K>, Vertex<_K,_VV>> {
		private Vertex<_K,_VV> vertex = new Vertex<>();

		@Override
		public Vertex<_K,_VV> join(Vertex<_K,_VV> first, Tuple2<_K,_K> second) throws Exception {
			vertex.f0 = second.f1;
			vertex.f1 = first.f1;

			return vertex;
		}
	}

	private static class ProjectEdgeSourceLabel<_K, _VV>
	implements JoinFunction<Edge<_K,_VV>, Tuple2<_K,_K>, Edge<_K,_VV>> {
		private Edge<_K,_VV> edge = new Edge<>();

		@Override
		public Edge<_K,_VV> join(Edge<_K,_VV> first, Tuple2<_K,_K> second) throws Exception {
			edge.f0 = second.f1;
			edge.f1 = first.f1;
			edge.f2 = first.f2;

			return edge;
		}
	}

	private static class ProjectEdgeTargetLabel<_K, _VV>
	implements JoinFunction<Edge<_K,_VV>, Tuple2<_K,_K>, Edge<_K,_VV>> {
		private Edge<_K,_VV> edge = new Edge<>();

		@Override
		public Edge<_K,_VV> join(Edge<_K,_VV> first, Tuple2<_K,_K> second) throws Exception {
			edge.f0 = first.f0;
			edge.f1 = second.f1;
			edge.f2 = first.f2;

			return edge;
		}
	}
}
