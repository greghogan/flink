/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.asm.permute;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;

import java.security.SecureRandom;
import java.util.Random;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

public class PermuteGraph<K, VV, EV>
implements GraphAlgorithm<K, VV, EV, Graph<K,VV,EV>> {

	// Optional configuration
	private int parallelism = PARALLELISM_DEFAULT;

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public PermuteGraph<K,VV,EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	public Graph<K,VV,EV> run(Graph<K,VV,EV> input)
			throws Exception {
		// Vertices
		DataSet<Vertex<K,VV>> permutedVertices = input
			.getVertices()
			.map(new ZipWithRandomId<Vertex<K,VV>>())
				.setParallelism(parallelism)
				.name("Zip vertices with random ID")
			.partitionByHash(0)
				.setParallelism(parallelism)
				.name("Hash partition vertices")
			.sortPartition(0, Order.ASCENDING)
				.setParallelism(parallelism)
				.name("Sort partition vertices")
			.map(new Unzip<Vertex<K,VV>>())
				.setParallelism(parallelism)
				.name("Unzip vertices");

		// Edges
		DataSet<Edge<K,EV>> permutedEdges = input
			.getEdges()
			.map(new ZipWithRandomId<Edge<K,EV>>())
				.setParallelism(parallelism)
				.name("Zip edges with random ID")
			.partitionByHash(0)
				.setParallelism(parallelism)
				.name("Hash partition edges")
			.sortPartition(0, Order.ASCENDING)
				.setParallelism(parallelism)
				.name("Sort partition edges")
			.map(new Unzip<Edge<K,EV>>())
				.setParallelism(parallelism)
				.name("Unzip edges");

		// Graph
		return Graph.fromDataSet(permutedVertices, permutedEdges, input.getContext());
	}

	@ForwardedFields("*->1")
	private static class ZipWithRandomId<T>
	implements MapFunction<T, Tuple2<LongValue,T>> {
		private Random random;

		private LongValue id = new LongValue();

		private Tuple2<LongValue,T> output = new Tuple2<>(id, null);

		public ZipWithRandomId() {
			long seed = new SecureRandom().nextLong();
			random = new Random(seed);
		}

		@Override
		public Tuple2<LongValue,T> map(T value) throws Exception {
			id.setValue(random.nextLong());
			output.f1 = value;
			return output;
		}
	}

	@ForwardedFields("1->*")
	private static class Unzip<T>
	implements MapFunction<Tuple2<LongValue,T>, T> {
		@Override
		public T map(Tuple2<LongValue, T> value)
				throws Exception {
			return value.f1;
		}
	}
}
