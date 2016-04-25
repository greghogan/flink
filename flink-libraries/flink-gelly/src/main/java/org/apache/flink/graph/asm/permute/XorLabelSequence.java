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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

public class XorLabelSequence<VV, EV>
implements GraphAlgorithm<LongValue, VV, EV, Graph<LongValue,VV,EV>> {

	// Required configuration
	private long vertexCount;

	// Optional configuration
	private int parallelism = PARALLELISM_DEFAULT;

	public XorLabelSequence(long vertexCount) {
		if (vertexCount < 0) {
			throw new RuntimeException("Vertex count must be non-negative");
		}

		this.vertexCount = vertexCount;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public XorLabelSequence<VV,EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	public Graph<LongValue,VV,EV> run(Graph<LongValue,VV,EV> input)
			throws Exception {
		long bitmask = 0x5555555555555555L & (1 << (64 - Long.numberOfLeadingZeros(vertexCount - 1)));

		// Vertices
		DataSet<Vertex<LongValue,VV>> permutedVertices = input
			.getVertices()
			.map(new XorVertex<VV>(bitmask))
				.setParallelism(parallelism)
				.name("XOR vertex labels");

		// Edges
		DataSet<Edge<LongValue,EV>> permutedEdges = input
			.getEdges()
			.map(new XorEdge<EV>(bitmask))
				.setParallelism(parallelism)
				.name("XOR edge labels");

		// Graph
		return Graph.fromDataSet(permutedVertices, permutedEdges, input.getContext());

	}

	private static class XorVertex<VT>
	implements MapFunction<Vertex<LongValue,VT>, Vertex<LongValue,VT>> {
		private long bitmask;

		public XorVertex(long bitmask) {
			this.bitmask = bitmask;
		}

		@Override
		public Vertex<LongValue,VT> map(Vertex<LongValue,VT> value) throws Exception {
			value.f0.setValue(bitmask ^ value.f0.getValue());
			return value;
		}
	}

	private static class XorEdge<ET>
	implements MapFunction<Edge<LongValue,ET>, Edge<LongValue,ET>> {
		private long bitmask;

		public XorEdge(long bitmask) {
			this.bitmask = bitmask;
		}

		@Override
		public Edge<LongValue,ET> map(Edge<LongValue,ET> value) throws Exception {
			value.f0.setValue(bitmask ^ value.f0.getValue());
			value.f1.setValue(bitmask ^ value.f1.getValue());
			return value;
		}
	}
}
