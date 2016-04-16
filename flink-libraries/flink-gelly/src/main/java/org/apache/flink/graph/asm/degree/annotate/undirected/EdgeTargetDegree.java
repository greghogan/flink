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

package org.apache.flink.graph.asm.degree.annotate.undirected;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.CachingGraphAlgorithm;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.JoinEdgeWithVertexDegree;
import org.apache.flink.types.LongValue;

/**
 * Annotates edges of an undirected graph with the target degree count.
 *
 * @param <K> graph label type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class EdgeTargetDegree<K, VV, EV>
extends CachingGraphAlgorithm<K, VV, EV, DataSet<Edge<K,LongValue>>> {

	// Optional configuration
	private boolean reduceOnSourceLabel = false;

	private int parallelism = ExecutionConfig.PARALLELISM_UNKNOWN;

	/**
	 * The degree can be counted from either the edge source or target labels.
	 * By default the target labels are counted. Reducing on source labels may
	 * optimize the algorithm if the input edge list is sorted by source label.
	 *
	 * @param reduceOnSourceLabel set to {@code true} if the input edge list
	 *                            is sorted by target label
	 * @return this
	 */
	public EdgeTargetDegree<K,VV,EV> setReduceOnSourceLabel(boolean reduceOnSourceLabel) {
		this.reduceOnSourceLabel = reduceOnSourceLabel;

		return this;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public EdgeTargetDegree<K,VV,EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return EdgeTargetDegree.class.getCanonicalName();
	}

	@Override
	public DataSet<Edge<K,LongValue>> runInternal(Graph<K,VV,EV> input)
			throws Exception {
		DataSet<Vertex<K,LongValue>> vertexDegrees;

		if (reduceOnSourceLabel) {
			// s, d(s)
			vertexDegrees = input
				.run(new VertexDegree<K,VV,EV>()
					.setParallelism(parallelism));
		} else {
			// t, d(t)
			vertexDegrees = input
				.run(new VertexDegree<K,VV,EV>()
					.setReduceOnTargetLabel(true)
					.setParallelism(parallelism));
		}

		// s, t, d(t)
		return input.getEdges()
			.join(vertexDegrees, JoinHint.REPARTITION_HASH_SECOND)
			.where(1)
			.equalTo(0)
			.with(new JoinEdgeWithVertexDegree<K,EV>())
				.setParallelism(parallelism)
				.name("Edge target degree");
	}
}
