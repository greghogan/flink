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

package org.apache.flink.graph.library.cores.undirected;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests for {@link KCore}.
 */
public class KCoreTest {

	@Test
	public void testSimpleGraph()
			throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.getConfig().enableObjectReuse();

		Object[][] edges = new Object[][] {
			new Object[]{0, 1},
			new Object[]{0, 2},
			new Object[]{2, 1},
			new Object[]{2, 3},
			new Object[]{3, 1},
			new Object[]{3, 4},
			new Object[]{5, 3},
		};

		List<Edge<IntValue, NullValue>> directedEdgeList = new LinkedList<>();

		for (Object[] edge : edges) {
			directedEdgeList.add(new Edge<>(new IntValue((int) edge[0]), new IntValue((int) edge[1]), NullValue.getInstance()));
		}

		Graph<IntValue, NullValue, NullValue> directedSimpleGraph = Graph.fromCollection(directedEdgeList, env);
		Graph<IntValue, NullValue, NullValue> undirectedSimpleGraph = directedSimpleGraph
			.getUndirected();

		Graph<IntValue, IntValue, IntValue> kcore = undirectedSimpleGraph
			.run(new KCore<IntValue, NullValue, NullValue>());

		kcore.getVertices().print();
		kcore.getEdges().print();
	}
}
