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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.library.paths.unweighted.AllPairsShortestPaths.Result;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class AllPairsShortestPathsTest
extends AsmTestBase {

	@Test
	public void testWithUndirectedSimpleGraph()
			throws Exception {
		String expectedResult =
			"(0,1,1,[])\n" +
			"(0,2,1,[])\n" +
			"(0,3,2,[2])\n" +
			"(0,3,2,[1])\n" +
			"(0,4,3,[1,3])\n" +
			"(0,4,3,[2,3])\n" +
			"(0,5,3,[1,3])\n" +
			"(0,5,3,[2,3])\n" +
			"(1,0,1,[])\n" +
			"(1,2,1,[])\n" +
			"(1,3,1,[])\n" +
			"(1,4,2,[3])\n" +
			"(1,5,2,[3])\n" +
			"(2,0,1,[])\n" +
			"(2,1,1,[])\n" +
			"(2,3,1,[])\n" +
			"(2,4,2,[3])\n" +
			"(2,5,2,[3])\n" +
			"(3,0,2,[1])\n" +
			"(3,0,2,[2])\n" +
			"(3,1,1,[])\n" +
			"(3,2,1,[])\n" +
			"(3,4,1,[])\n" +
			"(3,5,1,[])\n" +
			"(4,0,3,[3,1])\n" +
			"(4,0,3,[3,2])\n" +
			"(4,1,2,[3])\n" +
			"(4,2,2,[3])\n" +
			"(4,3,1,[])\n" +
			"(4,5,2,[3])\n" +
			"(5,0,3,[3,1])\n" +
			"(5,0,3,[3,2])\n" +
			"(5,1,2,[3])\n" +
			"(5,2,2,[3])\n" +
			"(5,3,1,[])\n" +
			"(5,4,2,[3])\n";

		DataSet<Result<IntValue>> apsp = new AllPairsShortestPaths<IntValue, NullValue, NullValue>()
			.run(undirectedSimpleGraph);

		TestBaseUtils.compareResultAsText(apsp.collect(), expectedResult);
	}

	@Test
	public void testWithDirectedSimpleGraph()
			throws Exception {
		String expectedResult =
			"(0,1,1,[])\n" +
			"(0,2,1,[])\n" +
			"(0,3,2,[2])\n" +
			"(0,4,3,[2,3])\n" +
			"(2,1,1,[])\n" +
			"(2,3,1,[])\n" +
			"(2,4,2,[3])\n" +
			"(3,1,1,[])\n" +
			"(3,4,1,[])\n" +
			"(5,1,2,[3])\n" +
			"(5,3,1,[])\n" +
			"(5,4,2,[3])";

		DataSet<Result<IntValue>> apsp = new AllPairsShortestPaths<IntValue, NullValue, NullValue>()
			.run(directedSimpleGraph);

		TestBaseUtils.compareResultAsText(apsp.collect(), expectedResult);
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		long expectedPathCount = completeGraphVertexCount * (completeGraphVertexCount - 1);

		DataSet<Result<LongValue>> apsp = new AllPairsShortestPaths<LongValue, NullValue, NullValue>()
			.run(completeGraph);

		List<Result<LongValue>> results = apsp.collect();

		Assert.assertEquals(expectedPathCount, results.size());

		for (Result<LongValue> result : results) {
			Assert.assertEquals(1, result.getPathLength().getValue());
			Assert.assertEquals(0, result.getIntermediatePathVertices().size());
		}
	}

	@Test
	public void testWithUndirectedRMatGraph()
			throws Exception {
		DataSet<Result<LongValue>> apsp = undirectedRMatGraph(5, 4)
			.run(new AllPairsShortestPaths<LongValue, NullValue, NullValue>());

		Checksum checksum = new ChecksumHashCode<Result<LongValue>>()
			.run(apsp)
			.execute();

		Assert.assertEquals(1142, checksum.getCount());
		Assert.assertEquals(0x00000240337ac46fL, checksum.getChecksum());
	}

	/*
	 * This test result can be verified with the following Python script.

import networkx as nx

graph=nx.read_edgelist('directedSimpleGraph.csv', delimiter=',', create_using=nx.DiGraph())
apsp=nx.all_pairs_shortest_path(graph)
paths = [path for source, target_path in apsp.iteritems() for target, path in target_path.iteritems() if len(path) > 1]
	 */
	@Test
	public void testWithDirectedRMatGraph()
			throws Exception {
		DataSet<Result<LongValue>> apsp = directedRMatGraph(6, 4)
			.run(new AllPairsShortestPaths<LongValue, NullValue, NullValue>());

		Checksum checksum = new ChecksumHashCode<Result<LongValue>>()
			.run(apsp)
			.execute();

		Assert.assertEquals(2990, checksum.getCount());
		Assert.assertEquals(0x000005bfb09c521cL, checksum.getChecksum());
	}
}
