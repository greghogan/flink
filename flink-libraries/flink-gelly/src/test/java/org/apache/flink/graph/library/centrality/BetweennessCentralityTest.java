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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.library.centrality.BetweennessCentrality.Result;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class BetweennessCentralityTest
extends AsmTestBase {

	@Test
	public void testWithUndirectedSimpleGraph()
			throws Exception {
		String expectedResult =
			"(1,6,3.0)\n" +
			"(2,6,3.0)\n" +
			"(3,18,14.0)";

		DataSet<Result<IntValue>> bc = new BetweennessCentrality<IntValue, NullValue, NullValue>()
			.run(undirectedSimpleGraph);

		TestBaseUtils.compareResultAsText(bc.collect(), expectedResult);
	}

	@Test
	public void testWithDirectedSimpleGraph()
			throws Exception {
		String expectedResult =
			"(2,2,2.0)\n" +
			"(3,4,4.0)";

		DataSet<Result<IntValue>> bc = new BetweennessCentrality<IntValue, NullValue, NullValue>()
			.run(directedSimpleGraph);

		TestBaseUtils.compareResultAsText(bc.collect(), expectedResult);
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		DataSet<Result<LongValue>> bc = new BetweennessCentrality<LongValue, NullValue, NullValue>()
			.run(completeGraph);

		List<Result<LongValue>> results = bc.collect();

		assertEquals(0, results.size());
	}

	@Test
	public void testWithUndirectedRMatGraph()
			throws Exception {
		DataSet<Result<LongValue>> bc = undirectedRMatGraph(5, 4)
			.run(new BetweennessCentrality<LongValue, NullValue, NullValue>());

		Checksum checksum = new ChecksumHashCode<Result<LongValue>>()
			.run(bc)
			.execute();

		assertEquals(18, checksum.getCount());
	}

	@Test
	public void testWithDirectedRMatGraph()
			throws Exception {
		DataSet<Result<LongValue>> bc = directedRMatGraph(6, 4)
			.run(new BetweennessCentrality<LongValue, NullValue, NullValue>());

		Checksum checksum = new ChecksumHashCode<Result<LongValue>>()
			.run(bc)
			.execute();

		assertEquals(28, checksum.getCount());
	}
}
