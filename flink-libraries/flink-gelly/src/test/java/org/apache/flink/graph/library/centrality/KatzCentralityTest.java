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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.simple.undirected.Simplify;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.asm.translate.translators.LongValueToUnsignedIntValue;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.junit.Test;

/**
 * Tests for {@link KatzCentrality}.
 */
public class KatzCentralityTest
extends AsmTestBase {

	@Test
	public void testWithUndirectedSimpleGraph()
			throws Exception {
		String expectedResult =
			"(1,3,1.5)\n" +
			"(2,3,1.5)\n" +
			"(3,9,7.0)";

		DataSet<Tuple2<IntValue, DoubleValue>> kc = new KatzCentrality<IntValue, NullValue, NullValue>(0.10, 10)
			.run(undirectedSimpleGraph);

		kc.print();
//		TestBaseUtils.compareResultAsText(kc.collect(), expectedResult);
	}

//	@Test
//	public void testWithDirectedSimpleGraph()
//			throws Exception {
//		String expectedResult =
//			"(2,2,2.0)\n" +
//			"(3,4,4.0)";
//
//		DataSet<Result<IntValue>> bc = new BetweennessCentrality<IntValue, NullValue, NullValue>()
//			.run(directedSimpleGraph);
//
//		TestBaseUtils.compareResultAsText(bc.collect(), expectedResult);
//	}
//
//	@Test
//	public void testWithCompleteGraph()
//			throws Exception {
//		DataSet<Result<LongValue>> bc = new BetweennessCentrality<LongValue, NullValue, NullValue>()
//			.run(completeGraph);
//
//		List<Result<LongValue>> results = bc.collect();
//
//		assertEquals(0, results.size());
//	}
//
//	@Test
//	public void testWithUndirectedRMatGraph()
//			throws Exception {
//		long vertexCount = 1 << 5;
//		long edgeCount = 4 * vertexCount;
//
//		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();
//
//		Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
//			.generate()
//			.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(false));
//
//		ChecksumHashCode checksum = DataSetUtils.checksumHashCode(graph
//			.run(new BetweennessCentrality<LongValue, NullValue, NullValue>()));
//
//		assertEquals(18, checksum.getCount());
//	}
//
	@Test
	public void testWithDirectedRMatGraph()
			throws Exception {
		env = ExecutionEnvironment.createLocalEnvironment();
		env.getConfig().enableObjectReuse();

		long vertexCount = 1 << 6;
		long edgeCount = 4 * vertexCount;

		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

		Graph<IntValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
			.generate()
			.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToUnsignedIntValue()))
			.run(new Simplify<IntValue, NullValue, NullValue>(false));

		DataSet<Tuple2<IntValue, DoubleValue>> kc = new KatzCentrality<IntValue, NullValue, NullValue>(0.5, 10)
			.run(graph);

		kc.print();
	}
}
