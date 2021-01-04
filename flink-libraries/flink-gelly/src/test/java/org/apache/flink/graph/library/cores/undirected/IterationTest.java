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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

import org.junit.Test;

/**
 *
 */
public class IterationTest {

	@Test
	public void test()
			throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.getConfig().enableObjectReuse();

		IterativeDataSet<Integer> iterative = env.fromElements(0, 1, 2, 3)
			.iterate(10);

		DataSet<Integer> incremented = iterative
			.map(new Increment())
			.name("Increment");

		DataSet<Integer> incrementedAgain = incremented
			.map(new Increment())
			.name("Increment again");

		incrementedAgain.writeAsText("IterationTest.txt");

		iterative
			.closeWith(incrementedAgain)
				.map(new Increment())
					.name("Increment at end")
				.print();
//				.output(new DiscardingOutputFormat<Integer>())
//				.name("Iteration termination");
	}

	private static class Increment
	implements MapFunction<Integer, Integer> {
		@Override
		public Integer map(Integer value) throws Exception {
			return ++value;
		}
	}
}
