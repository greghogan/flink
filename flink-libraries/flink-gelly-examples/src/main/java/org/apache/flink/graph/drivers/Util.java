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

package org.apache.flink.graph.drivers;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.asm.dataset.Collect;

import java.util.List;

/**
 * Utility methods for drivers.
 */
public class Util {

	private Util() {}

	/**
	 *
	 * @param executionName
	 * @param result
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	protected static <T> List<T> collectResults(String executionName, DataSet<T> result)
			throws Exception {
		Collect<T> collector = new Collect<>();

		// Refactored due to openjdk7 compile error: https://travis-ci.org/greghogan/flink/builds/200487761
		return collector.run(result).execute(executionName);
	}
}
