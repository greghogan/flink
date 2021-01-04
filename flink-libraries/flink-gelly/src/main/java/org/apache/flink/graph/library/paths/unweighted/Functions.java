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

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.types.LongValue;

/**
 *
 */
public class Functions {

	private Functions() {}

	/**
	 * A shortest path of length {@code n} contains two shortest paths of
	 * length {@code n-1}. Terminate when no new shortest paths are possible.
	 */
	protected static final class Convergence
	implements ConvergenceCriterion<LongValue> {
		@Override
		public boolean isConverged(int iteration, LongValue value) {
			return value.getValue() < 2;
		}
	}
}
