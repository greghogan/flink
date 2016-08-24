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

package org.apache.flink.graph.asm.bound;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.asm.result.BinaryResult;
import org.apache.flink.graph.types.valuearray.ValueArray;
import org.apache.flink.graph.types.valuearray.ValueArrayFactory;
import org.apache.flink.types.Value;

/**
 *
 */
public class Functions {

	private Functions() {}

	/**
	 *
	 * @param <T>
	 * @param <RT>
	 */
	@ForwardedFields("*->0")
	static class MapToValueArray<T extends Value, RT extends BinaryResult<T>>
	implements MapFunction<RT, Tuple2<RT, ValueArray<T>>> {
		private Tuple2<RT, ValueArray<T>> output = new Tuple2<>();

		@Override
		public Tuple2<RT, ValueArray<T>> map(RT value)
				throws Exception {
			if (output.f1 == null) {
				// create empty array
				output.f1 = ValueArrayFactory.createValueArray(value.getVertexId0().getClass());
			}

			output.f0 = value;
			return output;
		}
	}
}
