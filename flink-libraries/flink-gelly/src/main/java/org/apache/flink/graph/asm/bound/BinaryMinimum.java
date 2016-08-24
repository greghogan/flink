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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.asm.result.BinaryResult;
import org.apache.flink.graph.types.valuearray.ValueArray;
import org.apache.flink.graph.types.valuearray.ValueArrayTypeInfo;
import org.apache.flink.types.Value;

public class BinaryMinimum<K extends Value, RK extends BinaryResult<K> & Comparable<RK>>
implements CustomUnaryOperation<RK, Tuple2<RK, ValueArray<K>>> {

	private DataSet<RK> input;

	@Override
	public void setInput(DataSet<RK> input) {
		this.input = input;
	}

	@Override
	public DataSet<Tuple2<RK, ValueArray<K>>> createResult() {
		TypeInformation<RK> resultType = input.getType();

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) resultType).getTypeAt(0);
		ValueArrayTypeInfo<K> arrayType = new ValueArrayTypeInfo<>(keyType);

		TypeInformation<Tuple2<RK, ValueArray<K>>> returnType = new TupleTypeInfo<>(resultType, arrayType);

		return input
			.map(new Functions.MapToValueArray<K, RK>())
				.returns(returnType)
				.name("Map to ValueArray")
			.groupBy("0.0")
			.reduce(new ReduceMinimum<K, RK>())
				.setCombineHint(CombineHint.HASH)
				.returns(returnType)
				.name("Reduce minimum");
	}

	/**
	 *
	 * @param <T>
	 * @param <RT>
	 */
	private static class ReduceMinimum<T, RT extends BinaryResult<T> & Comparable<RT>>
	implements ReduceFunction<Tuple2<RT, ValueArray<T>>> {
		@Override
		public Tuple2<RT, ValueArray<T>> reduce(Tuple2<RT, ValueArray<T>> value1, Tuple2<RT, ValueArray<T>> value2)
				throws Exception {
			int cmp = value1.f0.compareTo(value2.f0);

			if (cmp < 0) {
				return value1;
			} else if (cmp > 0) {
				return value2;
			} else {
				value1.f1.add(value2.f0.getVertexId1());
				value1.f1.addAll(value2.f1);
				return value1;
			}
		}
	}
}
