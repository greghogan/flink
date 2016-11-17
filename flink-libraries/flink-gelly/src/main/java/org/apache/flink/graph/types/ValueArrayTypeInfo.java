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

package org.apache.flink.graph.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.CopyableValueComparator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Type information for data types that extend the {@link ValueArray} interface. This
 * interface allows types to define their custom serialization and deserialization routines.
 *
 * @param <T> The type of the class represented by this type information.
 */
@PublicEvolving
public class ValueArrayTypeInfo<T extends ValueArray> extends TypeInformation<T> implements AtomicType<T> {

	private static final long serialVersionUID = 1L;

	private final TypeInformation<T> type;

	private final Class<T> typeClass;

	public ValueArrayTypeInfo(TypeInformation<T> typeInfo) {
		Preconditions.checkNotNull(typeInfo);

		this.type = typeInfo;
		this.typeClass = typeInfo.getTypeClass();

		Preconditions.checkArgument(
			IntValue.class.isAssignableFrom(typeClass) || LongValue.class.isAssignableFrom(typeClass),
			"ValueArrayTypeInfo can only be used for IntValue or LongValue");
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	@PublicEvolving
	public Class<T> getTypeClass() {
		return this.typeClass;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public boolean isKeyType() {
		return Comparable.class.isAssignableFrom(typeClass);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		if (IntValue.class.isAssignableFrom(typeClass)) {
			return (TypeSerializer<T>) new IntValueArraySerializer();
		} else if (LongValue.class.isAssignableFrom(typeClass)) {
			return (TypeSerializer<T>) new LongValueArraySerializer();
		} else {
			throw new InvalidTypesException("No ValueArray class exists for " + type.getClass());
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
		if (IntValue.class.isAssignableFrom(typeClass)) {
			return (TypeComparator<T>) new CopyableValueComparator(sortOrderAscending, IntValueArray.class);
		} else if (LongValue.class.isAssignableFrom(typeClass)) {
			return (TypeComparator<T>) new CopyableValueComparator(sortOrderAscending, LongValueArray.class);
		} else {
			throw new InvalidTypesException("No ValueArray class exists for " + type.getClass());
		}
	}

	@Override
	public Map<String, TypeInformation<?>> getGenericParameters() {
		Map<String, TypeInformation<?>> m = new HashMap<>(1);
		m.put("T", type);
		return m;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return this.type.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ValueArrayTypeInfo) {
			@SuppressWarnings("unchecked")
			ValueArrayTypeInfo<T> valueTypeInfo = (ValueArrayTypeInfo<T>) obj;

			return valueTypeInfo.canEqual(this) &&
				type == valueTypeInfo.type;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ValueArrayTypeInfo;
	}

	@Override
	public String toString() {
		return "ValueArrayType<" + typeClass.getSimpleName() + ">";
	}
}
