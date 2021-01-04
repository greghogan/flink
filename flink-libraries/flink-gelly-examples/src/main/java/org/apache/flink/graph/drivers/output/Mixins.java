/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.drivers.output;

import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.Module;

/**
 * Jackson mixins for Flink's native types. The mixin classes are bound to the
 * applicable target class by the {@link Module} and may be reused for common
 * method signatures.
 */
public class Mixins {

	/**
	 * Mixin for {@link BooleanValue}.
	 */
	public interface BooleanValueMixin {
		@JsonValue
		boolean getValue();
	}

	/**
	 * Mixin for {@link ByteValue}.
	 */
	public interface ByteValueMixin {
		@JsonValue
		byte getValue();
	}

	/**
	 * Mixin for {@link CharValue}.
	 */
	public interface CharValueMixin {
		@JsonValue
		char getValue();
	}

	/**
	 * Mixin for {@link ShortValue}.
	 */
	public interface ShortValueMixin {
		@JsonValue
		short getValue();
	}

	/**
	 * Mixin for {@link IntValue}.
	 */
	public interface IntValueMixin {
		@JsonValue
		int getValue();
	}

	/**
	 * Mixin for {@link LongValue}.
	 */
	public interface LongValueMixin {
		@JsonValue
		long getValue();
	}

	/**
	 * Mixin for {@link FloatValue}.
	 */
	public interface FloatValueMixin {
		@JsonValue
		float getValue();
	}

	/**
	 * Mixin for {@link DoubleValue}.
	 */
	public interface DoubleValueMixin {
		@JsonValue
		double getValue();
	}

	/**
	 * Mixin for {@link StringValue}.
	 */
	public interface StringValueMixin {
		@JsonValue
		String getValue();
	}
}
