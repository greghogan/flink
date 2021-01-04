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

import org.apache.flink.graph.drivers.JaccardIndex.JaccardIndexResultMixinConcise;
import org.apache.flink.graph.drivers.JaccardIndex.JaccardIndexResultMixinVerbose;
import org.apache.flink.graph.drivers.output.Mixins.BooleanValueMixin;
import org.apache.flink.graph.drivers.output.Mixins.ByteValueMixin;
import org.apache.flink.graph.drivers.output.Mixins.CharValueMixin;
import org.apache.flink.graph.drivers.output.Mixins.DoubleValueMixin;
import org.apache.flink.graph.drivers.output.Mixins.FloatValueMixin;
import org.apache.flink.graph.drivers.output.Mixins.IntValueMixin;
import org.apache.flink.graph.drivers.output.Mixins.LongValueMixin;
import org.apache.flink.graph.drivers.output.Mixins.ShortValueMixin;
import org.apache.flink.graph.drivers.output.Mixins.StringValueMixin;
import org.apache.flink.graph.library.similarity.JaccardIndex;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 *
 */
public class Modules {

	/**
	 *
	 */
	public abstract static class Standard extends SimpleModule {
		private static final long serialVersionUID = 1L;

		private Standard(String name) {
			super(name, Version.unknownVersion());
		}

		/**
		 *
		 * @param context
		 */
		@Override
		public void setupModule(SetupContext context) {
			super.setupModule(context);

			context.setMixInAnnotations(BooleanValue.class, BooleanValueMixin.class);
			context.setMixInAnnotations(ByteValue.class, ByteValueMixin.class);
			context.setMixInAnnotations(CharValue.class, CharValueMixin.class);
			context.setMixInAnnotations(ShortValue.class, ShortValueMixin.class);
			context.setMixInAnnotations(IntValue.class, IntValueMixin.class);
			context.setMixInAnnotations(LongValue.class, LongValueMixin.class);
			context.setMixInAnnotations(FloatValue.class, FloatValueMixin.class);
			context.setMixInAnnotations(DoubleValue.class, DoubleValueMixin.class);
			context.setMixInAnnotations(StringValue.class, StringValueMixin.class);
		}
	}

	/**
	 *
	 */
	public static class Concise extends Standard {
		private static final long serialVersionUID = 1L;

		public Concise() {
			super("Concise");
		}

		@Override
		public void setupModule(SetupContext context) {
			super.setupModule(context);

			context.setMixInAnnotations(JaccardIndex.Result.class, JaccardIndexResultMixinConcise.class);
		}
	}

	/**
	 *
	 */
	public static class Verbose extends Standard {
		private static final long serialVersionUID = 1L;

		public Verbose() {
			super("Verbose");
		}

		@Override
		public void setupModule(SetupContext context) {
			super.setupModule(context);

			context.setMixInAnnotations(JaccardIndex.Result.class, JaccardIndexResultMixinVerbose.class);
		}
	}
}
