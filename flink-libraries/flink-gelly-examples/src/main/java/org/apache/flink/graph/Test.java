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

package org.apache.flink.graph;

import org.apache.flink.graph.drivers.output.Modules;
import org.apache.flink.graph.library.similarity.JaccardIndex;
import org.apache.flink.graph.library.similarity.JaccardIndex.Result;
import org.apache.flink.types.IntValue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

/**
 *
 */
public class Test {

	/**
	 *
	 */
	public static void main(String[] args) {
		JaccardIndex.Result<IntValue> result = new Result<>();
		result.setVertexId0(new IntValue(0));
		result.setVertexId1(new IntValue(1));
		result.setSharedNeighborCount(new IntValue(2));
		result.setDistinctNeighborCount(new IntValue(3));

		ObjectMapper mapper = new XmlMapper();
//		SimpleModule module = new SimpleModule("IntValueSerializer", Version.unknownVersion());
//		module.addSerializer(new IntValueSerializer());
		mapper.registerModule(new Modules.Concise());
		mapper.disable(MapperFeature.DEFAULT_VIEW_INCLUSION);

		try {
			System.out.println(mapper.writeValueAsString(result));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		ObjectMapper mapper2 = new ObjectMapper();
//		SimpleModule module = new SimpleModule("IntValueSerializer", Version.unknownVersion());
//		module.addSerializer(new IntValueSerializer());
		mapper2.registerModule(new Modules.Verbose());
		mapper2.disable(MapperFeature.DEFAULT_VIEW_INCLUSION);

		try {
			System.out.println(mapper2.writeValueAsString(result));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		CsvMapper m = new CsvMapper();
		m.registerModule(new MyModule());
		m.addMixIn(JaccardIndex.Result.class, JaccardIndexResultMixin.class);
		CsvSchema schema = m.schemaFor(JaccardIndexResultMixin.class);

		try {
			System.out.println(m.writer(schema).writeValueAsString(result));
		} catch (com.fasterxml.jackson.core.JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	/**
	 *
	 */
	public interface Views {
		/**
		 *
		 */
		class Concise {}

		/**
		 *
		 */
		class Verbose extends Concise {}
	}

	/**
	 *
	 */
	public static class MyModule extends SimpleModule {
		public MyModule() {
			super("MyModule", Version.unknownVersion());
//			this.addSerializer(new IntValueSerializer());
		}

		/**
		 *
		 */
		@Override
		public void setupModule(SetupContext context) {
			super.setupModule(context);

			context.setMixInAnnotations(JaccardIndex.Result.class, JaccardIndexResultMixin.class);
			context.setMixInAnnotations(IntValue.class, IntValueMixin.class);

//			this.addSerializer(new IntValueSerializer());
		}
	}

	/**
	 *
	 */
	@JsonPropertyOrder({"vertexId0", "vertexId1", "sharedNeighborCount", "distinctNeighborCount", "jaccardIndexScore"})
//	@JsonIgnoreProperties({"jaccardIndexScore"})
	public abstract class JaccardIndexResultMixin<K> {
		@JsonProperty @JsonView(Views.Concise.class) K vertexId0;
		@JsonProperty @JsonView(Views.Concise.class) K vertexId1;
		@JsonProperty @JsonView(Views.Concise.class) IntValue sharedNeighborCount;
		@JsonProperty @JsonView(Views.Concise.class) IntValue distinctNeighborCount;
		@JsonProperty @JsonView(Views.Verbose.class) double jaccardIndexScore;
	}

	/**
	 *
	 */
	public interface IntValueMixin {
		@JsonValue
		int getValue();
	}

//	public static class IntValueSerializer extends StdSerializer<IntValue> {
//		public IntValueSerializer() {
//			super(IntValue.class);
//		}
//
//		@Override
//		public void serialize(IntValue intValue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
//			jsonGenerator.writeNumber(intValue.getValue());
//		}
//	}
}
