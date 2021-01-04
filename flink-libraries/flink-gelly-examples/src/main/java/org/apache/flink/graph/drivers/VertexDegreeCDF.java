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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.apache.commons.math3.random.JDKRandomGenerator;

import java.text.NumberFormat;
import java.util.List;

/**
 *
 */
public class VertexDegreeCDF {

	public static final int DEFAULT_SCALE = 22;

	public static final int DEFAULT_EDGE_FACTOR = 8;

	public static final boolean DEFAULT_CLIP_AND_FLIP = false;

	public static void main(String[] args) throws Exception {
		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();

		ParameterTool parameters = ParameterTool.fromArgs(args);

		// Generate RMat graph
		int scale = parameters.getInt("scale", DEFAULT_SCALE);
		int edgeFactor = parameters.getInt("edge_factor", DEFAULT_EDGE_FACTOR);

		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

		long vertexCount = 1L << scale;
		long edgeCount = vertexCount * edgeFactor;

		boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

		Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
			.setNoise(true, 0.25f)
			.generate();
//			.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(clipAndFlip));

		DataSet<Tuple2<LongValue, LongValue>> degree = graph
			.run(new VertexDegree<LongValue, NullValue, NullValue>())
			.map(new MapFunction<Vertex<LongValue, LongValue>, Tuple2<LongValue, LongValue>>() {
				Tuple2<LongValue, LongValue> output = new Tuple2<>(null, new LongValue(1));

				@Override
				public Tuple2<LongValue, LongValue> map(Vertex<LongValue, LongValue> value)
						throws Exception {
					output.f0 = value.f1;
					return output;
				}
			})
			.groupBy(0)
				.sum(1)
			.sortPartition(0, Order.ASCENDING)
				.setParallelism(1);

		List<Tuple2<LongValue, LongValue>> output = degree.collect();

		long count = 0;
		long pairCount = 0;
		for (Tuple2<LongValue, LongValue> tuple : output) {
			long d = tuple.f0.getValue();
			long c = tuple.f1.getValue();

			count += c * d;
			pairCount += c * d * (d - 1) / 2;
		}

		System.out.printf("Edges: %d, neighbor pairs: %d\n", count, pairCount);

		long cumulativeCount = 0;
		long cumulativePairCount = 0;
		for (Tuple2<LongValue, LongValue> tuple : output) {
			long d = tuple.f0.getValue();
			long c = tuple.f1.getValue();

			cumulativeCount += c * d;
			cumulativePairCount += c * d * (d - 1) / 2;
			System.out.printf("Degree: %d, count: %d, edge CDF: %.4f, pair CDF: %.4f\n", tuple.f0.getValue(), tuple.f1.getValue(), 100 * cumulativeCount / (double) count, 100 * cumulativePairCount / (double) pairCount);
		}

		JobExecutionResult result = env.getLastJobExecutionResult();

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println("Execution runtime: " + nf.format(result.getNetRuntime()) + " ms");
	}
}
