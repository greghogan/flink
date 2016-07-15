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

package org.apache.flink.benchmark;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.library.AdamicAdar;
import org.apache.flink.benchmark.library.HITS;
import org.apache.flink.benchmark.library.JaccardIndex;
import org.apache.flink.benchmark.library.KitchenSink;
import org.apache.flink.benchmark.library.LocalClusteringCoefficientDirected;
import org.apache.flink.benchmark.library.LocalClusteringCoefficientUndirected;
import org.apache.flink.benchmark.library.TriangleListingDirected;
import org.apache.flink.benchmark.library.TriangleListingUndirected;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.client.JobCancellationException;

import java.io.StringWriter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

/*
 * TODO:
 *   restart benchmarks in progress
 *   compare results between multiple files
 *   capture more metadata such as bytes and records per subtask
 *   seed graphs
 *   verify checksums for known inputs
 */
public class Runner {

	private static int SAMPLES = 8;

	private static final Map<String, Class> AVAILABLE_ALGORITHMS;
	static
	{
		AVAILABLE_ALGORITHMS = new LinkedHashMap<>();
		AVAILABLE_ALGORITHMS.put("AdamicAdar".toLowerCase(), AdamicAdar.class);
		AVAILABLE_ALGORITHMS.put("HITS".toLowerCase(), HITS.class);
		AVAILABLE_ALGORITHMS.put("JaccardIndex".toLowerCase(), JaccardIndex.class);
		AVAILABLE_ALGORITHMS.put("KitchenSink".toLowerCase(), KitchenSink.class);
		AVAILABLE_ALGORITHMS.put("LocalClusteringCoefficientDirected".toLowerCase(), LocalClusteringCoefficientDirected.class);
		AVAILABLE_ALGORITHMS.put("LocalClusteringCoefficientUndirected".toLowerCase(), LocalClusteringCoefficientUndirected.class);
		AVAILABLE_ALGORITHMS.put("TriangleListingDirected".toLowerCase(), TriangleListingDirected.class);
		AVAILABLE_ALGORITHMS.put("TriangleListingUndirected".toLowerCase(), TriangleListingUndirected.class);
	}

	private static void printUsage() {
		System.out.println(WordUtils.wrap("Apache Flink macro-benchmarking runner.", 80));
		System.out.println();
		System.out.println("usage: Runner -p <parallelism> --types <all | type0[,type1[,...]]> --algorithms <all | alg0[=ratio0][,alg1[=ratio1][,...]]");
		System.out.println();
		System.out.println("types:");
		System.out.println("  int");
		System.out.println("  long");
		System.out.println("  string");
		System.out.println();
		System.out.println("algorithms:");
		for (Map.Entry<String, Class> entry : AVAILABLE_ALGORITHMS.entrySet()) {
			System.out.println("  " + entry.getValue().getSimpleName());
		}
	}

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.getConfig().disableSysoutLogging();

		ParameterTool parameters = ParameterTool.fromArgs(args);

		if (! (parameters.has("p") && parameters.has("types") && parameters.has("algorithms")) ) {
			printUsage();
			System.exit(-1);
		}

		int parallelism = parameters.getInt("p");
		env.setParallelism(parallelism);

		Set<IdType> types = new HashSet<IdType>();

		if (parameters.get("types").equals("all")) {
			types.add(IdType.INT);
			types.add(IdType.LONG);
			types.add(IdType.STRING);
		} else {
			for (String type : parameters.get("types").split(",")) {
				if (type.toLowerCase().equals("int")) {
					types.add(IdType.INT);
				} else if (type.toLowerCase().equals("long")) {
					types.add(IdType.LONG);
				} else if (type.toLowerCase().equals("string")) {
					types.add(IdType.STRING);
				} else {
					printUsage();
					throw new RuntimeException("Unknown type: " + type);
				}
			}
		}

		Queue<RunnerWithScore> queue = new PriorityQueue<>();

		if (parameters.get("algorithms").equals("all")) {
			for (Map.Entry<String, Class> entry : AVAILABLE_ALGORITHMS.entrySet()) {
				for (IdType type : types) {
					AlgorithmRunner runner = (AlgorithmRunner) entry.getValue().newInstance();
					runner.initialize(type, SAMPLES, parallelism);
					runner.warmup(env);
					queue.add(new RunnerWithScore(runner, 1.0));
				}
			}
		} else {
			for (String algorithm : parameters.get("algorithms").split(",")) {
				double ratio = 1.0;
				if (algorithm.contains("=")) {
					String[] split = algorithm.split("=");
					algorithm = split[0];
					ratio = Double.parseDouble(split[1]);
				}

				if (AVAILABLE_ALGORITHMS.containsKey(algorithm.toLowerCase())) {
					Class clazz = AVAILABLE_ALGORITHMS.get(algorithm.toLowerCase());

					for (IdType type : types) {
						AlgorithmRunner runner = (AlgorithmRunner) clazz.newInstance();
						runner.initialize(type, SAMPLES, parallelism);
						runner.warmup(env);
						queue.add(new RunnerWithScore(runner, ratio));
					}
				} else {
					printUsage();
					throw new RuntimeException("Unknown algorithm: " + algorithm);
				}
			}
		}

		JsonFactory factory = new JsonFactory();

		while (queue.size() > 0) {
			RunnerWithScore current = queue.poll();
			AlgorithmRunner runner = current.getRunner();

			StringWriter writer = new StringWriter();
			JsonGenerator gen = factory.createGenerator(writer);
			gen.writeStartObject();
			gen.writeStringField("algorithm", runner.getClass().getSimpleName());

			boolean running = true;

			while (running) {
				try {
					runner.run(env, gen);
					running = false;
				} catch(ProgramInvocationException e) {
					// only suppress job cancellations
					if (! (e.getCause() instanceof JobCancellationException)) {
						throw e;
					}
				}
			}

			JobExecutionResult result = env.getLastJobExecutionResult();

			long runtime_ms = result.getNetRuntime();
			gen.writeNumberField("runtime_ms", runtime_ms);
			current.credit(runtime_ms);

			if (! runner.finished()) {
				queue.add(current);
			}

			gen.writeObjectFieldStart("accumulators");
			for (Map.Entry<String, Object> accumulatorResult : result.getAllAccumulatorResults().entrySet()) {
				gen.writeStringField(accumulatorResult.getKey(), accumulatorResult.getValue().toString());
			}
			gen.writeEndObject();

			gen.writeEndObject();
			gen.close();
			System.out.println(writer.toString());
		}
	}

	private static class RunnerWithScore
	implements Comparable<RunnerWithScore> {
		private AlgorithmRunner runner;

		private double ratio;

		private double credits;

		public RunnerWithScore(AlgorithmRunner runner, double ratio) {
			this.runner = runner;
			this.ratio = ratio;
		}

		public AlgorithmRunner getRunner() {
			return runner;
		}

		public void credit(float seconds) {
			credits += seconds / ratio;
		}

		@Override
		public int compareTo(RunnerWithScore other) {
			return Double.compare(credits, other.credits);
		}
	}
}
