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

package org.apache.flink.benchmark.library;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.benchmark.AlgorithmRunner;
import org.apache.flink.benchmark.IdType;

import java.util.HashMap;
import java.util.Map;

public abstract class RMatAlgorithmRunner
implements AlgorithmRunner {

	protected IdType idType;

	private int samples;

	private int lowScale;

	private int currentScale;

	private Map<Integer, Integer> executions = new HashMap<>();

	protected abstract int getInitialScale(int parallelism);

	@Override
	public void initialize(IdType idType, int samples, int parallelism) {
		this.idType = idType;
		this.samples = samples;

		this.lowScale = getInitialScale(parallelism);
		this.currentScale = this.lowScale;
	}

	@Override
	public void warmup(ExecutionEnvironment env)
			throws Exception {
		runInternal(env, 10);
	}

	protected abstract void runInternal(ExecutionEnvironment env, int scale) throws Exception;

	@Override
	public void run(ExecutionEnvironment env, JsonGenerator json)
			throws Exception {
		runInternal(env, currentScale);

		json.writeStringField("idType", idType.toString());
		json.writeNumberField("scale", currentScale);

		Integer count = executions.get(currentScale);
		if (count == null) {
			count = 0;
		}
		count++;
		executions.put(currentScale, count);

		if (count == samples) {
			lowScale++;
			currentScale = lowScale;
		} else {
			if (count % 2 == 0) {
				currentScale++;
			} else {
				currentScale = lowScale;
			}
		}
	}

	@Override
	public boolean finished() {
		return idType.equals(IdType.INT) && lowScale > 32;
	}
}
