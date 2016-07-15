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

package org.apache.flink.benchmark.utils;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.util.AbstractID;

public class ChecksumHashCode<IN>
extends DataSetAnalyticBase<IN, Utils.ChecksumHashCode> {

	private final ExecutionEnvironment env;

	private String abstractID = new AbstractID().toString();

	public ChecksumHashCode(ExecutionEnvironment env, DataSet<IN> input)
			throws Exception {
		this.env = env;

		input
			.output(new Utils.ChecksumHashCodeHelper<IN>(abstractID))
				.name("ChecksumHashCode");
	}

	public Utils.ChecksumHashCode getResult() {
		JobExecutionResult res = env.getLastJobExecutionResult();
		return res.getAccumulatorResult(abstractID);
	}
}
