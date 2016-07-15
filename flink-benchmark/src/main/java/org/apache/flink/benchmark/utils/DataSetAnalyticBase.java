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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;

public abstract class DataSetAnalyticBase<IN, T>
implements DataSetAnalytic<IN, T> {

	protected ExecutionEnvironment env;

	@Override
	public DataSetAnalyticBase<IN, T> run(ExecutionEnvironment env, DataSet<IN> input)
			throws Exception {
		this.env = env;
		return null;
	}

	@Override
	public T execute(ExecutionEnvironment env)
			throws Exception {
		Preconditions.checkNotNull(env);

		this.env = env;
		env.execute();
		return getResult();
	}

	@Override
	public T execute(ExecutionEnvironment env, String jobName)
			throws Exception {
		Preconditions.checkNotNull(jobName);
		Preconditions.checkNotNull(env);

		this.env = env;
		env.execute(jobName);
		return getResult();
	}
}
