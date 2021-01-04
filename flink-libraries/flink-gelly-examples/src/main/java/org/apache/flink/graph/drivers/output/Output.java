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

package org.apache.flink.graph.drivers.output;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.drivers.parameter.BooleanParameter;
import org.apache.flink.graph.drivers.parameter.ChoiceParameter;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;

import java.io.PrintStream;

/**
 * Output writer for a {@link GraphAlgorithm} result.
 */
public class Output<T>
extends ParameterizedBase {

	private ChoiceParameter outputFormat = new ChoiceParameter(this, "output_format")
		.setOptional(true)
		.addChoices("csv", "json");

	private BooleanParameter outputVerbose = new BooleanParameter(this, "output_verbose");

	private BooleanParameter printExecutionPlan = new BooleanParameter(this, "print_execution_plan")
		.setHidden(true);

	/**
	 * Write the output {@link DataSet}.
	 *
	 * @param executionName job name
	 * @param out output printer
	 * @param data the output
	 */
	public void write(String executionName, PrintStream out, DataSet<T> data) throws Exception {
		if (printExecutionPlan.getValue()) {
			out.println();
			out.println(data.getExecutionEnvironment().getExecutionPlan());
		}

	}
}

/**
 * optional execution plan
 * analytics (hash, etc.)
 * configure OutputWriter
 *
 * nothing: hash
 * --quiet (disables printing analytics)
 * --output_format <json|xml|csv> [--verbose]
 * --output_format <json|xml|csv> [--verbose] --output_path <out.json>
 */
