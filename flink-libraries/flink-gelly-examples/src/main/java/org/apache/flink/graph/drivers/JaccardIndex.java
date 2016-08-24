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

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.bound.BinaryMaximum;
import org.apache.flink.graph.asm.bound.BinaryMinimum;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.drivers.output.CSV;
import org.apache.flink.graph.drivers.output.Hash;
import org.apache.flink.graph.drivers.output.Print;
import org.apache.flink.graph.drivers.parameter.Bound;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.graph.library.similarity.JaccardIndex.Result;
import org.apache.flink.graph.types.valuearray.ValueArray;
import org.apache.flink.types.CopyableValue;

import java.util.List;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Driver for {@link org.apache.flink.graph.library.similarity.JaccardIndex}.
 */
public class JaccardIndex<K extends CopyableValue<K>, VV, EV>
extends SimpleDriver<Result<K>>
implements Driver<K, VV, EV>, CSV, Hash, Print {

	private LongParameter minNumerator = new LongParameter(this, "minimum_numerator")
		.setDefaultValue(0);

	private LongParameter minDenominator = new LongParameter(this, "minimum_denominator")
		.setDefaultValue(1);

	private LongParameter maxNumerator = new LongParameter(this, "maximum_numerator")
		.setDefaultValue(1);

	private LongParameter maxDenominator = new LongParameter(this, "maximum_denominator")
		.setDefaultValue(0);

	private LongParameter littleParallelism = new LongParameter(this, "little_parallelism")
		.setDefaultValue(PARALLELISM_DEFAULT);

	private Bound bound = new Bound(this);

	private DataSet<Result<K>> result;

	private DataSet<Tuple2<Result<K>, ValueArray<K>>> boundedResult;

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String getShortDescription() {
		return "similarity score as fraction of common neighbors";
	}

	@Override
	public String getLongDescription() {
		return WordUtils.wrap(new StrBuilder()
			.appendln("Jaccard Index measures the similarity between vertex neighborhoods and " +
				"is computed as the number of shared neighbors divided by the number of " +
				"distinct neighbors. Scores range from 0.0 (no shared neighbors) to 1.0 (all " +
				"neighbors are shared).")
			.appendNewLine()
			.append("The result contains two vertex IDs, the number of shared neighbors, and " +
				"the number of distinct neighbors.")
			.toString(), 80);
	}

	@Override
	public void plan(Graph<K, VV, EV> graph) throws Exception {
		int lp = littleParallelism.getValue().intValue();

		result = graph
			.run(new org.apache.flink.graph.library.similarity.JaccardIndex<K, VV, EV>()
				.setMinimumScore(minNumerator.getValue().intValue(), minDenominator.getValue().intValue())
				.setMaximumScore(maxNumerator.getValue().intValue(), maxDenominator.getValue().intValue())
				.setLittleParallelism(lp));

		switch(bound.getValue()) {
			case MINIMUM:
				boundedResult = result
					.runOperation(new BinaryMinimum<K, Result<K>>());
				break;

			case MAXIMUM:
				boundedResult = result
					.runOperation(new BinaryMaximum<K, Result<K>>());
				break;
		}
	}

	@Override
	public void print(String executionName) throws Exception {
		switch(bound.getValue()) {
			case NONE: {
				Collect<Result<K>> collector = new Collect<>();

				// Refactored due to openjdk7 compile error: https://travis-ci.org/greghogan/flink/builds/200487761
				List<Result<K>> records = collector.run(result).execute(executionName);

				for (Result<K> result : records) {
					System.out.println(result);
				}
			} break;

			case MINIMUM:
			case MAXIMUM: {
				Collect<Tuple2<Result<K>, ValueArray<K>>> collector = new Collect<>();

				// Refactored due to openjdk7 compile error: https://travis-ci.org/greghogan/flink/builds/200487761
				List<Tuple2<Result<K>, ValueArray<K>>> records = collector.run(boundedResult).execute(executionName);

				for (Tuple2<Result<K>, ValueArray<K>> result : records) {
					System.out.println(result);
				}
			} break;

			default:
				throw new RuntimeException("Unknown bound: " + bound);
		}
	}

	@Override
	public void writeCSV(String filename, String lineDelimiter, String fieldDelimiter) {
		switch(bound.getValue()) {
			case NONE:
				result
					.writeAsCsv(filename, lineDelimiter, fieldDelimiter);
				break;

			case MINIMUM:
			case MAXIMUM:
				boundedResult
					.writeAsCsv(filename, lineDelimiter, fieldDelimiter);
				break;

			default:
				throw new RuntimeException("Unknown bound: " + bound);
		}
	}
}
