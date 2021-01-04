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

package org.apache.flink.graph.library.cores;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.types.LongValue;

/**
 * An {@link Aggregator} that computes the minimum of long values.
 */
@SuppressWarnings("serial")
public class LongMinAggregator implements Aggregator<LongValue> {

	private long min;

	private long initialValue;

	public LongMinAggregator() {}

	public LongMinAggregator(long initialValue) {
		this.min = this.initialValue = initialValue;
	}

	@Override
	public LongValue getAggregate() {
		return new LongValue(min);
	}

	@Override
	public void aggregate(LongValue value) {
		min = Math.min(min, value.getValue());
	}

	/**
	 * Adds the given value to the current aggregate.
	 *
	 * @param value The value to add to the aggregate.
	 */
	public void aggregate(long value) {
		min = Math.min(min, value);
	}

	@Override
	public void reset() {
		min = initialValue;
	}
}
