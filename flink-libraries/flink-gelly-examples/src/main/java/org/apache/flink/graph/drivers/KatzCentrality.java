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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.parameter.DoubleParameter;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.Value;

/**
 * Driver for {@link org.apache.flink.graph.library.centrality.KatzCentrality}.
 */
public class KatzCentrality<K extends Value & Comparable<K>, VV, EV>
extends DriverBase<K, VV, EV> {

	private DoubleParameter alpha = new DoubleParameter(this, "alpha");

	private LongParameter iterations = new LongParameter(this, "iterations");

	private DataSet<Tuple2<K, DoubleValue>> result;

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String getShortDescription() {
		return "KatzCentrality";
	}

	@Override
	public String getLongDescription() {
		return "KatzCentrality";
	}

	@Override
	public DataSet plan(Graph<K, VV, EV> graph) throws Exception {
		return graph
			.run(new org.apache.flink.graph.library.centrality.KatzCentrality<K, VV, EV>(
				alpha.getValue(),
				iterations.getValue().intValue()
			));
	}
}
