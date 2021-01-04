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

package org.apache.flink.graph.library.components;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 *
 */
public class ConnectedComponents<K extends Comparable<K> & CopyableValue<K>, VV, EV>
implements GraphAlgorithm<K, VV, EV, DataSet<Tuple2<K, K>>> {

	@Override
	public DataSet<Tuple2<K, K>> run(Graph<K, VV, EV> input) {
		DataSet<Tuple2<K, K>> edgePartitions = input
			.getEdges()
			.map(new DefaultPartitions<K, EV>())
				.name("Default partitions");

		IterativeDataSet<Tuple2<K, K>> iterative = edgePartitions
			.iterate(Integer.MAX_VALUE);

		DataSet<Tuple2<K, K>> updatedLabels = iterative
			.groupBy(0)
			.sortGroup(1, Order.ASCENDING)
			.reduceGroup(new UpdateLabels<K>())
				.name("Connected components");

		iterative.registerAggregationConvergenceCriterion("neighbor edge count", new LongSumAggregator(), new VertexConvergence());

		DataSet<Tuple2<K, K>> vertexLabels = iterative.closeWith(updatedLabels);

		DataSet<Tuple2<K, K>> connectedComponents = vertexLabels
			.filter(new FilterVertexLabels<K>())
				.name("Filter vertex labels");

		return connectedComponents;
	}

	@ForwardedFields("0; 1")
	private static class DefaultPartitions<T, EV>
	implements MapFunction<Edge<T, EV>, Tuple2<T, T>> {
		private static final long serialVersionUID = 1L;

		private Tuple2<T, T> output = new Tuple2<>(null, null);

		@Override
		public Tuple2<T, T> map(Edge<T, EV> value) throws Exception {
			output.f0 = value.f0;
			output.f1 = value.f1;
			return output;
		}
	}

	private static class UpdateLabels<T extends Comparable<T> & CopyableValue<T>>
	extends RichGroupReduceFunction<Tuple2<T, T>, Tuple2<T, T>> {
		private static final long serialVersionUID = 1L;

		private LongSumAggregator counter;

		private T newLabel;

		private Tuple2<T, T> output = new Tuple2<>();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			counter = getIterationRuntimeContext().getIterationAggregator("neighbor edge count");
		}

		@Override
		public void reduce(Iterable<Tuple2<T, T>> values, Collector<Tuple2<T, T>> out)
				throws Exception {
			Iterator<Tuple2<T, T>> iter = values.iterator();

			Tuple2<T, T> first = iter.next();

			T b = first.f0;
			T a = first.f1;

			if (a.compareTo(b) < 0) {
				// (b, a)
				out.collect(first);

				first.f0 = a;
				first.f1 = b;

				// (a, b)
				out.collect(first);

				if (newLabel == null) {
					newLabel = output.f1 = a.copy();
				} else {
					a.copyTo(newLabel);
				}

				while (iter.hasNext()) {
					output.f0 = iter.next().f1;

					// (c, a)
					out.collect(output);

					this.counter.aggregate(1);
				}
			}
		}
	}

	private static class VertexConvergence
	implements ConvergenceCriterion<LongValue> {
		@Override
		public boolean isConverged(int iteration, LongValue value) {
			return (value.getValue() == 0);
		}
	}

	@ForwardedFields("0; 1")
	private static class FilterVertexLabels<T extends Comparable<T>>
	implements FilterFunction<Tuple2<T, T>> {
		@Override
		public boolean filter(Tuple2<T, T> value) throws Exception {
			return (value.f0.compareTo(value.f1) > 0);
		}
	}
}
