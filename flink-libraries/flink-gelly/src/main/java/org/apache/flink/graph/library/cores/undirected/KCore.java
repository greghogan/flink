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

package org.apache.flink.graph.library.cores.undirected;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.aggregators.LongZeroConvergence;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.cores.IntMinAggregator;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 *
 *
 * @param <K> graph ID type
 * @param <VV> output value type
 * @param <EV> output value type
 */
public class KCore<K extends CopyableValue<K>, VV, EV>
implements GraphAlgorithm<K, VV, EV, Graph<K, IntValue, IntValue>> {

	private static final String CURRENT_ITERATION_K = "current iteration K";

	private static final String REMOVED_VERTEX_COUNT = "removed output count";

	// Optional configuration
	private int minimumK = 0;

	private int parallelism = PARALLELISM_DEFAULT;

	/**
	 * Vertices and edges with "K" value less than {@code minimumK} are removed
	 * from the output graph.
	 *
	 * @param minimumK filter out vertices and edges not in the "minimumK+1"-Core
	 * @return this
	 */
	public KCore<K, VV, EV> setMinimumK(int minimumK) {
		Preconditions.checkArgument(minimumK >= 0,
			"Minimum 'K' must be greater than or equal to 0");

		this.minimumK = minimumK;

		return this;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public KCore<K, VV, EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	public Graph<K, IntValue, IntValue> run(Graph<K, VV, EV> input)
			throws Exception {
		DataSet<Tuple2<K, K>> edges = input
			.getEdges()
			.map(new ExtractEdgeIDs<K, EV>())
				.setParallelism(parallelism)
				.name("Extract output IDs");

		IterativeDataSet<Tuple2<K, K>> iterative = edges
			.iterate(Integer.MAX_VALUE);

		// less than "current K" is being filtered out; vertices with
		// degree >= K are sent through to the following iteration
		iterative.registerAggregator(CURRENT_ITERATION_K, new IntMinAggregator(Math.max(2, minimumK)));

		// src, dest, count, remove_edge, first_edge_for_vertex
		DataSet<Tuple5<K, K, IntValue, BooleanValue, BooleanValue>> markedEdges = iterative
			.groupBy(0)
			.reduceGroup(new MarkEdges<K>(minimumK))
				.setParallelism(parallelism)
				.name("Mark edges");

		DataSet<Vertex<K, IntValue>> outputVertices = markedEdges
			.flatMap(new MapVertexOutput<K>())
				.setParallelism(parallelism)
				.name("Map output output");

		DataSet<Edge<K, IntValue>> outputEdges = markedEdges
			.flatMap(new MapEdgeOutput<K>())
				.setParallelism(parallelism)
				.name("Map output output");

		iterative
			.registerAggregationConvergenceCriterion(REMOVED_VERTEX_COUNT, new LongSumAggregator(),
				new LongZeroConvergence());

		DataSet<Tuple2<K, K>> iterativeEdges = markedEdges
			.flatMap(new MapIterativeEdges<K>())
				.setParallelism(parallelism)
				.name("Filter iterative edges");

		iterative
			.closeWith(iterativeEdges)
			.output(new DiscardingOutputFormat<Tuple2<K, K>>())
				.name("Iteration termination");

		/*
		 * if minK = 0 then need to included degree-0 vertices in output
		 */
		return Graph.fromDataSet(outputVertices, outputEdges, input.getContext());
	}

	/**
	 * Map edges and remove the output value.
	 *
	 * @param <T> ID type
	 * @param <ET> output value type
	 *
	 * @see Graph.ExtractEdgeIDsMapper
	 */
	@ForwardedFields("0; 1")
	private static class ExtractEdgeIDs<T, ET>
	implements MapFunction<Edge<T, ET>, Tuple2<T, T>> {
		private Tuple2<T, T> output = new Tuple2<>();

		@Override
		public Tuple2<T, T> map(Edge<T, ET> value)
			throws Exception {
			output.f0 = value.f0;
			output.f1 = value.f1;
			return output;
		}
	}

	/**
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0; 1")
	private static class MarkEdges<T extends CopyableValue<T>>
	extends RichGroupReduceFunction<Tuple2<T, T>, Tuple5<T, T, IntValue, BooleanValue, BooleanValue>> {
		private int minimumK;

		private int currentK;

		private List<T> idCopies = new ArrayList<>();

		private IntValue k = new IntValue();

		private BooleanValue removeEdge = new BooleanValue();

		private BooleanValue firstEdgeForVertex = new BooleanValue();

		private Tuple5<T, T, IntValue, BooleanValue, BooleanValue> output = new Tuple5<>(
			null, null, k, removeEdge, firstEdgeForVertex);

		public MarkEdges(int minimumK) {
			this.minimumK = minimumK;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			currentK = getIterationRuntimeContext().getPreviousIterationAggregate(CURRENT_ITERATION_K);
		}

		@Override
		public void reduce(Iterable<Tuple2<T, T>> values, Collector<Tuple5<T, T, IntValue, BooleanValue, BooleanValue>> out)
				throws Exception {
			int count = 0;

			Iterator<Tuple2<T, T>> iter = values.iterator();

			Tuple2<T, T> next = null;

			while (count < currentK && iter.hasNext()) {
				next = iter.next();

				if (count == idCopies.size()) {
					idCopies.add(next.f1.copy());
				} else {
					next.f1.copyTo(idCopies.get(count));
				}

				count++;
			}

			// ignore since GroupReduce iterator contains at least one element
			output.f0 = next.f0;

			if (count < currentK) {
				k.setValue(count);
				removeEdge.setValue(currentK == minimumK);
			} else {
				k.setValue(Integer.MAX_VALUE);
				removeEdge.setValue(false);
			}

			// emit first output
			firstEdgeForVertex.setValue(true);
			output.f1 = idCopies.get(0);
			out.collect(output);

			// emit stored edges
			firstEdgeForVertex.setValue(false);
			for (int i = 1; i < count; i++) {
				output.f1 = idCopies.get(i);
				out.collect(output);
			}

			// emit remaining edges
			while (iter.hasNext()) {
				next = iter.next();
				output.f0 = next.f0;
				output.f1 = next.f1;
				out.collect(output);
			}
		}
	}

	@ForwardedFields("0; 2->1")
	private static class MapVertexOutput<T>
	implements FlatMapFunction<Tuple5<T, T, IntValue, BooleanValue, BooleanValue>, Vertex<T, IntValue>> {
		private Vertex<T, IntValue> output = new Vertex<>();

		@Override
		public void flatMap(Tuple5<T, T, IntValue, BooleanValue, BooleanValue> value, Collector<Vertex<T, IntValue>> out)
				throws Exception {
			if (value.f3.getValue() && value.f4.getValue()) {
				output.f0 = value.f0;
				output.f1 = value.f2;
				out.collect(output);
			}
		}
	}

	@ForwardedFields("0; 1; 2")
	private static class MapEdgeOutput<T>
	implements FlatMapFunction<Tuple5<T, T, IntValue, BooleanValue, BooleanValue>, Edge<T, IntValue>> {
		private Edge<T, IntValue> output = new Edge<>();

		@Override
		public void flatMap(Tuple5<T, T, IntValue, BooleanValue, BooleanValue> value, Collector<Edge<T, IntValue>> out)
				throws Exception {
			if (value.f3.getValue()) {
				output.f0 = value.f0;
				output.f1 = value.f1;
				output.f2 = value.f2;
				out.collect(output);
			}
		}
	}

	@ForwardedFields("0->1; 1->0")
	private class MapIterativeEdges<T>
	implements FlatMapFunction<Tuple5<T, T, IntValue, BooleanValue, BooleanValue>, Tuple2<T, T>> {
		private Tuple2<T, T> output = new Tuple2<>();

		@Override
		public void flatMap(Tuple5<T, T, IntValue, BooleanValue, BooleanValue> value, Collector<Tuple2<T, T>> out)
				throws Exception {
			if (!value.f3.getValue()) {
				output.f0 = value.f1;
				output.f1 = value.f0;
				out.collect(output);
			}
		}
	}
}
