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

package org.apache.flink.graph.library.paths;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.paths.AllPairsShortestPaths.Result;
import org.apache.flink.graph.types.IntValueArray;
import org.apache.flink.graph.types.LongValueArray;
import org.apache.flink.graph.types.ValueArray;
import org.apache.flink.graph.utils.Murmur3_32;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class AllPairsShortestPaths<K extends Comparable<K>, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Result<K>> {

	private static final String NEW_LONGEST_PATHS = "new longest paths";

	// Optional configuration
	private boolean filterPathsByOrder = false;

	/**
	 * In parallel processing frameworks each edge in an undirected graph is
	 * stored twice, both as (u, v) and its twin (v, u). Likewise, a shortest
	 * path (u, v, [w, x, y, z]) will have twin (v, u, [z, y, x, w]).
	 *
	 * By enabling this filter only shortest paths (u, v, [...]) with u < v
	 * are returned.
	 *
	 * @param filterPathsByOrder whether to filter the returned paths
	 * @return this
	 */
	public AllPairsShortestPaths<K, VV, EV> setFilterPathsByOrder(boolean filterPathsByOrder) {
		this.filterPathsByOrder = filterPathsByOrder;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return AllPairsShortestPaths.class.getName();
	}

	@Override
	protected boolean mergeConfiguration(GraphAlgorithmWrappingDataSet other) {
		Preconditions.checkNotNull(other);

		if (! AllPairsShortestPaths.class.isAssignableFrom(other.getClass())) {
			return false;
		}

		AllPairsShortestPaths rhs = (AllPairsShortestPaths) other;

		if (filterPathsByOrder != rhs.filterPathsByOrder) {
			return false;
		}

		return true;
	}

	@Override
	public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// u, v, 0, ValueArray(Ø)
		DataSet<Tuple4<K, K, IntValue, ValueArray<K>>> edges = input
			.getEdges()
			.map(new InitializeEdges<K, EV>())
				.name("Initialize edges");

		IterativeDataSet<Tuple4<K, K, IntValue, ValueArray<K>>> paths = edges
			.iterate(Integer.MAX_VALUE);

		DataSet<Tuple4<K, K, IntValue, ValueArray<K>>> maxLengthPaths = paths
			.filter(new FilterPaths<K>())
				.name("Longest paths");

		DataSet<Tuple4<K, K, IntValue, ValueArray<K>>> shortestPaths = paths
			.join(maxLengthPaths)
			.where(1)
			.equalTo(0)
			.with(new JoinPaths<K>())
				.name("Connect paths")
			.union(paths)
				.name("All paths")
			.groupBy(0, 1)
			.sortGroup(2, Order.ASCENDING)
			.reduceGroup(new FindShortestPaths<K>())
				.name("Find shortest paths");

		paths.registerAggregationConvergenceCriterion(NEW_LONGEST_PATHS, new LongSumAggregator(), new Convergence());

		DataSet<Result<K>> result = paths
			.closeWith(shortestPaths)
			.map(new TranslateResult<K>())
				.name("Map result");

		if (filterPathsByOrder) {
			return result
				.filter(new FilterPathsByOrder<K>())
					.name("Filter paths by order");
		} else {
			return result;
		}
	}

	@ForwardedFields("0; 1")
	private static final class InitializeEdges<T, ET>
	implements MapFunction<Edge<T, ET>, Tuple4<T, T, IntValue, ValueArray<T>>> {
		private Tuple4<T, T, IntValue, ValueArray<T>> output = new Tuple4<>(null, null, new IntValue(1), null);

		@Override
		public Tuple4<T, T, IntValue, ValueArray<T>> map(Edge<T, ET> edge)
				throws Exception {
			if (output.f3 == null) {
				Class cls = edge.f0.getClass();
				if (IntValue.class.isAssignableFrom(cls)) {
					output.f3 = (ValueArray<T>)new IntValueArray();
				} else if (LongValue.class.isAssignableFrom(cls)) {
					output.f3 = (ValueArray<T>)new LongValueArray();
				} else {
					throw new IllegalArgumentException("Unable to create ValueArray for type " + cls);
				}
			}

			output.f0 = edge.f0;
			output.f1 = edge.f1;
			return output;
		}
	}

	private static final class FilterPaths<T>
	extends RichFilterFunction<Tuple4<T, T, IntValue, ValueArray<T>>> {
		private int pathLengthFilter;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			int superstep = getIterationRuntimeContext().getSuperstepNumber();

			// each iteration doubles the length of the largest path
			// and the first iteration processes edges
			pathLengthFilter = 1 << (superstep-1);
		}

		@Override
		public boolean filter(Tuple4<T, T, IntValue, ValueArray<T>> path)
				throws Exception {
			return path.f2.getValue() == pathLengthFilter;
		}
	}

	@ForwardedFieldsFirst("0")
	@ForwardedFieldsSecond("1")
	private class JoinPaths<T extends Comparable<T>>
	implements FlatJoinFunction<Tuple4<T, T, IntValue, ValueArray<T>>, Tuple4<T, T, IntValue, ValueArray<T>>, Tuple4<T, T, IntValue, ValueArray<T>>> {
		private Tuple4<T, T, IntValue, ValueArray<T>> output = new Tuple4<>(null, null, new IntValue(), null);

		@Override
		public void join(Tuple4<T, T, IntValue, ValueArray<T>> first, Tuple4<T, T, IntValue, ValueArray<T>> second, Collector<Tuple4<T, T, IntValue, ValueArray<T>>> out)
				throws Exception {
			if (first.f0.compareTo(second.f1) != 0) {
				output.f0 = first.f0;
				output.f1 = second.f1;
				output.f2.setValue(first.f2.getValue() + second.f2.getValue());

				first.f3.mark();

				output.f3 = first.f3;
				output.f3.add(second.f0);
				output.f3.addAll(second.f3);
				out.collect(output);

				first.f3.reset();
			}
		}
	}

	@ForwardedFields("*")
	private class FindShortestPaths<T>
	extends RichGroupReduceFunction<Tuple4<T, T, IntValue, ValueArray<T>>, Tuple4<T, T, IntValue, ValueArray<T>>> {
		private int maxLength;

		private int maxLengthCount;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			int superstep = getIterationRuntimeContext().getSuperstepNumber();

			// each iteration doubles the length of the largest path
			// and the first iteration processes edges joined with edges
			maxLength = 1 << superstep;
			maxLengthCount = 0;
		}

		@Override
		public void close()
				throws Exception {
			super.close();

			LongSumAggregator agg = getIterationRuntimeContext().getIterationAggregator(NEW_LONGEST_PATHS);
			agg.aggregate(maxLengthCount);
		}

		@Override
		public void reduce(Iterable<Tuple4<T, T, IntValue, ValueArray<T>>> paths, Collector<Tuple4<T, T, IntValue, ValueArray<T>>> out)
				throws Exception {
			int shortestPathLength = Integer.MAX_VALUE;
			int shortestPathCount = 0;

			for(Tuple4<T, T, IntValue, ValueArray<T>> path : paths) {
				int pathLength = path.f2.getValue();

				if (pathLength > shortestPathLength) {
					break;
				}

				shortestPathLength = pathLength;
				out.collect(path);
				shortestPathCount++;
			}

			if (shortestPathLength == maxLength) {
				maxLengthCount += shortestPathCount;
			}
		}
	}

	private static class Convergence
	implements ConvergenceCriterion<LongValue> {
		@Override
		public boolean isConverged(int iteration, LongValue value) {
			return value.getValue() < 2;
		}
	}

	@ForwardedFields("0")
	private static class TranslateResult<T>
	implements MapFunction<Tuple4<T, T, IntValue, ValueArray<T>>, Result<T>> {
		private Result<T> output = new Result<>();

		@Override
		public Result<T> map(Tuple4<T, T, IntValue, ValueArray<T>> value)
				throws Exception {
			output.f0 = value.f0;
			output.f1 = value.f1;
			output.f2 = value.f2;
			output.f3 = value.f3;
			return output;
		}
	}

	private static class FilterPathsByOrder<T extends Comparable<T>>
	implements FilterFunction<Result<T>> {
		@Override
		public boolean filter(Result<T> value)
				throws Exception {
			return value.f0.compareTo(value.f1) < 0;
		}
	}

	/**
	 * Wraps the results from the AllPairsShortestPaths algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends Tuple4<T, T, IntValue, ValueArray<T>> {
		public static final int HASH_SEED = 0xa3a03a40;

		private Murmur3_32 hasher = new Murmur3_32(HASH_SEED);

		/**
		 * Get the source vertex ID.
		 *
		 * @return source vertex ID
		 */
		public T getSourceVertexID() {
			return f0;
		}

		/**
		 * Get the target vertex ID.
		 *
		 * @return target vertex ID
		 */
		public T getTargetVertexID() {
			return f1;
		}

		/**
		 * Get the path length.
		 *
		 * @return path length
		 */
		public IntValue getPathLength() {
			return f2;
		}

		/**
		 * Get the intermediate path vertices.
		 *
		 * @return intermediate path vertices
		 */
		public ValueArray<T> getIntermediatePathVertices() {
			return f3;
		}

		public String toVerboseString() {
			StringBuilder builder = new StringBuilder()
				.append(getSourceVertexID())
				.append("->");

			for (T vertex : getIntermediatePathVertices()) {
				builder
					.append(vertex)
					.append("->");
			}

			return builder
				.append(getTargetVertexID())
				.toString();
		}

		@Override
		public int hashCode() {
			return hasher.reset()
				.hash(f0.hashCode())
				.hash(f1.hashCode())
				.hash(f2.getValue())
				.hash(f3.hashCode())
				.hash();
		}
	}
}
