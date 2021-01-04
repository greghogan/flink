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

package org.apache.flink.graph.library.clustering.undirected;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.undirected.EdgeDegreePair;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
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
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class RectangleListing<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Tuple4<K, K, K, K>> {

	// Optional configuration
	private OptionalBoolean sortRectangleVertices = new OptionalBoolean(false, false);

	private int littleParallelism = PARALLELISM_DEFAULT;

	/**
	 * Normalize the rectangle listing such that for each result (K0, K1, K2, K3)
	 * the vertex IDs are sorted K0 < K1 < K2 < K3.
	 *
	 * @param sortRectangleVertices whether to output each rectangle's vertices in sorted order
	 * @return this
	 */
	public RectangleListing<K, VV, EV> setSortRectangleVertices(boolean sortRectangleVertices) {
		this.sortRectangleVertices.set(sortRectangleVertices);

		return this;
	}

	/**
	 * Override the parallelism of operators processing small amounts of data.
	 *
	 * @param littleParallelism operator parallelism
	 * @return this
	 */
	public RectangleListing<K, VV, EV> setLittleParallelism(int littleParallelism) {
		Preconditions.checkArgument(littleParallelism > 0 || littleParallelism == PARALLELISM_DEFAULT,
			"The parallelism must be greater than zero.");

		this.littleParallelism = littleParallelism;

		return this;
	}

	@Override
	protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
		super.mergeConfiguration(other);

		RectangleListing rhs = (RectangleListing) other;

		sortRectangleVertices.mergeWith(rhs.sortRectangleVertices);
	}

	/*
	 * Implementation notes:
	 *
	 * The requirement that "K extends CopyableValue<K>" can be removed when
	 *   Flink has a self-join and GenerateTriplets is implemented as such.
	 */

	@Override
	public DataSet<Tuple4<K, K, K, K>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// u, v, (edge value, deg(u), deg(v))
		DataSet<Edge<K, Tuple3<EV, LongValue, LongValue>>> pairDegree = input
			.run(new EdgeDegreePair<K, VV, EV>()
				.setParallelism(littleParallelism));

		// u, v, deg(u) < deg(v) or (deg(u) == deg(v) and u < v)
		DataSet<Tuple3<K, K, BooleanValue>> markedByDegree = pairDegree
			.map(new MarkByDegree<K, EV>())
				.setParallelism(littleParallelism)
				.name("Mark by degree");

		// u, v, w where (u, v) and (u, w) are edges in graph, v < w
		DataSet<Tuple3<K, K, K>> triplets = markedByDegree
			.groupBy(0)
			.sortGroup(2, Order.DESCENDING)
			.reduceGroup(new GenerateTriplets<K>())
				.setParallelism(littleParallelism)
				.name("Generate triplets");

		// u, v, w, x where (u, v), (v, w), (w, x), and (x, u) are edges in graph
		DataSet<Tuple4<K, K, K, K>> rectangles = triplets
			.groupBy(1, 2)
			.reduceGroup(new ListRectangles<K>())
				.setParallelism(littleParallelism)
				.name("Rectangle listing");

//		if (sortTriangleVertices.get()) {
//			triangles = triangles
//				.map(new SortTriangleVertices<K>())
//					.name("Sort triangle vertices");
//		}

		return rectangles;
	}

	/**
	 *
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0; 1")
	private static final class MarkByDegree<T extends Comparable<T>, ET>
	implements MapFunction<Edge<T, Tuple3<ET, LongValue, LongValue>>, Tuple3<T, T, BooleanValue>> {
		private Tuple3<T, T, BooleanValue> edge = new Tuple3<>();

		@Override
		public Tuple3<T, T, BooleanValue> map(Edge<T, Tuple3<ET, LongValue, LongValue>> value)
				throws Exception {
			edge.f0 = value.f0;
			edge.f1 = value.f1;

			Tuple3<ET, LongValue, LongValue> degrees = value.f2;
			long sourceDegree = degrees.f1.getValue();
			long targetDegree = degrees.f2.getValue();

			if (sourceDegree < targetDegree ||
					(sourceDegree == targetDegree && value.f0.compareTo(value.f1) < 0)) {
				edge.f2 = BooleanValue.TRUE;
			} else {
				edge.f2 = BooleanValue.FALSE;
			}

			return edge;
		}
	}

	/**
	 * @param <T> ID type
	 */
	@ForwardedFields("0")
	private static final class GenerateTriplets<T extends Comparable<T> & CopyableValue<T>>
	implements GroupReduceFunction<Tuple3<T, T, BooleanValue>, Tuple3<T, T, T>> {
		private Tuple3<T, T, T> output = new Tuple3<>();

		private List<T> visited = new ArrayList<>();

		@Override
		public void reduce(Iterable<Tuple3<T, T, BooleanValue>> values, Collector<Tuple3<T, T, T>> out)
				throws Exception {
			int visitedCount = 0;

			Iterator<Tuple3<T, T, BooleanValue>> iter = values.iterator();

			while (true) {
				Tuple3<T, T, BooleanValue> edge = iter.next();

				output.f0 = edge.f0;

				for (int i = 0; i < visitedCount; i++) {
					T prior = visited.get(i);

					if (prior.compareTo(edge.f1) < 0) {
						output.f1 = prior;
						output.f2 = edge.f1;
					} else {
						output.f1 = edge.f1;
						output.f2 = prior;
					}

					out.collect(output);
				}

				if (! iter.hasNext()) {
					break;
				}

				if (edge.f2.getValue()) {
					if (visitedCount == visited.size()) {
						visited.add(edge.f1.copy());
					} else {
						edge.f1.copyTo(visited.get(visitedCount));
					}

					visitedCount += 1;
				}
			}
		}
	}

	/**
	 *
	 *
	 * @param <T> ID type
     */
	private static class ListRectangles<T extends CopyableValue<T>>
	implements GroupReduceFunction<Tuple3<T, T, T>, Tuple4<T, T, T, T>> {
		private List<Tuple4<T, T, T, T>> visited = new ArrayList<>();

		@Override
		public void reduce(Iterable<Tuple3<T, T, T>> values, Collector<Tuple4<T, T, T, T>> out)
				throws Exception {
			int visitedCount = 0;

			Iterator<Tuple3<T, T, T>> iter = values.iterator();

			while (true) {
				Tuple3<T, T, T> triplet = iter.next();

				for (int i = 0; i < visitedCount; i++) {
					Tuple4<T, T, T, T> prior = visited.get(i);

					prior.f3 = triplet.f0;
					out.collect(prior);
				}

				if (! iter.hasNext()) {
					break;
				}

				if (visitedCount == visited.size()) {
					visited.add(new Tuple4<T, T, T, T>(triplet.f0.copy(), triplet.f1.copy(), triplet.f2.copy(), null));
				} else {
					Tuple4<T, T, T, T> prior = visited.get(visitedCount);
					triplet.f0.copyTo(prior.f0);
					triplet.f1.copyTo(prior.f1);
					triplet.f2.copyTo(prior.f2);
				}

				visitedCount += 1;
			}
		}
	}

	/**
	 * Reorders the vertices of each emitted rectangle with path (K0, K1, K2, K3)
	 * into sorted order (K0, K1, K3, K2) such that K0 < K1 < K2.
	 *
	 * @param <T> ID type
	 */
//	private static final class SortTriangleVertices<T extends Comparable<T>>
//	implements MapFunction<Tuple3<T, T, T>, Tuple3<T, T, T>> {
//		@Override
//		public Tuple3<T, T, T> map(Tuple3<T, T, T> value)
//				throws Exception {
//			// by the triangle listing algorithm we know f1 < f2
//			if (value.f0.compareTo(value.f1) > 0) {
//				T temp_val = value.f0;
//				value.f0 = value.f1;
//
//				if (temp_val.compareTo(value.f2) <= 0) {
//					value.f1 = temp_val;
//				} else {
//					value.f1 = value.f2;
//					value.f2 = temp_val;
//				}
//			}
//
//			return value;
//		}
//	}
}
