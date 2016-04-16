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

package org.apache.flink.graph.asm.degree.annotate;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;

public class DegreeAnnotationFunctions {

	// --------------------------------------------------------------------------------------------
	//  Vertex functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Emits the source vertex label along with an initial count.
	 *
	 * @param <K> label type
	 * @param <EV> edge value type
	 */
	@ForwardedFields("0")
	public static class MapEdgeToSourceLabel<K, EV>
	implements MapFunction<Edge<K,EV>, Vertex<K,LongValue>> {
		private Vertex<K,LongValue> output = new Vertex<>(null, new LongValue(1));

		@Override
		public Vertex<K,LongValue> map(Edge<K,EV> value) throws Exception {
			output.f0 = value.f0;
			return output;
		}
	}

	/**
	 * Emits the target vertex label along with an initial count.
	 *
	 * @param <K> label type
	 * @param <EV> edge value type
	 */
	@ForwardedFields("1->0")
	public static class MapEdgeToTargetLabel<K, EV>
	implements MapFunction<Edge<K,EV>, Vertex<K,LongValue>> {
		private Vertex<K,LongValue> output = new Vertex<>(null, new LongValue(1));

		@Override
		public Vertex<K,LongValue> map(Edge<K,EV> value) throws Exception {
			output.f0 = value.f1;
			return output;
		}
	}

	/**
	 * Combines the vertex degree count.
	 *
	 * @param <K> label type
	 */
	@ForwardedFields("0")
	public static class DegreeCount<K>
	implements ReduceFunction<Vertex<K,LongValue>> {
		@Override
		public Vertex<K,LongValue> reduce(Vertex<K,LongValue> left, Vertex<K,LongValue> right)
				throws Exception {
			LongValue count = left.f1;
			count.setValue(count.getValue() + right.f1.getValue());
			return left;
		}
	}

	/**
	 * Performs a left outer join to apply a zero count for vertices with
	 * out- or in-degree of zero.
	 *
	 * @param <K> label type
	 * @param <VV> vertex value type
	 */
	@ForwardedFieldsFirst("0")
	@ForwardedFieldsSecond("0")
	public static final class JoinVertexWithVertexDegree<K,VV>
	implements JoinFunction<Vertex<K,VV>, Vertex<K,LongValue>, Vertex<K,LongValue>> {
		private LongValue zero = new LongValue(0);

		private Vertex<K,LongValue> output = new Vertex<>();

		@Override
		public Vertex<K,LongValue> join(Vertex<K,VV> vertex, Vertex<K,LongValue> vertexDegree)
				throws Exception {
			output.f0 = vertex.f0;
			output.f1 = (vertexDegree == null) ? zero : vertexDegree.f1;

			return output;
		}
	}

	/**
	 * Performs a full outer join composing vertex out- and in-degree and
	 * applying a zero count for vertices having an out- or in-degree of zero.
	 *
	 * @param <K> label type
	 */
	@ForwardedFieldsFirst("0")
	@ForwardedFieldsSecond("0")
	public static final class JoinVertexDegreeWithVertexDegree<K>
	implements JoinFunction<Vertex<K,LongValue>, Vertex<K,LongValue>, Vertex<K,Tuple2<LongValue,LongValue>>> {
		private LongValue zero = new LongValue(0);

		private Tuple2<LongValue,LongValue> degrees = new Tuple2<>();

		private Vertex<K,Tuple2<LongValue,LongValue>> output = new Vertex<>(null, degrees);

		@Override
		public Vertex<K, Tuple2<LongValue,LongValue>> join(Vertex<K,LongValue> left, Vertex<K,LongValue> right)
				throws Exception {
			if (left == null) {
				output.f0 = right.f0;
				degrees.f0 = zero;
				degrees.f1 = right.f1;
			} else {
				output.f0 = left.f0;
				degrees.f0 = left.f1;
				degrees.f1 = (right == null) ? zero : right.f1;
			}

			return output;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Edge functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Assigns the vertex degree to this edge value.
	 *
	 * @param <K> label type
	 * @param <EV> edge value type
	 */
	@ForwardedFieldsFirst("0; 1")
	@ForwardedFieldsSecond("0; 1->2")
	public static final class JoinEdgeWithVertexDegree<K, EV>
	implements JoinFunction<Edge<K,EV>, Vertex<K,LongValue>, Edge<K,LongValue>> {
		private Edge<K, LongValue> output = new Edge<>();

		@Override
		public Edge<K,LongValue> join(Edge<K,EV> edge, Vertex<K,LongValue> vertex) throws Exception {
			output.f0 = edge.f0;
			output.f1 = edge.f1;
			output.f2 = vertex.f1;

			return output;
		}
	}

	/**
	 * Composes the vertex degree with this edge value.
	 *
	 * @param <K> label type
	 */
	@ForwardedFieldsFirst("0; 1; 2->2.1")
	@ForwardedFieldsSecond("0; 1->2.0")
	public static final class JoinEdgeDegreeWithVertexDegree<K>
	implements JoinFunction<Edge<K,LongValue>, Vertex<K,LongValue>, Edge<K,Tuple2<LongValue,LongValue>>> {
		private Tuple2<LongValue,LongValue> degrees = new Tuple2<>();

		private Edge<K,Tuple2<LongValue,LongValue>> output = new Edge<>(null, null, degrees);

		@Override
		public Edge<K, Tuple2<LongValue,LongValue>> join(Edge<K,LongValue> edge, Vertex<K,LongValue> vertex)
				throws Exception {
			output.f0 = edge.f0;
			output.f1 = edge.f1;
			degrees.f0 = edge.f2;
			degrees.f1 = vertex.f1;

			return output;
		}
	}
}
