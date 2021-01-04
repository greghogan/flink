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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.types.valuearray.ValueArray;
import org.apache.flink.graph.types.valuearray.ValueArrayFactory;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.text.WordUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
public class AdjacencyList<K extends Value, VV, EV extends Value>
extends DriverBase<K, VV, EV> {

	private DataSet<Tuple3<K, ValueArray<K>, ValueArray<EV>>> adjacencyList;

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String getShortDescription() {
		return "the adjacency list";
	}

	@Override
	public String getLongDescription() {
		return WordUtils.wrap("", 80);
	}

	@Override
	public DataSet plan(Graph<K, VV, EV> graph) throws Exception {
		return graph
			.getEdges()
			.groupBy(0)
			.reduceGroup(new GroupAdjacency<K, EV>())
				.name("Group adjacency");
	}

	@Override
	public void hash(String executionName) throws Exception {
		Checksum checksum = new ChecksumHashCode<Tuple3<K, ValueArray<K>, ValueArray<EV>>>()
			.run(adjacencyList)
			.execute(executionName);

		System.out.println(checksum);
	}

	@Override
	public void print(String executionName) throws Exception {
		Collect<Tuple3<K, ValueArray<K>, ValueArray<EV>>> collector = new Collect<>();

		for (Tuple3<K, ValueArray<K>, ValueArray<EV>> result : collector.run(adjacencyList).execute(executionName)) {
			System.out.println(result);
		}

	}

	@Override
	public void writeCSV(String filename, String lineDelimiter, String fieldDelimiter) {
		adjacencyList
			.writeAsFormattedText(filename, new AdjacencyFormatter<K, EV>(fieldDelimiter));
	}

	/**
	 *
	 * @param <T>
	 * @param <ET>
	 */
	private static class GroupAdjacency<T extends Value, ET extends Value>
	implements GroupReduceFunction<Edge<T, ET>, Tuple3<T, ValueArray<T>, ValueArray<ET>>> {
		private Tuple3<T, ValueArray<T>, ValueArray<ET>> output = new Tuple3<>();

		@Override
		public void reduce(Iterable<Edge<T, ET>> edges, Collector<Tuple3<T, ValueArray<T>, ValueArray<ET>>> out)
				throws Exception {
			for (Edge<T, ET> edge : edges) {
				if (output.f1 == null) {
					output.f1 = ValueArrayFactory.createValueArray(edge.f1.getClass());
					output.f2 = ValueArrayFactory.createValueArray(edge.f2.getClass());
				}

				output.f0 = edge.f0;
				output.f1.add(edge.f1);
				output.f2.add(edge.f2);
			}

			out.collect(output);
			output.f1.clear();
			output.f2.clear();
		}
	}

	/**
	 *
	 * @param <T>
	 * @param <ET>
	 */
	private static class AdjacencyFormatter<T extends Value, ET extends Value>
	implements TextFormatter<Tuple3<T, ValueArray<T>, ValueArray<ET>>> {
		private final String fieldDelimiter;

		public AdjacencyFormatter(String fieldDelimiter) {
			this.fieldDelimiter = fieldDelimiter;
		}

		@Override
		public String format(Tuple3<T, ValueArray<T>, ValueArray<ET>> value) {
			StringBuilder sb = new StringBuilder()
				.append(value.f0)
				.append(fieldDelimiter)
				.append("[");

			Iterator<T> iter0 = value.f1.iterator();
			Iterator<ET> iter1 = value.f2.iterator();

			String listDelimiter = "";

			while (iter0.hasNext() && iter1.hasNext()) {
				sb
					.append(listDelimiter)
					.append("(")
					.append(iter0.next())
					.append(fieldDelimiter)
					.append(iter1.next())
					.append(")");

				listDelimiter = fieldDelimiter;
			}

			if (iter0.hasNext() || iter1.hasNext()) {
				throw new NoSuchElementException("Value arrays for adjacency list should be the same length.");
			}

			return sb
				.append("]")
				.toString();
		}
	}
}
