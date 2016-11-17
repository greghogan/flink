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

package org.apache.flink.graph.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Wrapper for an array of the primitive type {@code int}.
 */
@PublicEvolving
public class IntValueArray
implements ValueArray<IntValue> {

	public static final int ELEMENT_LENGTH_IN_BYTES = 4;

	private static final int INITIAL_CAPACITY_IN_BYTES = 4096;

	private boolean isBounded;

	private int[] data;

	private int position;

	private int mark;

	/**
	 * Initializes the array with an unlimited capacity.
	 */
	public IntValueArray() {
		isBounded = false;
		initialize(INITIAL_CAPACITY_IN_BYTES);
	}

	/**
	 * Initializes the array of the provided number of bytes.
	 *
	 * @param capacity Initial capacity of the encapsulated array in bytes.
	 */
	public IntValueArray(int capacity) {
		isBounded = true;
		initialize(capacity);
	}

	private void initialize(int capacity) {
		int size = capacity / ELEMENT_LENGTH_IN_BYTES;

		Preconditions.checkArgument(size > 0, "Requested array with zero capacity");

		data = new int[size];
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof IntValueArray) {
			IntValueArray other = (IntValueArray) obj;

			if (other.position != position) {
				return false;
			}

			for (int i = 0 ; i < position ; i++) {
				if (data[i] != other.data[i]) {
					return false;
				}
			}

			return true;
		}

		return false;
	}

	@Override
	public int hashCode() {
		int hash = 43 * position;

		for (int i = 0 ; i < position ; i++) {
			hash = 43 * hash + data[i];
		}

		return hash;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		for (int idx = 0 ; idx < this.position ; idx++) {
			sb.append(data[idx]);
			if (idx < position - 1) {
				sb.append(",");
			}
		}
		sb.append("]");

		return sb.toString();
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void setValue(ValueArray<IntValue> value) {
		value.copyTo(this);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int getBinaryLength() {
		return 4 + ELEMENT_LENGTH_IN_BYTES * position;
	}

	@Override
	public void copyTo(ValueArray<IntValue> target) {
		IntValueArray other = (IntValueArray) target;

		other.position = position;

		other.ensureCapacity(position);
		System.arraycopy(data, 0, other.data, 0, position);
	}

	@Override
	public ValueArray<IntValue> copy() {
		ValueArray<IntValue> copy = new IntValueArray();

		this.copyTo(copy);

		return copy;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target)
			throws IOException {
		copyInternal(source, target);
	}

	public static void copyInternal(DataInputView source, DataOutputView target)
			throws IOException {
		int count = source.readInt();
		target.writeInt(count);

		int bytes = ELEMENT_LENGTH_IN_BYTES * count;
		target.write(source, bytes);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void write(DataOutputView out)
			throws IOException {
		out.writeInt(position);

		for (int i = 0 ; i < position ; i++) {
			out.writeInt(data[i]);
		}
	}

	@Override
	public void read(DataInputView in)
			throws IOException {
		position = in.readInt();

		ensureCapacity(position);

		for (int i = 0 ; i < position ; i++) {
			data[i] = in.readInt();
		}
	}

	// --------------------------------------------------------------------------------------------

	private final ReadIterator iterator = new ReadIterator();

	@Override
	public Iterator<IntValue> iterator() {
		iterator.reset();
		return iterator;
	}

	private class ReadIterator
	implements Iterator<IntValue> {
		private IntValue value = new IntValue();

		private int pos;

		@Override
		public boolean hasNext() {
			return pos < position;
		}

		@Override
		public IntValue next() {
			value.setValue(data[pos++]);
			return value;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove");
		}

		public void reset() {
			pos = 0;
		}
	}

	@Override
	public void clear() {
		position = 0;
	}

	@Override
	public int size() { return position; }

	@Override
	public boolean isFull() {
		if (isBounded) {
			return position == data.length;
		} else {
			return position == Integer.MAX_VALUE;
		}
	}

	@Override
	public boolean add(IntValue value) {
		if (position == data.length) {
			if (isBounded) {
				throw new RuntimeException("Bounded array capacity exceeded");
			} else {
				ensureCapacity(position + 1);
			}
		}

		data[position++] = value.getValue();

		return true;
	}

	@Override
	public boolean addAll(ValueArray<IntValue> valueArray) {
		for (IntValue value : valueArray) {
			add(value);
		}

		return true;
	}

	// --------------------------------------------------------------------------------------------

	private void ensureCapacity(int minCapacity) {
		long currentCapacity = data.length;

		if (minCapacity < currentCapacity) {
			return;
		}

		long expandedCapacity = Math.min(Integer.MAX_VALUE, currentCapacity + (currentCapacity >> 1));
		int newCapacity = Math.max(minCapacity, (int)expandedCapacity);
		data = Arrays.copyOf(data, newCapacity);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void mark() {
		mark = position;
	}

	@Override
	public void reset() {
		position = mark;
	}
}
