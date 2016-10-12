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

package org.apache.flink.runtime.operators.sort;

public class Index {

	private int index;

	private int pageNumber;

	private int pageOffset;

	private final int recordSize;

	private final int lastOffset;

	public Index(int index, int recordSize, int recordsPerSegment) {
		this.index = index;
		this.pageNumber = index / recordsPerSegment;
		this.pageOffset = index - pageNumber * recordsPerSegment;

		this.recordSize = recordSize;
		this.lastOffset = (recordSize - 1) + recordsPerSegment;
	}

	public Index(Index copy) {
		this.index = copy.index;
		this.pageNumber = copy.pageNumber;
		this.pageOffset = copy.pageOffset;

		this.recordSize = copy.recordSize;
		this.lastOffset = copy.lastOffset;
	}

	public int getIndex() {
		return index;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public int getPageOffset() {
		return pageOffset;
	}

	public int getRecordSize() {
		return recordSize;
	}

	public int getLastOffset() {
		return lastOffset;
	}

	public Index increment() {
		Index copy = new Index(this);
		copy.incrementAndGet();
		return copy;
	}

	public int incrementAndGet() {
		pageOffset += recordSize;
		if (pageOffset > lastOffset) {
			pageNumber++;
			pageOffset = 0;
		}

		return ++index;
	}

	public int getAndIncrement() {
		pageOffset += recordSize;
		if (pageOffset > lastOffset) {
			pageNumber++;
			pageOffset = 0;
		}

		int oldValue = index++;
		return oldValue;
	}

	public Index decrement() {
		Index copy = new Index(this);
		copy.decrementAndGet();
		return copy;
	}

	public int decrementAndGet() {
		pageOffset -= recordSize;
		if (pageOffset < 0) {
			pageNumber--;
			pageOffset = lastOffset;
		}

		return --index;
	}

	public int getAndDecrement() {
		pageOffset -= recordSize;
		if (pageOffset < 0) {
			pageNumber--;
			pageOffset = lastOffset;
		}

		int oldValue = index--;
		return oldValue;
	}

	@Override
	public String toString() {
		return "index=" + index + ", pageNumber=" + pageNumber + ", pageOffset=" + pageOffset
			+ ", recordSize=" + recordSize + ", lastOffset=" + lastOffset;
	}

	public static void main(String[] args) {
		Index i = new Index(0, 16, 1024);
		System.out.println(i);
		System.out.println(i.incrementAndGet());
		System.out.println(i.getAndIncrement());
	}
}
