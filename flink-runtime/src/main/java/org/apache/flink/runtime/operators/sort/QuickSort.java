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

public final class QuickSort implements IndexedSorter {

	private static final IndexedSorter alt = new HeapSort();

	public QuickSort() {
	}

	private static void fix(IndexedSortable s, Index p, Index r) {
		if (s.compare(p, r) > 0) {
			s.swap(p, r);
		}
	}

	/**
	 * Deepest recursion before giving up and doing a heapsort.
	 * Returns 2 * ceil(log(n)).
	 */
	protected static int getMaxDepth(int x) {
		if (x <= 0) {
			throw new IllegalArgumentException("Undefined for " + x);
		}
		return (32 - Integer.numberOfLeadingZeros(x - 1)) << 2;
	}

	/**
	 * Sort the given range of items using quick sort. {@inheritDoc} If the recursion depth falls below
	 * {@link #getMaxDepth},
	 * then switch to {@link HeapSort}.
	 */
	@Override
	public void sort(final IndexedSortable s, Index p, Index r) {
		sortInternal(s, p, r, getMaxDepth(r.getIndex() - p.getIndex()));
	}

	@Override
	public void sort(IndexedSortable s) {
		Index p = new Index(0, s.getRecordSize(), s.getRecordsPerSegment());
		Index r = new Index(s.size(), s.getRecordSize(), s.getRecordsPerSegment());
		sort(s, p, r);
	}

	private static void sortInternal(final IndexedSortable s, Index p, Index r, int depth) {
		while (true) {
			if (r.getIndex() - p.getIndex() < 13) {
				for (Index i = new Index(p); i.getIndex() < r.getIndex(); i.incrementAndGet()) {
					Index j = new Index(i);
					Index j_minus = j.decrement();

					while (j.getIndex() > p.getIndex() && s.compare(j_minus, j) > 0) {
						s.swap(j, j_minus);
						j.decrementAndGet();
						j_minus.decrementAndGet();
					}
				}
				return;
			}
			if (--depth < 0) {
				// give up
				alt.sort(s, p, r);
				return;
			}

			// select, move pivot into first position
			Index r_minus = r.decrement();
			Index middle = new Index((p.getIndex() + r.getIndex()) >>> 1, p.getRecordSize(), p.getLastOffset());
			fix(s, middle, p);
			fix(s, middle, r_minus);
			fix(s, p, r_minus);

			// Divide
			Index i = p;
			Index j = r;
			Index ll = p;
			Index rr = r;
			int cr;
			while (true) {
				while (i.incrementAndGet() < j.getIndex()) {
					if ((cr = s.compare(i, p)) > 0) {
						break;
					}
					if (0 == cr && ll.incrementAndGet() != i.getIndex()) {
						s.swap(ll, i);
					}
				}
				while (j.decrementAndGet() > i.getIndex()) {
					if ((cr = s.compare(p, j)) > 0) {
						break;
					}
					if (0 == cr && rr.decrementAndGet() != j.getIndex()) {
						s.swap(rr, j);
					}
				}
				if (i.getIndex() < j.getIndex()) {
					s.swap(i, j);
				} else {
					break;
				}
			}
			j = i;
			// swap pivot- and all eq values- into position
			while (ll.getIndex() >= p.getIndex()) {
				i.decrementAndGet();
				s.swap(ll, i);
				ll.decrementAndGet();
			}
			while (rr.getIndex() < r.getIndex()) {
				s.swap(rr, j);
				rr.incrementAndGet();
				j.incrementAndGet();
			}

			// Conquer
			// Recurse on smaller interval first to keep stack shallow
			assert i.getIndex() != j.getIndex();
			if (i.getIndex() - p.getIndex() < r.getIndex() - j.getIndex()) {
				sortInternal(s, p, i, depth);
				p = j;
			} else {
				sortInternal(s, j, r, depth);
				r = i;
			}
		}
	}

}
