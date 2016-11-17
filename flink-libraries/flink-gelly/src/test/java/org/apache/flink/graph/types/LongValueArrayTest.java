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

import org.apache.flink.types.LongValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class LongValueArrayTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testBoundedArray() {
		int count = 1024;

		ValueArray<LongValue> va = new LongValueArray(count * LongValueArray.ELEMENT_LENGTH_IN_BYTES);

		// fill the array
		for (int i = 0 ; i < count ; i++) {
			assertFalse(va.isFull());
			va.add(new LongValue(i));
		}

		// array is now full
		assertTrue(va.isFull());

		// verify the array values
		int idx = 0;
		for (LongValue lv : va) {
			assertEquals(idx++, lv.getValue());
		}

		// add element past end of array
		exception.expect(RuntimeException.class);
		va.add(new LongValue(count));
	}

	@Test
	public void testUnboundedArray() {
		int count = 4096;

		ValueArray<LongValue> va = new LongValueArray();

		// add several elements
		for (int i = 0 ; i < count ; i++) {
			assertFalse(va.isFull());
			va.add(new LongValue(i));
		}

		// array never fills
		assertFalse(va.isFull());

		// verify the array values
		int idx = 0;
		for (LongValue lv : va) {
			assertEquals(idx++, lv.getValue());
		}
	}
}
