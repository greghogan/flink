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

import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ValueArrayTest {

	LongValueArray lva;
	IntValueArray iva;

	@Before
	public void setup() {
		lva = new LongValueArray(4096);
		LongValue lv = new LongValue();

		for (int i = 0 ; i < 23 ; i++) {
			lv.setValue(i);
			lva.add(lv);
		}

		iva = new IntValueArray(4096);
		IntValue iv = new IntValue();

		for (int i = 0 ; i < 23 ; i++) {
			iv.setValue(i);
			iva.add(iv);
		}
	}

	@Test
	public void testCopy() {
		CopyableValue<?>[] value_types = new CopyableValue[] { lva, iva };

		for (CopyableValue<?> type : value_types) {
			assertEquals(type, type.copy());
		}
	}

	@Test
	public void testCopyTo() {
		LongValueArray lva_to = new LongValueArray();

		lva.copyTo(lva_to);
		assertEquals(lva, lva_to);

		IntValueArray iva_to = new IntValueArray();

		iva.copyTo(iva_to);
		assertEquals(iva, iva_to);
	}
}
