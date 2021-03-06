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

import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class AllPairsDistanceITCase
extends CopyableValueDriverBaseITCase {

	public AllPairsDistanceITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(int scale, String output, String... additionalParameters) {
		String[] parameters = new String[] {
			"--algorithm", "AllPairsDistance",
			"--input", "RMatGraph", "--scale", Integer.toString(scale), "--type", idType, "--simplify", "directed",
			"--output", output};

		return ArrayUtils.addAll(parameters, additionalParameters);
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new AllPairsDistance().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "AllPairsDistance"},
			expected,
			ProgramParametrizationException.class);
	}

	@Test
	public void testHashWithSmallRMatGraph() throws Exception {
		expectedChecksum(parameters(7, "hash"), 11779, 0x000017159c104711L);
	}

	@Test
	public void testPrintWithSmallRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(parameters(7, "print"), new ChecksumHashCode.Checksum(11779, 0x0000170c57e0ff3eL));
	}
}
