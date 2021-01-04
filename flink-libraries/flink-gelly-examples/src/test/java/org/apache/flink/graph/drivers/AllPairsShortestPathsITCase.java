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
public class AllPairsShortestPathsITCase
extends CopyableValueDriverBaseITCase {

	public AllPairsShortestPathsITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(int scale, String output, String... additionalParameters) {
		String[] parameters = new String[] {
			"--algorithm", "AllPairsShortestPaths",
			"--input", "RMatGraph", "--scale", Integer.toString(scale), "--type", idType, "--simplify", "directed",
				"--edge_factor", "4",
			"--output", output};

		return ArrayUtils.addAll(parameters, additionalParameters);
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new AllPairsShortestPaths().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "AllPairsShortestPaths"},
			expected,
			ProgramParametrizationException.class);
	}

	@Test
	public void testHashWithSmallRMatGraph() throws Exception {
		expectedChecksum(parameters(6, "hash"), 2990, 0x000005d0684226faL);
	}

	@Test
	public void testPrintWithSmallRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(parameters(6, "print"), new ChecksumHashCode.Checksum(2990, 0x000005ad15a39d30L));
	}
}
