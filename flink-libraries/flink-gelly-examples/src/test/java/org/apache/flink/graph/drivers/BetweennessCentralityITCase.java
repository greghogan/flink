/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.drivers;

import org.apache.flink.client.program.ProgramParametrizationException;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class BetweennessCentralityITCase
extends CopyableValueDriverBaseITCase {

	public BetweennessCentralityITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(int scale, String output, String... additionalParameters) {
		String[] parameters = new String[] {
			"--algorithm", "BetweennessCentrality",
			"--input", "RMatGraph", "--scale", Integer.toString(scale), "--type", idType, "--simplify", "directed",
			"--output", output};

		return ArrayUtils.addAll(parameters, additionalParameters);
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new BetweennessCentrality().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "BetweennessCentrality"},
			expected,
			ProgramParametrizationException.class);
	}

	@Test
	public void testPrintWithSmallRMatGraph() throws Exception {
		// computation is too large for collection mode
		Assume.assumeFalse(mode == TestExecutionMode.COLLECTION);

		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedCount(
			parameters(6, "print"),
			48);
	}
}
