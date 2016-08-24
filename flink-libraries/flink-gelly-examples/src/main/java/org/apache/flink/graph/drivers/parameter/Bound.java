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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.drivers.parameter.Bound.LIMIT;

/**
 *
 */
public class Bound
implements Parameter<LIMIT> {

	public enum LIMIT {
		//
		NONE,

		//
		MINIMUM,

		//
		MAXIMUM
	}

	private LIMIT value;

	/**
	 * Add this parameter to the list of parameters stored by owner.
	 *
	 * @param owner the {@link Parameterized} using this {@link Parameter}
	 */
	public Bound(ParameterizedBase owner) {
		owner.addParameter(this);
	}

	@Override
	public String getUsage() {
		return "[--bound <minimum | maximum>]";
	}

	@Override
	public void configure(ParameterTool parameterTool) {
		String bound = parameterTool.get("bound");

		if (bound == null) {
			value = LIMIT.NONE;
		} else {
			switch (bound.toLowerCase()) {
				case "minimum":
					value = LIMIT.MINIMUM;
					break;
				case "maximum":
					value = LIMIT.MAXIMUM;
					break;
				default:
					throw new ProgramParametrizationException(
						"Expected 'minimum' or 'maximum' bound but received '" + bound + "'");
			}
		}
	}

	@Override
	public LIMIT getValue() {
		return value;
	}
}
