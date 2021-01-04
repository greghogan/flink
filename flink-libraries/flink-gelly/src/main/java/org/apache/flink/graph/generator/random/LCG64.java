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

package org.apache.flink.graph.generator.random;

import org.apache.flink.util.Preconditions;

import java.math.BigInteger;

/**
 *
 */
public class LCG64 {

	private long[] skipForward;

	private final long multiplier;

	private final long increment;

	private final long modulus;

	private long state;

	public static LCG64 jdkRandom() {
		return new LCG64(0x5DEECE66DL, 0xBL, (1L << 48) - 1);
	}

	public LCG64(long multiplier, long increment, long modulus) {
		Preconditions.checkArgument(modulus > 0, "LCG modulus must be greater than 0");

		this.multiplier = multiplier;
		this.increment = increment;
		this.modulus = modulus;
	}

	public LCG64 setState(long x) {
		this.state = x;

		return this;
	}

	public long next() {
		state = (multiplier * state + increment) % modulus;
		if (state < 0) {
			System.out.println("state = " + state + ", modulus = " + modulus);
			state += modulus;
		}

		return state;
	}

	public long skipForward(long count) {
		BigInteger a = BigInteger.valueOf(multiplier);
		BigInteger x = BigInteger.valueOf(state);
		BigInteger b = BigInteger.valueOf(increment);
		BigInteger m = BigInteger.valueOf(modulus);

		BigInteger a1 = a.subtract(BigInteger.ONE);
		BigInteger ma = a1.multiply(m);
		BigInteger n = BigInteger.valueOf(count);

		BigInteger y = a.modPow(n, ma).subtract(BigInteger.ONE).divide(a1).multiply(b);   // (a^n - 1) / (a - 1) * b, sort of
		BigInteger z = a.modPow(n, m).multiply(x);   // a^n * x, sort of

		state = y.add(z).mod(m).longValue();  // (y + z) mod m

		return state;
	}

	public long skipBack(long count) {
		return state;
	}

	private void buildTables() {
		int entries = 64 - Long.numberOfLeadingZeros(modulus);

		skipForward = new long[2 * entries];

		skipForward[0] = multiplier;
		skipForward[1] = increment;

		long lastMultiplier = multiplier;
		long lastIncrement = increment;

		BigInteger a = BigInteger.valueOf(multiplier);
		BigInteger b = BigInteger.valueOf(increment);
		BigInteger m = BigInteger.valueOf(modulus);
		BigInteger a1 = a.subtract(BigInteger.ONE);
		BigInteger ma = a1.multiply(m);

		for (int i = 1; i < entries; i++) {
			skipForward[2 * i] = (lastMultiplier * lastMultiplier) % modulus;
//			skipForward[2 * i + 1] = (lastIncrement * (lastMultiplier + 1)) % modulus;
//			skipForward[2 * i + 1] =

			lastMultiplier = skipForward[2 * i];
			lastIncrement = skipForward[2 * i + 1];
		}

//		BigInteger a1 = a.subtract(BigInteger.ONE);  // a - 1
//		BigInteger ma = a1.multiply(m);              // (a - 1) * m
//		BigInteger y = a.modPow(n, ma).subtract(BigInteger.ONE).divide(a1).multiply(b);  // (a^n - 1) / (a - 1) * b, sort of

	}
}
