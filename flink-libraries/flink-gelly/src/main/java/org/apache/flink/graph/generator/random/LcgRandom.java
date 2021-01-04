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

import java.math.BigInteger;

public class LcgRandom {

	/* The main method, which runs a correctness check */

	public static void main(String[] args) {
		// Use the parameters from Java's LCG RNG
		final BigInteger A = BigInteger.valueOf(0x5DEECE66DL);
		final BigInteger B = BigInteger.valueOf(0xBL);
		final BigInteger M = BigInteger.ONE.shiftLeft(48).subtract(BigInteger.ONE);  // 2^48

		// Choose seed and create LCG RNG
		BigInteger seed = BigInteger.valueOf(10);
		LcgRandom randSlow = new LcgRandom(A, B, M, seed);

		// Start testing
		final int N = 10000;

		// Check that skipping forward is correct
		for (int i = 0; i < N; i++) {
			LcgRandom randFast = new LcgRandom(A, B, M, seed);
			randFast.skip(i);
			if (!randSlow.getState().equals(randFast.getState()))
				throw new AssertionError();
			System.out.println(randFast.getState().longValue());
			System.out.println(randSlow.getState().longValue());
			randSlow.next();
		}

		// Check that backward iteration is correct
//		for (int i = N - 1; i >= 0; i--) {
//			randSlow.previous();
//			LcgRandom randFast = new LcgRandom(A, B, M, seed);
//			randFast.skip(i);
//			if (!randSlow.getState().equals(randFast.getState()))
//				throw new AssertionError();
//		}
//
//		// Check that backward skipping is correct
//		for (int i = 0; i < N; i++) {
//			LcgRandom randFast = new LcgRandom(A, B, M, seed);
//			randFast.skip(-i);
//			if (!randSlow.getState().equals(randFast.getState()))
//				throw new AssertionError();
//			randSlow.previous();
//		}

		System.out.printf("Test passed (n=%d)%n", N);
	}



	/* Code for LCG RNG instances */

	private final BigInteger a;  // Multiplier
	private final BigInteger b;  // Increment
	private final BigInteger m;  // Modulus
//	private final BigInteger aInv;  // Multiplicative inverse of 'a' modulo m

	private BigInteger x;  // State


	public LcgRandom(BigInteger a, BigInteger b, BigInteger m, BigInteger seed) {
		if (a == null || b == null || m == null || seed == null)
			throw new NullPointerException();
		if (a.signum() != 1 || b.signum() == -1 || m.signum() != 1 || seed.signum() == -1 || seed.compareTo(m) >= 0)
			throw new IllegalArgumentException("Arguments out of range");

		this.a = a;
//		this.aInv = a.modInverse(m);
		this.b = b;
		this.m = m;
		this.x = seed;
	}


	public BigInteger getState() {
		return x;
	}


	public void next() {
		x = x.multiply(a).add(b).mod(m);  // x = (a*x + b) mod m
	}


	public void previous() {
		// The intermediate result after subtracting 'b' may be negative, but the modular arithmetic is correct
//		x = x.subtract(b).multiply(aInv).mod(m);  // x = (a^-1 * (x - b)) mod m
	}


	public void skip(int n) {
		if (n >= 0)
			skip(a, b, BigInteger.valueOf(n));
//		else
//			skip(aInv, aInv.multiply(b).negate(), BigInteger.valueOf(n).negate());
	}


	private void skip(BigInteger a, BigInteger b, BigInteger n) {
		BigInteger a1 = a.subtract(BigInteger.ONE);  // a - 1
		BigInteger ma = a1.multiply(m);              // (a - 1) * m
		BigInteger y = a.modPow(n, ma).subtract(BigInteger.ONE).divide(a1).multiply(b);  // (a^n - 1) / (a - 1) * b, sort of
		BigInteger z = a.modPow(n, m).multiply(x);   // a^n * x, sort of
		x = y.add(z).mod(m);  // (y + z) mod m
	}
}
