/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LexicographicalComparatorJUnitTest extends ComparisonTestCase {
  private LexicographicalComparator lc;

  public void testMixedNumeric() throws Exception {
    compare(lc, (byte) 1,  (short) 2,    Comparison.LT);
    compare(lc, (byte) 1,  (int)   2,    Comparison.LT);
    compare(lc, (byte) 1,  (long)  2,    Comparison.LT);
    compare(lc, (byte) 1,          2.1f, Comparison.LT);
    compare(lc, (byte) 1,          2.1d, Comparison.LT);
    
    compare(lc, (short) 1, (byte) 2,    Comparison.LT);
    compare(lc, (short) 1, (int)  2,    Comparison.LT);
    compare(lc, (short) 1, (long) 2,    Comparison.LT);
    compare(lc, (short) 1,        2.1f, Comparison.LT);
    compare(lc, (short) 1,        2.1d, Comparison.LT);
    
    compare(lc, (int) 1,  (byte)  2,    Comparison.LT);
    compare(lc, (int) 1,  (short) 2,    Comparison.LT);
    compare(lc, (int) 1,  (long)  2,    Comparison.LT);
    compare(lc, (int) 1,          2.1f, Comparison.LT);
    compare(lc, (int) 1,          2.1d, Comparison.LT);

    compare(lc, (long) 1,  (byte)  2,    Comparison.LT);
    compare(lc, (long) 1,  (short) 2,    Comparison.LT);
    compare(lc, (long) 1,  (int)   2,    Comparison.LT);
    compare(lc, (long) 1,          2.1f, Comparison.LT);
    compare(lc, (long) 1,          2.1d, Comparison.LT);

    compare(lc, 1.1f,  (byte)  2,    Comparison.LT);
    compare(lc, 1.1f,  (short) 2,    Comparison.LT);
    compare(lc, 1.1f,  (int)   2,    Comparison.LT);
    compare(lc, 1.1f,  (long)  2,    Comparison.LT);
    compare(lc, 1.1f,          2.1d, Comparison.LT);

    compare(lc, 1.1d,  (byte)  2,    Comparison.LT);
    compare(lc, 1.1d,  (short) 2,    Comparison.LT);
    compare(lc, 1.1d,  (int)   2,    Comparison.LT);
    compare(lc, 1.1d,  (long)  2,    Comparison.LT);
    compare(lc, 1.1d,          2.1f, Comparison.LT);
  }
  
  public void testBoolean() throws Exception {
    compare(lc, Boolean.TRUE,  Boolean.TRUE,  Comparison.EQ);
    compare(lc, Boolean.FALSE, Boolean.FALSE, Comparison.EQ);
    compare(lc, Boolean.TRUE,  Boolean.FALSE, Comparison.GT);
    compare(lc, Boolean.FALSE, Boolean.TRUE,  Comparison.LT);
  }
  
  public void testByte() throws Exception {
    compare(lc, (byte)  0, (byte)  0, Comparison.EQ);
    compare(lc, (byte)  0, (byte)  1, Comparison.LT);
    compare(lc, (byte) -1, (byte)  1, Comparison.LT);
    compare(lc, (byte)  1, (byte) -1, Comparison.GT);
    compare(lc, (byte) -2, (byte) -1, Comparison.LT);
    compare(lc, (byte)  1, (byte)  2, Comparison.LT);
    compare(lc, (byte)  2, (byte)  1, Comparison.GT);
  }
  
  public void testShort() throws Exception {
    compare(lc, (short)  0, (short)  0, Comparison.EQ);
    compare(lc, (short)  0, (short)  1, Comparison.LT);
    compare(lc, (short) -1, (short)  1, Comparison.LT);
    compare(lc, (short)  1, (short) -1, Comparison.GT);
    compare(lc, (short) -2, (short) -1, Comparison.LT);
    compare(lc, (short)  1, (short)  2, Comparison.LT);
    compare(lc, (short)  2, (short)  1, Comparison.GT);
  }

  public void testInt() throws Exception {
    compare(lc, (int)  0, (int)  0, Comparison.EQ);
    compare(lc, (int)  0, (int)  1, Comparison.LT);
    compare(lc, (int) -1, (int)  1, Comparison.LT);
    compare(lc, (int)  1, (int) -1, Comparison.GT);
    compare(lc, (int) -2, (int) -1, Comparison.LT);
    compare(lc, (int)  1, (int)  2, Comparison.LT);
    compare(lc, (int)  2, (int)  1, Comparison.GT);
  }

  public void testLong() throws Exception {
    compare(lc, (long)  0, (long)  0, Comparison.EQ);
    compare(lc, (long)  0, (long)  1, Comparison.LT);
    compare(lc, (long) -1, (long)  1, Comparison.LT);
    compare(lc, (long)  1, (long) -1, Comparison.GT);
    compare(lc, (long) -2, (long) -1, Comparison.LT);
    compare(lc, (long)  1, (long)  2, Comparison.LT);
    compare(lc, (long)  2, (long)  1, Comparison.GT);
  }

  public void testFloat() throws Exception {
    compare(lc,  0.0f,  0.0f, Comparison.EQ);
    compare(lc,  0.0f,  1.0f, Comparison.LT);
    compare(lc, -1.0f,  1.0f, Comparison.LT);
    compare(lc,  1.0f, -1.0f, Comparison.GT);
    compare(lc, -2.0f, -1.0f, Comparison.LT);
    compare(lc,  1.0f,  2.0f, Comparison.LT);
    compare(lc,  2.0f,  1.0f, Comparison.GT);
    compare(lc,  2.1f,  0.9f, Comparison.GT);
  }

  public void testDouble() throws Exception {
    compare(lc,  0.0d,  0.0d, Comparison.EQ);
    compare(lc,  0.0d,  1.0d, Comparison.LT);
    compare(lc, -1.0d,  1.0d, Comparison.LT);
    compare(lc,  1.0d, -1.0d, Comparison.GT);
    compare(lc, -2.0d, -1.0d, Comparison.LT);
    compare(lc,  1.0d,  2.0d, Comparison.LT);
    compare(lc,  2.0d,  1.0d, Comparison.GT);
    compare(lc,  2.1d,  0.9d, Comparison.GT);
  }

  public void testString() throws Exception {
    compare(lc, "",        "",         Comparison.EQ);
    compare(lc, "aa",      "a",        Comparison.GT);
    compare(lc, "a",       "b",        Comparison.LT);
    compare(lc, "b",       "a",        Comparison.GT);
    compare(lc, "baah",    "aardvark", Comparison.GT);
    compare(lc, "Chloé",   "Réal",     Comparison.LT);
    compare(lc, "chowder", "Réal",     Comparison.GT);
    compare(lc, "Réal",    "chowder",  Comparison.LT);
    compare(lc, "Réal",    "Réa",      Comparison.GT);
    compare(lc, "Réal",    "Réa",      Comparison.GT);
    compare(lc, "\u0061\u00a2\u0f00", "\u0061\u00a2\u0f00\u0061", Comparison.LT);
  }
  
  public void testChar() throws Exception {
    compare(lc, 'a', 'a', Comparison.EQ);
    compare(lc, 'a', 'b', Comparison.LT);
    compare(lc, 'b', 'a', Comparison.GT);
  }
  
  public void testNull() throws Exception {
    compare(lc, null,     null,     Comparison.EQ);
    compare(lc, null,     "hi mom", Comparison.GT);
    compare(lc, "hi mom", null,     Comparison.LT);
  }

  public void testObject() throws Exception {
    compare(lc, new BigInteger("1"),  new BigInteger("1"), Comparison.EQ);
    compare(lc, new BigInteger("1"),  new BigInteger("0"), Comparison.GT);
    compare(lc, new BigInteger("-1"), new BigInteger("0"), Comparison.LT);
  }

  public void testIntPerformance() throws Exception {
    ByteBuffer b1 = ByteBuffer.allocate(5).put(0, DSCODE.INTEGER);
    ByteBuffer b2 = ByteBuffer.allocate(5).put(0, DSCODE.INTEGER);
    
    for (int n = 0; n < 5; n++) {
    long diff = 0;
      int count = 10000000;
      long start = System.nanoTime();
      for (int i = 0; i < count; i++) {
        b1.putInt(1, i);
        b2.putInt(1, i + 1);
        diff += lc.compare(b1.array(), b1.arrayOffset(), b1.capacity(), b2.array(), b2.arrayOffset(), b2.capacity());
      }
      long nanos = System.nanoTime() - start;
      
      System.out.printf("(%d) %f int comparisons / ms\n", diff, 1000000.0 * count / nanos);
  
      diff = 0;
      start = System.nanoTime();
      for (int i = 0; i < count; i++) {
        b1.putInt(1, i);
        b2.putInt(1, i + 1);
        diff += Bytes.compareTo(b1.array(), b1.arrayOffset(), b1.capacity(), b2.array(), b2.arrayOffset(), b2.capacity());
      }
      nanos = System.nanoTime() - start;
      
      System.out.printf("(%d) %f byte comparisons / ms\n\n", diff, 1000000.0 * count / nanos);
    }
  }

  protected void setUp() {
    lc = new LexicographicalComparator();
  }
}
