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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class IndexComparatorJUnitTest extends ComparisonTestCase {
  protected IndexSerializedComparator comp;
  
  public void testSearch() throws IOException {
    byte[] k1 = comp.createCompositeKey(convert("aaaa"), convert(1));
    byte[] k2 = comp.createCompositeKey(convert("bbbb"), convert(2));
    byte[] k3 = comp.createCompositeKey(convert("bbbb"), convert(3));
    byte[] k4 = comp.createCompositeKey(convert(null), convert(1));

    byte[] s1 = comp.createCompositeKey(convert("aaaa"), new byte[] {SoplogToken.WILDCARD.toByte()});
    byte[] s2 = comp.createCompositeKey(convert("bbbb"), new byte[] {SoplogToken.WILDCARD.toByte()});
    byte[] s3 = comp.createCompositeKey(new byte[] {SoplogToken.WILDCARD.toByte()}, convert(1));
    
    compareAsIs(comp, k1, s1, Comparison.EQ);
    compareAsIs(comp, k2, s1, Comparison.GT);
    compareAsIs(comp, k1, s2, Comparison.LT);
    compareAsIs(comp, k2, s2, Comparison.EQ);
    compareAsIs(comp, k3, s2, Comparison.EQ);
    compareAsIs(comp, k4, s2, Comparison.GT);
    compareAsIs(comp, s3, k4, Comparison.EQ);
  }
  
  public void testCompositeKey() throws IOException {
    byte[] k1 = comp.createCompositeKey(convert("aaaa"), convert(1));
    byte[] k2 = comp.createCompositeKey(convert("bbbb"), convert(2));
    byte[] k3 = comp.createCompositeKey(convert("bbbb"), convert(3));
    byte[] k4 = comp.createCompositeKey(convert("cccc"), convert(1));
    byte[] k5 = comp.createCompositeKey(convert(null), convert(1));
    
    compareAsIs(comp, k1, k1, Comparison.EQ);
    compareAsIs(comp, k1, k2, Comparison.LT);
    compareAsIs(comp, k2, k1, Comparison.GT);
    compareAsIs(comp, k2, k3, Comparison.LT);
    compareAsIs(comp, k3, k4, Comparison.LT);
    
    compareAsIs(comp, k4, k5, Comparison.LT);
    compareAsIs(comp, k5, k1, Comparison.GT);
  }
  
  public void testGetKey() throws Exception {
    ByteBuffer key = ByteBuffer.wrap(comp.createCompositeKey(convert("aaaa"), convert(1)));
    
    ByteBuffer k1 = comp.getKey(key, 0);
    assertEquals("aaaa", recover(k1.array(), k1.arrayOffset(), k1.remaining()));
    
    ByteBuffer k2 = comp.getKey(key, 1);
    assertEquals(1, recover(k2.array(), k2.arrayOffset(), k2.remaining()));
  }
  
  public void setUp() {
    comp = new IndexSerializedComparator();
  }
}
