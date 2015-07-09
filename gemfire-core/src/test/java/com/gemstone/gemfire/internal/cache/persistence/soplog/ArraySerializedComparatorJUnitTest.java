/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ArraySerializedComparatorJUnitTest extends ComparisonTestCase {
  protected ArraySerializedComparator comp;
  
  public void testSearch() throws IOException {
    byte[] k1 = comp.createCompositeKey(convert("aaaa"), convert(1), convert(true));
    byte[] k2 = comp.createCompositeKey(convert("bbbb"), convert(2), convert(false));
    byte[] k3 = comp.createCompositeKey(convert("bbbb"), convert(3), convert(true));
    byte[] k4 = comp.createCompositeKey(convert("cccc"), convert(1), convert(false));

    byte[] s1 = comp.createCompositeKey(convert("aaaa"), 
        new byte[] {SoplogToken.WILDCARD.toByte()}, new byte[] {SoplogToken.WILDCARD.toByte()});
    
    
    byte[] s2 = comp.createCompositeKey(convert("bbbb"), 
        new byte[] {SoplogToken.WILDCARD.toByte()}, new byte[] {SoplogToken.WILDCARD.toByte()});
    
    byte[] s3 = comp.createCompositeKey(new byte[] {SoplogToken.WILDCARD.toByte()}, convert(1), 
        new byte[] {SoplogToken.WILDCARD.toByte()});
    
    compareAsIs(comp, k1, s1, Comparison.EQ);
    compareAsIs(comp, k2, s1, Comparison.GT);
    compareAsIs(comp, k1, s2, Comparison.LT);
    compareAsIs(comp, k2, s2, Comparison.EQ);
    compareAsIs(comp, k3, s2, Comparison.EQ);
    compareAsIs(comp, k4, s2, Comparison.GT);
    compareAsIs(comp, s3, k4, Comparison.EQ);
  }
  
  public void testCompositeKey() throws IOException {
    byte[] k1 = comp.createCompositeKey(convert("aaaa"), convert(1), convert(true));
    byte[] k2 = comp.createCompositeKey(convert("bbbb"), convert(2), convert(false));
    byte[] k3 = comp.createCompositeKey(convert("bbbb"), convert(3), convert(true));
    byte[] k4 = comp.createCompositeKey(convert("cccc"), convert(1), convert(false));
    byte[] k5 = comp.createCompositeKey(convert(null),   convert(1), convert(false));
    byte[] k6 = comp.createCompositeKey(convert(null),   convert(1), convert(true));
    
    compareAsIs(comp, k1, k1, Comparison.EQ);
    compareAsIs(comp, k1, k2, Comparison.LT);
    compareAsIs(comp, k2, k1, Comparison.GT);
    compareAsIs(comp, k2, k3, Comparison.LT);
    compareAsIs(comp, k3, k4, Comparison.LT);
    
    compareAsIs(comp, k4, k5, Comparison.LT);
    compareAsIs(comp, k5, k1, Comparison.GT);
    compareAsIs(comp, k5, k6, Comparison.LT);
  }
  
  public void testGetKey() throws Exception {
    Object[] vals = new Object[] { "aaaa", 1, true };
    
    byte[][] composite = new byte[][] { convert(vals[0]), convert(vals[1]), convert(vals[2]) };
    ByteBuffer key = ByteBuffer.wrap(comp.createCompositeKey(composite));
    
    for (int i = 0; i < 3; i++) {
      ByteBuffer subkey = comp.getKey(key, i);
      assertEquals(vals[i], recover(subkey.array(), subkey.arrayOffset(), subkey.remaining()));
    }
  }

  public void setUp() {
    comp = new ArraySerializedComparator();
    comp.setComparators(new SerializedComparator[] {
        new LexicographicalComparator(), // string
        new LexicographicalComparator(), // int
        new LexicographicalComparator()  // boolean
    });
  }
}
