/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.UniqueIdGenerator;
import com.gemstone.junit.UnitTest;

/**
 * Tests UniqueIdGenerator.
 * @author Darrel
 * @since 5.0.2 (cbb5x_PerfScale)
 */
@Category(UnitTest.class)
public class UniqueIdGeneratorJUnitTest extends TestCase {
  
  public UniqueIdGeneratorJUnitTest() {
  }
  
  public void setup() {
  }
  
  public void tearDown() {
  }
  
  public void testBasics() throws Exception {
    UniqueIdGenerator uig = new UniqueIdGenerator(1);
    assertEquals(0, uig.obtain());
    try {
      uig.obtain();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    uig.release(0);
    assertEquals(0, uig.obtain());

    uig = new UniqueIdGenerator(32768);
    for (int i=0; i < 32768; i++) {
      assertEquals(i, uig.obtain());
    }
    try {
      uig.obtain();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    for (int i=32767; i >= 0; i--) {
      uig.release(i);
      assertEquals(i, uig.obtain());
    }
  }
}
