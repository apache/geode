/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.versions;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;

@Category(UnitTest.class)
public class RVVExceptionJUnitTest extends TestCase {

  public RVVExceptionJUnitTest() {
  }
  
  public void testRVVExceptionB() {
    RVVExceptionB ex = new RVVExceptionB(5, 10);
    ex.add(8);
    ex.add(6);
    assertEquals(8, ex.getHighestReceivedVersion());
    ex.add(5);
    assertEquals(8, ex.getHighestReceivedVersion());
    
  }

  public void testRVVExceptionT() {
    RVVExceptionT ex = new RVVExceptionT(5, 10);
    ex.add(8);
    ex.add(6);
    assertEquals(8, ex.getHighestReceivedVersion());

  }
}
