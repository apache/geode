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

/**
 * A test of the region version holder, but using a smaller bit
 * set so that we test the merging of exceptions functionality.
 */
@Category(UnitTest.class)
public class RegionVersionHolderSmallBitSetJUnitTest extends RegionVersionHolderJUnitTest {
  
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    originalBitSetWidth = RegionVersionHolder.BIT_SET_WIDTH;
    RegionVersionHolder.BIT_SET_WIDTH = 4;
  }
  
  protected void tearDown() throws Exception {
    super.tearDown();
    RegionVersionHolder.BIT_SET_WIDTH = originalBitSetWidth;
  }
  
  
  

}
