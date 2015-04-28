/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import junit.framework.TestCase;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.DiskStoreImpl.OplogEntryIdSet;
import com.gemstone.junit.UnitTest;

/**
 * Tests DiskStoreImpl.OplogEntryIdSet
 * 
 * @author darrel
 *  
 */
@Category(UnitTest.class)
public class OplogEntryIdSetJUnitTest extends TestCase
{
  public OplogEntryIdSetJUnitTest(String arg0) {
    super(arg0);
  }

  public void testBasics() {
    OplogEntryIdSet s = new OplogEntryIdSet();
    for (long i=1; i < 777777; i++) {
      assertEquals(false, s.contains(i));
    }
    for (long i=1; i < 777777; i++) {
      s.add(i);
    }
    for (long i=1; i < 777777; i++) {
      assertEquals(true, s.contains(i));
    }

    try {
      s.add(DiskStoreImpl.INVALID_ID);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    assertEquals(false, s.contains(0));

    assertEquals(false, s.contains(0x00000000FFFFFFFFL));
    s.add(0x00000000FFFFFFFFL);
    assertEquals(true, s.contains(0x00000000FFFFFFFFL));

    for (long i=0x00000000FFFFFFFFL+1; i < 0x00000000FFFFFFFFL+777777; i++) {
      assertEquals(false, s.contains(i));
    }
    for (long i=0x00000000FFFFFFFFL+1; i < 0x00000000FFFFFFFFL+777777; i++) {
      s.add(i);
    }
    for (long i=0x00000000FFFFFFFFL+1; i < 0x00000000FFFFFFFFL+777777; i++) {
      assertEquals(true, s.contains(i));
    }

    for (long i=1; i < 777777; i++) {
      assertEquals(true, s.contains(i));
    }

    assertEquals(false, s.contains(Long.MAX_VALUE));
    s.add(Long.MAX_VALUE);
    assertEquals(true, s.contains(Long.MAX_VALUE));
    assertEquals(false, s.contains(Long.MIN_VALUE));
    s.add(Long.MIN_VALUE);
    assertEquals(true, s.contains(Long.MIN_VALUE));
  }
}
