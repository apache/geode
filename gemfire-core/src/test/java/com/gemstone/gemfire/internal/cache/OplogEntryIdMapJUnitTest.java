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

import com.gemstone.gemfire.internal.cache.Oplog.OplogEntryIdMap;
import com.gemstone.junit.UnitTest;

/**
 * Tests DiskStoreImpl.OplogEntryIdMap
 * 
 * @author darrel
 *  
 */
@Category(UnitTest.class)
public class OplogEntryIdMapJUnitTest extends TestCase
{
  public OplogEntryIdMapJUnitTest(String arg0) {
    super(arg0);
  }

  public void testBasics() {
    OplogEntryIdMap m = new OplogEntryIdMap();
    for (long i=1; i <= 777777; i++) {
      assertEquals(null, m.get(i));
    }
    for (long i=1; i <= 777777; i++) {
      m.put(i, new Long(i));
    }
    for (long i=1; i <= 777777; i++) {
      assertEquals(new Long(i), m.get(i));
    }

    assertEquals(777777, m.size());

    try {
      m.put(DiskStoreImpl.INVALID_ID, new Object());
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    assertEquals(null, m.get(0));
    assertEquals(777777, m.size());

    assertEquals(null, m.get(0x00000000FFFFFFFFL));
    m.put(0x00000000FFFFFFFFL, new Long(0x00000000FFFFFFFFL));
    assertEquals(new Long(0x00000000FFFFFFFFL), m.get(0x00000000FFFFFFFFL));
    assertEquals(777777+1, m.size());

    for (long i=0x00000000FFFFFFFFL+1; i <= 0x00000000FFFFFFFFL+777777; i++) {
      assertEquals(null, m.get(i));
    }
    for (long i=0x00000000FFFFFFFFL+1; i <= 0x00000000FFFFFFFFL+777777; i++) {
      m.put(i, new Long(i));
    }
    for (long i=0x00000000FFFFFFFFL+1; i <= 0x00000000FFFFFFFFL+777777; i++) {
      assertEquals(new Long(i), m.get(i));
    }
    assertEquals(777777+1+777777, m.size());

    for (long i=1; i < 777777; i++) {
      assertEquals(new Long(i), m.get(i));
    }

    assertEquals(null, m.get(Long.MAX_VALUE));
    m.put(Long.MAX_VALUE, new Long(Long.MAX_VALUE));
    assertEquals(new Long(Long.MAX_VALUE), m.get(Long.MAX_VALUE));
    assertEquals(777777+1+777777+1, m.size());
    assertEquals(null, m.get(Long.MIN_VALUE));
    m.put(Long.MIN_VALUE, new Long(Long.MIN_VALUE));
    assertEquals(new Long(Long.MIN_VALUE), m.get(Long.MIN_VALUE));
    assertEquals(777777+1+777777+1+1, m.size());

    int count = 0;
    for (OplogEntryIdMap.Iterator it = m.iterator(); it.hasNext();) {
      count++;
      it.advance();
      it.key();
      it.value();
    }
    assertEquals(777777+1+777777+1+1, count);
  }
}
