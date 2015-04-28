/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.ha;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier.WanType;
import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

@Category(UnitTest.class)
public class ThreadIdentifierJUnitTest extends TestCase {

  public void testPutAllId() {
    int id = 42;
    int bucketNumber = 113;
    
    long putAll = ThreadIdentifier.createFakeThreadIDForBulkOp(bucketNumber, id);
    
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(putAll));
    assertEquals(42, ThreadIdentifier.getRealThreadID(putAll));
  }
  
  public void testWanId() {
    int id = 42;
    
    long wan1 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(1, id, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan1));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan1));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan1);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.PRIMARY.matches(real_tid_with_wan));
    }
    
    long wan2 = ThreadIdentifier.createFakeThreadIDForParallelGSSecondaryBucket(1, id, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan2));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan2));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan2);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.SECONDARY.matches(real_tid_with_wan));
    }

    long wan3 = ThreadIdentifier.createFakeThreadIDForParallelGateway(1, id, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan3));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan3));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan3);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.PARALLEL.matches(real_tid_with_wan));
    }
  }
  
  public void testWanAndPutAllId() {
    int id = 42;
    int bucketNumber = 113;
    
    long putAll = ThreadIdentifier.createFakeThreadIDForBulkOp(bucketNumber, id);

    long wan1 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(1, putAll, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan1));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan1));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan1));
    { 
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan1);  
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.PRIMARY.matches(real_tid_with_wan));
    }
    
    long wan2 = ThreadIdentifier.createFakeThreadIDForParallelGSSecondaryBucket(1, putAll, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan2));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan2));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan2));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan2);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.SECONDARY.matches(real_tid_with_wan));
    }

    long wan3 = ThreadIdentifier.createFakeThreadIDForParallelGateway(1, putAll, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan3));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan3));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan3));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan3);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.PARALLEL.matches(real_tid_with_wan));
    }
    
    long tid = 4054000001L;
    assertTrue(ThreadIdentifier.isParallelWANThreadID(tid));
    assertFalse(ThreadIdentifier.isParallelWANThreadID(putAll));
  }
}
