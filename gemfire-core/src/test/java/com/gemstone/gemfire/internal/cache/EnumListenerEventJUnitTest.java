/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * @author Mitul Bid
 * @author Vishal Rao
 * 
 */
@Category(UnitTest.class)
public class EnumListenerEventJUnitTest extends TestCase
{
  /**
   * tests whether EnumListenerEvent.getEnumListenerEvent(int cCode) returns the
   * right result
   * 
   */
  public void testGetEnumListEvent()
  {
    checkAndAssert(0,  null);
    checkAndAssert(1,  EnumListenerEvent.AFTER_CREATE);
    checkAndAssert(2,  EnumListenerEvent.AFTER_UPDATE);
    checkAndAssert(3,  EnumListenerEvent.AFTER_INVALIDATE);
    checkAndAssert(4,  EnumListenerEvent.AFTER_DESTROY);
    checkAndAssert(5,  EnumListenerEvent.AFTER_REGION_CREATE);
    checkAndAssert(6,  EnumListenerEvent.AFTER_REGION_INVALIDATE);
    checkAndAssert(7,  EnumListenerEvent.AFTER_REGION_CLEAR);
    checkAndAssert(8,  EnumListenerEvent.AFTER_REGION_DESTROY);
    checkAndAssert(9,  EnumListenerEvent.AFTER_REMOTE_REGION_CREATE);
    checkAndAssert(10, EnumListenerEvent.AFTER_REMOTE_REGION_DEPARTURE);
    checkAndAssert(11, EnumListenerEvent.AFTER_REMOTE_REGION_CRASH);
    checkAndAssert(12, EnumListenerEvent.AFTER_ROLE_GAIN);
    checkAndAssert(13, EnumListenerEvent.AFTER_ROLE_LOSS);
    checkAndAssert(14, EnumListenerEvent.AFTER_REGION_LIVE);
    checkAndAssert(15, EnumListenerEvent.AFTER_REGISTER_INSTANTIATOR);    
    checkAndAssert(16, EnumListenerEvent.AFTER_REGISTER_DATASERIALIZER);
    checkAndAssert(17, EnumListenerEvent.AFTER_TOMBSTONE_EXPIRATION);
    // extra non-existent code checks as a markers so that this test will
    // fail if further events are added (0th or +1 codes) without updating this test
    checkAndAssert(18, EnumListenerEvent.TIMESTAMP_UPDATE);
    checkAndAssert(19, null);
  }
  
  // check that the code and object both match
  private void checkAndAssert(int code, EnumListenerEvent event) {
    EnumListenerEvent localEvent = EnumListenerEvent.getEnumListenerEvent(code); 
    Assert.assertTrue( localEvent == event);
    if (localEvent != null) {
      Assert.assertTrue( localEvent.getEventCode() == code);
    }
  }
}
