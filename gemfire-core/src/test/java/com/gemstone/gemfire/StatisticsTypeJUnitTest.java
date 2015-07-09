/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests the functionality of the {@link StatisticsType} class.
 *
 * @author David Whitlock
 *
 */
@Category(IntegrationTest.class)
public class StatisticsTypeJUnitTest extends GemFireTestCase {

  private StatisticsFactory factory() {
    return InternalDistributedSystem.getAnyInstance();
  }
  
  /**
   * Get the offset of an unknown statistic
   */
  @Test
  public void testNameToIdUnknownStatistic() {
    StatisticDescriptor[] stats = {
      factory().createIntGauge("test", "TEST", "ms")
    };

    StatisticsType type = factory().createType("testNameToIdUnknownStatistic", "TEST", stats);
    assertEquals(0, type.nameToId("test"));
    try {
      type.nameToId("Fred");
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  @Test
  public void testNameToDescriptorUnknownStatistic() {
    StatisticDescriptor[] stats = {
      factory().createIntGauge("test", "TEST", "ms")
    };

    StatisticsType type = factory().createType("testNameToDescriptorUnknownStatistic", "TEST", stats);
    assertEquals("test", type.nameToDescriptor("test").getName());
    try {
      type.nameToDescriptor("Fred");
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

}
