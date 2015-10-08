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
