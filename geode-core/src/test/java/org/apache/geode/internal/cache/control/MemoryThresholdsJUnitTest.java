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
package org.apache.geode.internal.cache.control;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.control.MemoryThresholds.MemoryState;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class MemoryThresholdsJUnitTest {

  @Test
  public void testDefaults() {
    MemoryThresholds thresholds = new MemoryThresholds(1000);
    assertFalse(thresholds.isEvictionThresholdEnabled());
    assertFalse(thresholds.isCriticalThresholdEnabled());
    assertEquals(1000l, thresholds.getMaxMemoryBytes());
    assertEquals(0f, thresholds.getEvictionThreshold(), 0.01);
    assertEquals(0f, thresholds.getCriticalThreshold(), 0.01);
  }

  @Test
  public void testSetAndGetters() {
    try {
      new MemoryThresholds(1000, 49.8f, 84.2f);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
    
    try {
      new MemoryThresholds(1000, 100.1f, 0f);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
    
    try {
      new MemoryThresholds(1000, -0.1f, 0f);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
    
    try {
      new MemoryThresholds(1000, 0f, 100.1f);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
    
    try {
      new MemoryThresholds(1000, 0f, -0.1f);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
    
    MemoryThresholds thresholds = new MemoryThresholds(1000, 84.2f, 49.8f);
    assertTrue(thresholds.isEvictionThresholdEnabled());
    assertTrue(thresholds.isCriticalThresholdEnabled());
    assertEquals(1000l, thresholds.getMaxMemoryBytes());
    assertEquals(49.8f, thresholds.getEvictionThreshold(), 0.01);
    assertTrue(Math.abs(498 - thresholds.getEvictionThresholdBytes()) <= 1); // Allow for rounding
    assertEquals(84.2f, thresholds.getCriticalThreshold(), 0.01);
    assertTrue(Math.abs(842 - thresholds.getCriticalThresholdBytes()) <= 1); // Allow for rounding
  }
 
  @Test
  public void testTransitionsNoThresholds() {
    MemoryThresholds thresholds = new MemoryThresholds(1000, 0f, 0f);
    assertEquals(MemoryState.DISABLED, thresholds.computeNextState(MemoryState.DISABLED, 100));
    assertEquals(MemoryState.DISABLED, thresholds.computeNextState(MemoryState.EVICTION_DISABLED, 100));
    assertEquals(MemoryState.DISABLED, thresholds.computeNextState(MemoryState.EVICTION_DISABLED_CRITICAL, 100));
    assertEquals(MemoryState.DISABLED, thresholds.computeNextState(MemoryState.CRITICAL_DISABLED, 100));
    assertEquals(MemoryState.DISABLED, thresholds.computeNextState(MemoryState.EVICTION_CRITICAL_DISABLED, 100));
    assertEquals(MemoryState.DISABLED, thresholds.computeNextState(MemoryState.NORMAL, 100));
    assertEquals(MemoryState.DISABLED, thresholds.computeNextState(MemoryState.EVICTION, 100));
    assertEquals(MemoryState.DISABLED, thresholds.computeNextState(MemoryState.CRITICAL, 100));
    assertEquals(MemoryState.DISABLED, thresholds.computeNextState(MemoryState.EVICTION_CRITICAL, 100));
  }
  
  @Test
  public void testTransitionsEvictionSet() {
    MemoryThresholds thresholds = new MemoryThresholds(1000, 0f, 50f);
    
    assertEquals(MemoryState.CRITICAL_DISABLED, thresholds.computeNextState(MemoryState.DISABLED, 499));
    assertEquals(MemoryState.CRITICAL_DISABLED, thresholds.computeNextState(MemoryState.EVICTION, 450));
    assertEquals(MemoryState.CRITICAL_DISABLED, thresholds.computeNextState(MemoryState.CRITICAL, 499));
    
    assertEquals(MemoryState.EVICTION_CRITICAL_DISABLED, thresholds.computeNextState(MemoryState.DISABLED, 500));
    assertEquals(MemoryState.EVICTION_CRITICAL_DISABLED, thresholds.computeNextState(MemoryState.EVICTION, 499));
    assertEquals(MemoryState.EVICTION_CRITICAL_DISABLED, thresholds.computeNextState(MemoryState.CRITICAL, 500));
  }
  
  @Test
  public void testTransitionsCriticalSet() {
    MemoryThresholds thresholds = new MemoryThresholds(1000, 50f, 0f);
    
    assertEquals(MemoryState.EVICTION_DISABLED, thresholds.computeNextState(MemoryState.DISABLED, 499));
    assertEquals(MemoryState.EVICTION_DISABLED, thresholds.computeNextState(MemoryState.EVICTION, 499));
    
    assertEquals(MemoryState.EVICTION_DISABLED_CRITICAL, thresholds.computeNextState(MemoryState.DISABLED, 500));
    assertEquals(MemoryState.EVICTION_DISABLED_CRITICAL, thresholds.computeNextState(MemoryState.EVICTION, 500));
    assertEquals(MemoryState.EVICTION_DISABLED_CRITICAL, thresholds.computeNextState(MemoryState.CRITICAL, 499));
  }
  
  @Test
  public void testTransitionsEvictionAndCriticalSet() {
    MemoryThresholds thresholds = new MemoryThresholds(1000, 80f, 50f);
    
    assertEquals(MemoryState.NORMAL, thresholds.computeNextState(MemoryState.DISABLED, 0));
    assertEquals(MemoryState.NORMAL, thresholds.computeNextState(MemoryState.DISABLED, 499));
    assertEquals(MemoryState.NORMAL, thresholds.computeNextState(MemoryState.NORMAL, 499));
    assertEquals(MemoryState.NORMAL, thresholds.computeNextState(MemoryState.CRITICAL, 499));
    
    assertEquals(MemoryState.EVICTION, thresholds.computeNextState(MemoryState.DISABLED, 500));
    assertEquals(MemoryState.EVICTION, thresholds.computeNextState(MemoryState.NORMAL, 500));
    assertEquals(MemoryState.EVICTION, thresholds.computeNextState(MemoryState.EVICTION, 499));
    assertEquals(MemoryState.EVICTION, thresholds.computeNextState(MemoryState.EVICTION, 500));
    assertEquals(MemoryState.EVICTION, thresholds.computeNextState(MemoryState.EVICTION, 799));
    
    assertEquals(MemoryState.EVICTION_CRITICAL, thresholds.computeNextState(MemoryState.DISABLED, 800));
    assertEquals(MemoryState.EVICTION_CRITICAL, thresholds.computeNextState(MemoryState.NORMAL, 800));
    assertEquals(MemoryState.EVICTION_CRITICAL, thresholds.computeNextState(MemoryState.EVICTION, 800));
    assertEquals(MemoryState.EVICTION_CRITICAL, thresholds.computeNextState(MemoryState.CRITICAL, 800));
    assertEquals(MemoryState.EVICTION_CRITICAL, thresholds.computeNextState(MemoryState.CRITICAL, 799));
    assertEquals(MemoryState.EVICTION_CRITICAL, thresholds.computeNextState(MemoryState.EVICTION_CRITICAL, 800));
    assertEquals(MemoryState.EVICTION_CRITICAL, thresholds.computeNextState(MemoryState.EVICTION_CRITICAL, 799));
  }
}
