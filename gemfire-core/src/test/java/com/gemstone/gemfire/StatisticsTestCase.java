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

import com.gemstone.gemfire.distributed.DistributedSystem;

import java.util.*;

import org.junit.Test;

/**
 * Tests the functionality of {@link Statistics}.  Uses a
 * subclass to tests JOM-only statistics.
 */
public abstract class StatisticsTestCase extends GemFireTestCase {

  /** The distributed system used in this test */
  protected DistributedSystem system;

  private StatisticsFactory factory() {
    return system;
  }

  /**
   * Creates the distributed system
   * @throws Exception 
   */
  @Override
  public void setUp() throws Exception {
    this.system = getSystem();
  }

  /**
   * Closes the distributed system
   * @throws Exception 
   */
  @Override
  public void tearDown() throws Exception {
    this.system.disconnect();
    this.system = null;
  }

  /**
   * Returns the distributed system used when creating statistics.  It
   * determines whether or not shared memory is used.
   */
  protected abstract DistributedSystem getSystem();

  ////////  Test methods

  /**
   * Tests <code>int</code> statistics
   */
  @Test
  public void testIntStatistics() {
    String statName1 = "one";
    String statName2 = "two";
    String statName3 = "three";
    String[] statNames = { statName1, statName2, statName3 };

    StatisticsType type =
      factory().createType(this.getUniqueName(), "", new
        StatisticDescriptor[] {
          factory().createIntGauge(statName1, "ONE", "x"),
          factory().createIntGauge(statName2, "TWO", "x"),
          factory().createIntGauge(statName3, "THREE", "x")
      });

    Statistics stats = factory().createAtomicStatistics(type, "Display");
    stats.setInt(statName1, 0);
    stats.setInt(statName2, 0);
    stats.setInt(statName3, 0);

    for (int j = 0; j < statNames.length; j++) {
      String statName = statNames[j];
      for (int i = 0; i < 10; i++) {
        stats.setInt(statName, i);
        stats.incInt(statName, 1);
        assertEquals(i + 1, stats.getInt(statName));
      }
    }
  }

  /**
   * Tests <code>long</code> statistics
   */
  @Test
  public void testLongStatistics() {
    String statName1 = "one";
    String statName2 = "two";
    String statName3 = "three";
    String[] statNames = { statName1, statName2, statName3 };

    StatisticsType type =
      factory().createType(this.getUniqueName(), "", new
        StatisticDescriptor[] {
          factory().createLongGauge(statName1, "ONE", "x"),
          factory().createLongGauge(statName2, "TWO", "x"),
          factory().createLongGauge(statName3, "THREE", "x")
      });

    Statistics stats = factory().createAtomicStatistics(type, "Display");
    stats.setLong(statName1, 0L);
    stats.setLong(statName2, 0L);
    stats.setLong(statName3, 0L);

    Random random = new Random();

    // Set/get some random long values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < statNames.length; j++) {
        String statName = statNames[j];
        long value = random.nextLong();
        stats.setLong(statName, value);
        assertEquals(value, stats.getLong(statName));
      }
    }

    // Increment by some random values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < statNames.length; j++) {
        String statName = statNames[j];
        long inc = random.nextLong();
        long before = stats.getLong(statName);
        stats.incLong(statName, inc);
        assertEquals(before + inc, stats.getLong(statName));
      }
    }
  }

  /**
   * Tests <code>double</code> statistics
   */
  @Test
  public void testDoubleStatistics() {
    String statName1 = "one";
    String statName2 = "two";
    String statName3 = "three";
    String[] statNames = { statName1, statName2, statName3 };

    StatisticsType type =
      factory().createType(this.getUniqueName(), "", new
        StatisticDescriptor[] {
          factory().createDoubleGauge(statName1, "ONE", "x"),
          factory().createDoubleGauge(statName2, "TWO", "x"),
          factory().createDoubleGauge(statName3, "THREE", "x")
      });

    Statistics stats = factory().createAtomicStatistics(type, "Display");
    stats.setDouble(statName1, 0.0);
    stats.setDouble(statName2, 0.0);
    stats.setDouble(statName3, 0.0);

    Random random = new Random();

    // Set/get some random double values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < statNames.length; j++) {
        String statName = statNames[j];
        double value = random.nextDouble();
        stats.setDouble(statName, value);
        assertEquals(value, stats.getDouble(statName), 0.0);
      }
    }

    // Increment by some random values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < statNames.length; j++) {
        String statName = statNames[j];
        double inc = random.nextDouble();
        double before = stats.getDouble(statName);
        stats.incDouble(statName, inc);
        assertEquals(before + inc, stats.getDouble(statName), 0.0);
      }
    }
  }

  /**
   * Tests that accessing an <code>int</code> stat throws the
   * appropriate exceptions.
   */
  @Test
  public void testAccessingIntStat() {
    String statName1 = "one";

    StatisticsType type =
      factory().createType(this.getUniqueName(), "", new
        StatisticDescriptor[] {
          factory().createIntGauge(statName1, "ONE", "x"),
      });

    Statistics stats = factory().createAtomicStatistics(type, "Display");

    stats.getInt(statName1);
    try {
      stats.getDouble(statName1);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
    try {
      stats.getLong(statName1);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    stats.setInt(statName1, 4);
    try {
      stats.setDouble(statName1, 4.0);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
    try {
      stats.setLong(statName1, 4L);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    stats.incInt(statName1, 4);
    try {
      stats.incDouble(statName1, 4.0);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
    try {
      stats.incLong(statName1, 4L);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  /**
   * Tests that accessing a <code>long</code> stat throws the
   * appropriate exceptions.
   */
  @Test
  public void testAccessingLongStat() {
    String statName1 = "one";

    StatisticsType type =
      factory().createType(this.getUniqueName(), "", new
        StatisticDescriptor[] {
          factory().createLongGauge(statName1, "ONE", "x"),
      });

    Statistics stats = factory().createAtomicStatistics(type, "Display");

    stats.getLong(statName1);
    try {
      stats.getDouble(statName1);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
    try {
      stats.getInt(statName1);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    stats.setLong(statName1, 4L);
    try {
      stats.setDouble(statName1, 4.0);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
    try {
      stats.setInt(statName1, 4);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    stats.incLong(statName1, 4L);
    try {
      stats.incDouble(statName1, 4.0);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
    try {
      stats.incInt(statName1, 4);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  /**
   * Tests that accessing an <code>double</code> stat throws the
   * appropriate exceptions.
   */
  @Test
  public void testAccessingDoubleStat() {
    String statName1 = "one";

    StatisticsType type =
      factory().createType(this.getUniqueName(), "", new
        StatisticDescriptor[] {
          factory().createDoubleGauge(statName1, "ONE", "x"),
      });

    Statistics stats = factory().createStatistics(type, "Display");

    stats.getDouble(statName1);
    try {
      stats.getInt(statName1);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
    try {
      stats.getLong(statName1);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    stats.setDouble(statName1, 4.0);
    try {
      stats.setInt(statName1, 4);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
    try {
      stats.setLong(statName1, 4L);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    stats.incDouble(statName1, 4.0);
    try {
      stats.incInt(statName1, 4);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
    try {
      stats.incLong(statName1, 4L);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }
}
