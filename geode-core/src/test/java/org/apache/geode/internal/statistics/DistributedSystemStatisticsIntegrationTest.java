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
package com.gemstone.gemfire.internal.statistics;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for {@link Statistics} as implemented by {@link DistributedSystem}.
 */
@Category(IntegrationTest.class)
public class DistributedSystemStatisticsIntegrationTest {

  private DistributedSystem system;

  private String statName1;
  private String statName2;
  private String statName3;
  private String[] statNames;

  private Random random;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(NAME, getUniqueName());

    this.system = DistributedSystem.connect(props);

    this.statName1 = "one";
    this.statName2 = "two";
    this.statName3 = "three";
    this.statNames = new String[] { statName1, statName2, statName3 };

    this.random = new Random();
  }

  @After
  public void tearDown() throws Exception {
    this.system.disconnect();
    this.system = null;
  }

  /**
   * Tests {@code int} statistics
   */
  @Test
  public void testIntStatistics() {
    Statistics stats = setUpIntStatistics(3);

    for (int j = 0; j < this.statNames.length; j++) {
      String statName = this.statNames[j];
      for (int i = 0; i < 10; i++) {
        stats.setInt(statName, i);
        stats.incInt(statName, 1);
        assertThat(stats.getInt(statName)).isEqualTo(i + 1);
      }
    }
  }

  /**
   * Tests {@code long} statistics
   */
  @Test
  public void testLongStatistics() {
    Statistics stats = setUpLongStatistics(3);

    // Set/get some random long values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < this.statNames.length; j++) {
        String statName = this.statNames[j];
        long value = this.random.nextLong();
        stats.setLong(statName, value);
        assertThat(stats.getLong(statName)).isEqualTo(value);
      }
    }

    // Increment by some random values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < this.statNames.length; j++) {
        String statName = this.statNames[j];
        long inc = this.random.nextLong();
        long before = stats.getLong(statName);
        stats.incLong(statName, inc);
        assertThat(stats.getLong(statName)).isEqualTo(before + inc);
      }
    }
  }

  /**
   * Tests {@code double} statistics
   */
  @Test
  public void testDoubleStatistics() {
    Statistics stats = setUpDoubleStatistics(3);

    // Set/get some random double values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < this.statNames.length; j++) {
        String statName = this.statNames[j];
        double value = this.random.nextDouble();
        stats.setDouble(statName, value);
        assertThat(stats.getDouble(statName)).isEqualTo(value);
      }
    }

    // Increment by some random values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < this.statNames.length; j++) {
        String statName = this.statNames[j];
        double inc = this.random.nextDouble();
        double before = stats.getDouble(statName);
        stats.incDouble(statName, inc);
        assertThat(stats.getDouble(statName)).isEqualTo(before + inc);
      }
    }
  }

  /**
   * Tests that accessing an {@code int} stat throws the appropriate exceptions.
   */
  @Test
  public void testAccessingIntStat() {
    Statistics stats = setUpIntStatistics(1);

    assertThat(stats.getInt(this.statName1)).isEqualTo(0);
    assertThatThrownBy(() -> stats.getDouble(this.statName1)).isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.getLong(this.statName1)).isExactlyInstanceOf(IllegalArgumentException.class);

    stats.setInt(this.statName1, 4);
    assertThatThrownBy(() -> stats.setDouble(this.statName1, 4.0)).isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.setLong(this.statName1, 4L)).isExactlyInstanceOf(IllegalArgumentException.class);

    stats.incInt(this.statName1, 4);
    assertThatThrownBy(() -> stats.incDouble(this.statName1, 4.0)).isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.incLong(this.statName1, 4L)).isExactlyInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Tests that accessing a {@code long} stat throws the appropriate exceptions.
   */
  @Test
  public void testAccessingLongStat() {
    Statistics stats = setUpLongStatistics(1);

    assertThat(stats.getLong(this.statName1)).isEqualTo(0L);
    assertThatThrownBy(() -> stats.getDouble(this.statName1)).isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.getInt(this.statName1)).isExactlyInstanceOf(IllegalArgumentException.class);

    stats.setLong(this.statName1, 4L);
    assertThatThrownBy(() -> stats.setDouble(this.statName1, 4.0)).isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.setInt(this.statName1, 4)).isExactlyInstanceOf(IllegalArgumentException.class);

    stats.incLong(this.statName1, 4L);
    assertThatThrownBy(() -> stats.incDouble(this.statName1, 4.0)).isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.incInt(this.statName1, 4)).isExactlyInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Tests that accessing an {@code double} stat throws the appropriate exceptions.
   */
  @Test
  public void testAccessingDoubleStat() {
    Statistics stats = setUpDoubleStatistics(1);

    assertThat(stats.getDouble(this.statName1)).isEqualTo(0.0);
    assertThatThrownBy(() -> stats.getInt(this.statName1)).isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.getLong(this.statName1)).isExactlyInstanceOf(IllegalArgumentException.class);

    stats.setDouble(this.statName1, 4.0);
    assertThatThrownBy(() -> stats.setInt(this.statName1, 4)).isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.setLong(this.statName1, 4L)).isExactlyInstanceOf(IllegalArgumentException.class);

    stats.incDouble(this.statName1, 4.0);
    assertThatThrownBy(() -> stats.incInt(this.statName1, 4)).isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.incLong(this.statName1, 4L)).isExactlyInstanceOf(IllegalArgumentException.class);
  }

  private StatisticsFactory factory() {
    return this.system;
  }

  private Statistics setUpIntStatistics(final int count) {
    String[] descriptions = new String[] {"ONE", "TWO", "THREE"};
    StatisticDescriptor[] descriptors = new StatisticDescriptor[count];
    for (int i = 0; i < count; i++) {
      descriptors[i] = factory().createIntGauge(this.statNames[i], descriptions[i], "x");
    }

    StatisticsType type = factory().createType(getUniqueName(), "", descriptors);
    Statistics stats = factory().createStatistics(type, "Display");

    for (int i = 0; i < count; i++) {
      stats.setInt(this.statNames[i], 0);
    }
    return stats;
  }

  private Statistics setUpLongStatistics(final int count) {
    String[] descriptions = new String[] {"ONE", "TWO", "THREE"};
    StatisticDescriptor[] descriptors = new StatisticDescriptor[count];
    for (int i = 0; i < count; i++) {
      descriptors[i] = factory().createLongGauge(this.statNames[i], descriptions[i], "x");
    }

    StatisticsType type = factory().createType(getUniqueName(), "", descriptors);
    Statistics stats = factory().createStatistics(type, "Display");

    for (int i = 0; i < count; i++) {
      stats.setLong(this.statNames[i], 0L);
    }
    return stats;
  }

  private Statistics setUpDoubleStatistics(final int count) {
    String[] descriptions = new String[] {"ONE", "TWO", "THREE"};
    StatisticDescriptor[] descriptors = new StatisticDescriptor[count];
    for (int i = 0; i < count; i++) {
      descriptors[i] = factory().createDoubleGauge(this.statNames[i], descriptions[i], "x");
    }

    StatisticsType type = factory().createType(getUniqueName(), "", descriptors);
    Statistics stats = factory().createStatistics(type, "Display");

    for (int i = 0; i < count; i++) {
      stats.setDouble(this.statNames[i], 0.0);
    }
    return stats;
  }

  private String getUniqueName() {
    return getClass().getSimpleName() + "_" + this.testName.getMethodName();
  }

}
