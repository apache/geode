/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.statistics;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Integration tests for {@link Statistics} as implemented by {@link DistributedSystem}.
 */
@Category({StatisticsTest.class})
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

    system = DistributedSystem.connect(props);

    statName1 = "one";
    statName2 = "two";
    statName3 = "three";
    statNames = new String[] {statName1, statName2, statName3};

    random = new Random();
  }

  @After
  public void tearDown() throws Exception {
    system.disconnect();
    system = null;
  }

  /**
   * Tests {@code int} statistics
   */
  @Test
  public void testIntStatistics() {
    Statistics stats = setUpIntStatistics(3);

    for (int j = 0; j < statNames.length; j++) {
      String statName = statNames[j];
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
      for (int j = 0; j < statNames.length; j++) {
        String statName = statNames[j];
        long value = random.nextLong();
        stats.setLong(statName, value);
        assertThat(stats.getLong(statName)).isEqualTo(value);
      }
    }

    // Increment by some random values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < statNames.length; j++) {
        String statName = statNames[j];
        long inc = random.nextLong();
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
      for (int j = 0; j < statNames.length; j++) {
        String statName = statNames[j];
        double value = random.nextDouble();
        stats.setDouble(statName, value);
        assertThat(stats.getDouble(statName)).isEqualTo(value);
      }
    }

    // Increment by some random values
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < statNames.length; j++) {
        String statName = statNames[j];
        double inc = random.nextDouble();
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

    assertThat(stats.getInt(statName1)).isEqualTo(0);
    assertThat(stats.getLong(statName1)).isEqualTo(0);
    assertThatThrownBy(() -> stats.getDouble(statName1))
        .isExactlyInstanceOf(IllegalArgumentException.class);

    stats.setInt(statName1, 4);
    assertThat(stats.getInt(statName1)).isEqualTo(4);
    stats.setLong(statName1, 5);
    assertThat(stats.getInt(statName1)).isEqualTo(5);
    assertThatThrownBy(() -> stats.setDouble(statName1, 4.0))
        .isExactlyInstanceOf(IllegalArgumentException.class);

    stats.incInt(statName1, 4);
    assertThat(stats.getInt(statName1)).isEqualTo(9);
    stats.incLong(statName1, 4);
    assertThat(stats.getInt(statName1)).isEqualTo(13);
    assertThatThrownBy(() -> stats.incDouble(statName1, 4.0))
        .isExactlyInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Tests that accessing a {@code long} stat throws the appropriate exceptions.
   */
  @Test
  public void testAccessingLongStat() {
    Statistics stats = setUpLongStatistics(1);

    assertThat(stats.getLong(statName1)).isEqualTo(0L);
    assertThat(stats.getInt(statName1)).isEqualTo(0);
    assertThatThrownBy(() -> stats.getDouble(statName1))
        .isExactlyInstanceOf(IllegalArgumentException.class);

    stats.setLong(statName1, 4L);
    assertThat(stats.getLong(statName1)).isEqualTo(4L);
    stats.setInt(statName1, 5);
    assertThat(stats.getLong(statName1)).isEqualTo(5L);
    assertThatThrownBy(() -> stats.setDouble(statName1, 4.0))
        .isExactlyInstanceOf(IllegalArgumentException.class);

    stats.incLong(statName1, 4L);
    assertThat(stats.getLong(statName1)).isEqualTo(9L);
    stats.incInt(statName1, 4);
    assertThat(stats.getLong(statName1)).isEqualTo(13L);
    assertThatThrownBy(() -> stats.incDouble(statName1, 4.0))
        .isExactlyInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Tests that accessing an {@code double} stat throws the appropriate exceptions.
   */
  @Test
  public void testAccessingDoubleStat() {
    Statistics stats = setUpDoubleStatistics(1);

    assertThat(stats.getDouble(statName1)).isEqualTo(0.0);
    assertThatThrownBy(() -> stats.getInt(statName1))
        .isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.getLong(statName1))
        .isExactlyInstanceOf(IllegalArgumentException.class);

    stats.setDouble(statName1, 4.0);
    assertThatThrownBy(() -> stats.setInt(statName1, 4))
        .isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.setLong(statName1, 4L))
        .isExactlyInstanceOf(IllegalArgumentException.class);

    stats.incDouble(statName1, 4.0);
    assertThatThrownBy(() -> stats.incInt(statName1, 4))
        .isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> stats.incLong(statName1, 4L))
        .isExactlyInstanceOf(IllegalArgumentException.class);
  }

  private StatisticsFactory factory() {
    return system;
  }

  private Statistics setUpIntStatistics(final int count) {
    String[] descriptions = new String[] {"ONE", "TWO", "THREE"};
    StatisticDescriptor[] descriptors = new StatisticDescriptor[count];
    for (int i = 0; i < count; i++) {
      descriptors[i] = factory().createIntGauge(statNames[i], descriptions[i], "x");
    }

    StatisticsType type = factory().createType(getUniqueName(), "", descriptors);
    Statistics stats = factory().createStatistics(type, "Display");

    for (int i = 0; i < count; i++) {
      stats.setInt(statNames[i], 0);
    }
    return stats;
  }

  private Statistics setUpLongStatistics(final int count) {
    String[] descriptions = new String[] {"ONE", "TWO", "THREE"};
    StatisticDescriptor[] descriptors = new StatisticDescriptor[count];
    for (int i = 0; i < count; i++) {
      descriptors[i] = factory().createLongGauge(statNames[i], descriptions[i], "x");
    }

    StatisticsType type = factory().createType(getUniqueName(), "", descriptors);
    Statistics stats = factory().createStatistics(type, "Display");

    for (int i = 0; i < count; i++) {
      stats.setLong(statNames[i], 0L);
    }
    return stats;
  }

  private Statistics setUpDoubleStatistics(final int count) {
    String[] descriptions = new String[] {"ONE", "TWO", "THREE"};
    StatisticDescriptor[] descriptors = new StatisticDescriptor[count];
    for (int i = 0; i < count; i++) {
      descriptors[i] = factory().createDoubleGauge(statNames[i], descriptions[i], "x");
    }

    StatisticsType type = factory().createType(getUniqueName(), "", descriptors);
    Statistics stats = factory().createStatistics(type, "Display");

    for (int i = 0; i < count; i++) {
      stats.setDouble(statNames[i], 0.0);
    }
    return stats;
  }

  private String getUniqueName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

}
