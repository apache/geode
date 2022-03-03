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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Integration tests for {@link StatisticsType} as implemented by {@link DistributedSystem}.
 */
@Category({StatisticsTest.class})
public class DistributedSystemStatisticsTypeIntegrationTest {

  private DistributedSystem system;
  private StatisticsType type;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(NAME, getUniqueName());
    system = DistributedSystem.connect(props);

    StatisticDescriptor[] stats = {factory().createIntGauge("test", "TEST", "ms")};

    type = factory().createType(getUniqueName(), "TEST", stats);
  }

  @After
  public void tearDown() throws Exception {
    system.disconnect();
    system = null;
  }

  @Test
  public void testNameToIdUnknownStatistic() {
    assertThat(type.nameToId("test")).isEqualTo(0);
    assertThatThrownBy(() -> type.nameToId("Fred"))
        .isExactlyInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testNameToDescriptorUnknownStatistic() {
    assertThat(type.nameToDescriptor("test").getName()).isEqualTo("test");
    assertThatThrownBy(() -> type.nameToDescriptor("Fred"))
        .isExactlyInstanceOf(IllegalArgumentException.class);
  }

  private String getUniqueName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  private StatisticsFactory factory() {
    return system;
  }

}
