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
package org.apache.geode.management.bean.stats;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.categories.StatisticsTest;

@Category({JMXTest.class, StatisticsTest.class})
public class MemberLevelStatsIntegrationTest {

  @Rule
  public TestName name = new TestName();

  private InternalCache cache;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_SAMPLE_RATE, "60000");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "false");

    cache = (InternalCache) new CacheFactory(props).create();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  @Test
  public void testCacheBasedStats() {
    ManagementService service = ManagementService.getExistingManagementService(cache);
    long start = cache.getInternalResourceManager().getStats().startRebalance();

    assertEquals(1, service.getMemberMXBean().getRebalancesInProgress());

    cache.getInternalResourceManager().getStats().endRebalance(start);

    assertEquals(0, service.getMemberMXBean().getRebalancesInProgress());
  }
}
