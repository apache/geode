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
package org.apache.geode.management.bean.stats;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 
 * @since GemFire  1.4
 */
@Category(IntegrationTest.class)
public class DistributedSystemStatsJUnitTest {

  protected static final long SLEEP = 100;
  protected static final long TIMEOUT = 4 * 1000;

  protected InternalDistributedSystem system;

  protected Cache cache;

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {

    final Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "false");
    props.setProperty(STATISTIC_SAMPLE_RATE, "60000");
    props.setProperty(JMX_MANAGER, "true");
    props.setProperty(JMX_MANAGER_START, "true");
    // set JMX_MANAGER_UPDATE_RATE to practically an infinite value, so that
    // LocalManager wont try to run ManagementTask
    props.setProperty(JMX_MANAGER_UPDATE_RATE, "60000");
    props.setProperty(JMX_MANAGER_PORT, "0");

    this.system = (InternalDistributedSystem) DistributedSystem.connect(props);
    assertNotNull(this.system.getStatSampler());
    assertNotNull(this.system.getStatSampler().waitForSampleCollector(TIMEOUT));

    this.cache = new CacheFactory().create();

  }

  @Test
  public void testIssue51048() throws InterruptedException {
    SystemManagementService service = (SystemManagementService) ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean dsmbean = service.getDistributedSystemMXBean();

    CachePerfStats cachePerfStats = ((GemFireCacheImpl) cache).getCachePerfStats();

    for (int i = 1; i <= 10; i++) {
      cachePerfStats.incCreates();
    }

    sample();

    service.getLocalManager().runManagementTaskAdhoc();
    assertTrue(dsmbean.getAverageWrites() == 10);

    sample();

    service.getLocalManager().runManagementTaskAdhoc();

    assertTrue(dsmbean.getAverageWrites() == 0);

  }

  @After
  public void tearDown() throws Exception {
    // System.clearProperty("gemfire.stats.debug.debugSampleCollector");
    this.system.disconnect();
    this.system = null;
  }

  protected void waitForNotification() throws InterruptedException {
    this.system.getStatSampler().waitForSample(TIMEOUT);
    Thread.sleep(SLEEP);
  }

  protected void sample() throws InterruptedException {
    this.system.getStatSampler().getSampleCollector().sample(NanoTimer.getTime());
    Thread.sleep(SLEEP);
  }

}
