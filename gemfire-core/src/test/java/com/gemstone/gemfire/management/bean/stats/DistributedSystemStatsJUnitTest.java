/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.bean.stats;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author rishim
 * @since  1.4
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
    props.setProperty("mcast-port", "0");
    props.setProperty("enable-time-statistics", "true");
    props.setProperty("statistic-sampling-enabled", "false");
    props.setProperty("statistic-sample-rate", "60000");
    props.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
    props.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "true");
    // set JMX_MANAGER_UPDATE_RATE to practically an infinite value, so that
    // LocalManager wont try to run ManagementTask
    props.setProperty(DistributionConfig.JMX_MANAGER_UPDATE_RATE_NAME, "60000");
    props.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, "0");

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
