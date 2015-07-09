/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.bean.stats;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.NanoTimer;

/**
 * Base test case for the management.bean.stats tests.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public abstract class MBeanStatsTestCase {

  protected static final long SLEEP = 100;
  protected static final long TIMEOUT = 4*1000;
  
  protected InternalDistributedSystem system;

  
  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
    //System.setProperty("gemfire.stats.debug.debugSampleCollector", "true");

    final Properties props = new Properties();
    //props.setProperty("log-level", "finest");
    props.setProperty("mcast-port", "0");
    props.setProperty("enable-time-statistics", "true");
    props.setProperty("statistic-sampling-enabled", "false");
    props.setProperty("statistic-sample-rate", "60000");
    
    this.system = (InternalDistributedSystem) DistributedSystem.connect(props);
    assertNotNull(this.system.getStatSampler());
    assertNotNull(this.system.getStatSampler().waitForSampleCollector(TIMEOUT));
    
    new CacheFactory().create();
    
    init();
    
    sample();
  }

  @After
  public void tearDown() throws Exception {
    //System.clearProperty("gemfire.stats.debug.debugSampleCollector");
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
  
  protected abstract void init();
}
