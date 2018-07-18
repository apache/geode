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
package org.apache.geode.internal.monitoring;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 *
 * @since Geode 1.5
 */
@Category(IntegrationTest.class)
public class ThreadsMonitoringIntegrationTest {

  private Properties nonDefault;
  private InternalCache cache;

  @Before
  public void setUpThreadsMonitoringIntegrationTest() throws Exception {
    initInternalDistributedSystem();
  }

  @After
  public void tearDownThreadsMonitoringIntegrationTest() throws Exception {
    stopInternalDistributedSystem();
  }

  private void stopInternalDistributedSystem() {
    cache.close();
  }

  private void initInternalDistributedSystem() {
    nonDefault = new Properties();
    nonDefault.put(ConfigurationProperties.MCAST_PORT, "0");
    nonDefault.put(ConfigurationProperties.LOCATORS, "");

    cache = (InternalCache) new CacheFactory(nonDefault).create();
  }

  /**
   * Tests that in case no instance of internal distribution system exists dummy instance is used
   */
  @Test
  public void testThreadsMonitoringWorkflow() {

    ThreadsMonitoring threadMonitoring = null;

    DistributionManager distributionManager = cache.getDistributionManager();
    if (distributionManager != null) {
      threadMonitoring = distributionManager.getThreadMonitoring();
    }


    DistributionConfigImpl distributionConfigImpl = new DistributionConfigImpl(nonDefault);
    if (distributionConfigImpl.getThreadMonitorEnabled() && threadMonitoring != null) {

      ((ThreadsMonitoringImpl) threadMonitoring).getTimer().cancel();

      // to initiate ResourceManagerStats
      ((ThreadsMonitoringImpl) threadMonitoring).getThreadsMonitoringProcess().run();

      assertTrue(threadMonitoring instanceof ThreadsMonitoringImpl);
      assertFalse(
          ((ThreadsMonitoringImpl) threadMonitoring).getThreadsMonitoringProcess().mapValidation());
      threadMonitoring.startMonitor(ThreadsMonitoring.Mode.FunctionExecutor);
      assertFalse(
          ((ThreadsMonitoringImpl) threadMonitoring).getThreadsMonitoringProcess().mapValidation());
      AbstractExecutor abstractExecutorGroup = ((ThreadsMonitoringImpl) threadMonitoring)
          .getMonitorMap().get(Thread.currentThread().getId());
      abstractExecutorGroup.setStartTime(abstractExecutorGroup.getStartTime()
          - distributionConfigImpl.getThreadMonitorTimeLimit() - 1);
      assertTrue(
          ((ThreadsMonitoringImpl) threadMonitoring).getThreadsMonitoringProcess().mapValidation());
      assertTrue(((ThreadsMonitoringImpl) threadMonitoring).getThreadsMonitoringProcess()
          .getResourceManagerStats().getNumThreadStuck() == 1);
      ((ThreadsMonitoringImpl) threadMonitoring).getMonitorMap()
          .put(abstractExecutorGroup.getThreadID() + 1, abstractExecutorGroup);
      ((ThreadsMonitoringImpl) threadMonitoring).getMonitorMap()
          .put(abstractExecutorGroup.getThreadID() + 2, abstractExecutorGroup);
      ((ThreadsMonitoringImpl) threadMonitoring).getThreadsMonitoringProcess().mapValidation();
      assertTrue(((ThreadsMonitoringImpl) threadMonitoring).getThreadsMonitoringProcess()
          .getResourceManagerStats().getNumThreadStuck() == 3);
      ((ThreadsMonitoringImpl) threadMonitoring).endMonitor();
      ((ThreadsMonitoringImpl) threadMonitoring).getThreadsMonitoringProcess().mapValidation();
      assertTrue(((ThreadsMonitoringImpl) threadMonitoring).getThreadsMonitoringProcess()
          .getResourceManagerStats().getNumThreadStuck() == 2);

    }
  }
}
