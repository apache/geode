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
package org.apache.geode.distributed.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.ThreadMonitoring;
import org.apache.geode.internal.statistics.AbstractExecutorGroup;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 *
 * @since Geode 1.5
 */
@Category(IntegrationTest.class)
public class ThreadMonitoringIntegrationTest {

  private Properties nonDefault;
  private Cache cache;

  @Before
  public void setUpThreadMonitoringIntegrationTest() throws Exception {
    initInternalDistributedSystem();
  }

  @After
  public void tearDownThreadMonitoringIntegrationTest() throws Exception {
    stopInternalDistributedSystem();
  }

  private void stopInternalDistributedSystem() {
    cache.close();
  }

  private void initInternalDistributedSystem() {
    nonDefault = new Properties();
    nonDefault.put(ConfigurationProperties.MCAST_PORT, "0");
    nonDefault.put(ConfigurationProperties.LOCATORS, "");

    cache = new CacheFactory(nonDefault).create();
  }

  /**
   * Tests that in case no instance of internal distribution system exists dummy instance is used
   */
  @Test
  public void testThreadMonitoringWorkflow() {

    ThreadMonitoring threadMonitoring = null;
    InternalDistributedSystem internalDistributedSystem =
        InternalDistributedSystem.getAnyInstance();
    if (internalDistributedSystem != null) {
      DistributionManager distributionManager = internalDistributedSystem.getDistributionManager();
      if (distributionManager != null) {
        threadMonitoring = distributionManager.getThreadMonitoring();
      }
    }

    DistributionConfigImpl distributionConfigImpl = new DistributionConfigImpl(nonDefault);
    if (distributionConfigImpl.getThreadMonitorEnabled() && threadMonitoring != null) {

      ((ThreadMonitoringImpl) threadMonitoring).getTimer().cancel();

      // to initiate ResourceManagerStats
      ((ThreadMonitoringImpl) threadMonitoring).getThreadMonitoringProcess().run();

      assertTrue(threadMonitoring instanceof ThreadMonitoringImpl);
      assertFalse(
          ((ThreadMonitoringImpl) threadMonitoring).getThreadMonitoringProcess().mapValidation());
      threadMonitoring.startMonitor(ThreadMonitoring.Mode.FunctionExecutor);
      assertFalse(
          ((ThreadMonitoringImpl) threadMonitoring).getThreadMonitoringProcess().mapValidation());
      AbstractExecutorGroup abstractExecutorGroup = ((ThreadMonitoringImpl) threadMonitoring)
          .getMonitorMap().get(Thread.currentThread().getId());
      abstractExecutorGroup.setStartTime(abstractExecutorGroup.getStartTime()
          - distributionConfigImpl.getThreadMonitorTimeLimit() - 1);
      assertTrue(
          ((ThreadMonitoringImpl) threadMonitoring).getThreadMonitoringProcess().mapValidation());
      assertTrue(((ThreadMonitoringImpl) threadMonitoring).getThreadMonitoringProcess()
          .getResourceManagerStats().getIsThreadStuck() == 1);
      ((ThreadMonitoringImpl) threadMonitoring).getMonitorMap()
          .put(abstractExecutorGroup.getThreadID() + 1, abstractExecutorGroup);
      ((ThreadMonitoringImpl) threadMonitoring).getMonitorMap()
          .put(abstractExecutorGroup.getThreadID() + 2, abstractExecutorGroup);
      ((ThreadMonitoringImpl) threadMonitoring).getThreadMonitoringProcess().mapValidation();
      assertTrue(((ThreadMonitoringImpl) threadMonitoring).getThreadMonitoringProcess()
          .getResourceManagerStats().getIsThreadStuck() == 3);
      ((ThreadMonitoringImpl) threadMonitoring).endMonitor();
      ((ThreadMonitoringImpl) threadMonitoring).getThreadMonitoringProcess().mapValidation();
      assertTrue(((ThreadMonitoringImpl) threadMonitoring).getThreadMonitoringProcess()
          .getResourceManagerStats().getIsThreadStuck() == 2);

    }
  }
}
