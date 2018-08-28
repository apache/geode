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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;

/**
 *
 * @since Geode 1.5
 */
public class ThreadsMonitoringIntegrationTest {

  private Properties nonDefault;
  private InternalCache cache;

  @Before
  public void setUpThreadsMonitoringIntegrationTest() {
    initInternalDistributedSystem();
  }

  @After
  public void tearDownThreadsMonitoringIntegrationTest() {
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
      assertThat(threadMonitoring).isInstanceOf(ThreadsMonitoringImpl.class);
      ThreadsMonitoringImpl impl = ((ThreadsMonitoringImpl) threadMonitoring);

      impl.getTimer().cancel();

      // to initiate ResourceManagerStats
      impl.getThreadsMonitoringProcess().run();

      assertThat(impl.getThreadsMonitoringProcess().mapValidation())
          .describedAs("ThreadMonitor monitoring process map validation should be false.")
          .isFalse();

      threadMonitoring.startMonitor(ThreadsMonitoring.Mode.FunctionExecutor);

      assertThat(impl.getThreadsMonitoringProcess().mapValidation())
          .describedAs("ThreadMonitor monitoring process map validation should still be false.")
          .isFalse();

      AbstractExecutor abstractExecutorGroup =
          impl.getMonitorMap().get(Thread.currentThread().getId());
      abstractExecutorGroup.setStartTime(abstractExecutorGroup.getStartTime()
          - distributionConfigImpl.getThreadMonitorTimeLimit() - 1);

      assertThat(impl.getThreadsMonitoringProcess().mapValidation())
          .describedAs("ThreadMonitor monitoring process map validation should now be true.")
          .isTrue();
      assertThat((impl.getThreadsMonitoringProcess().getResourceManagerStats().getNumThreadStuck()))
          .describedAs("ThreadMonitor monitoring process should identify one stuck thread.")
          .isEqualTo(1);

      impl.getMonitorMap().put(abstractExecutorGroup.getThreadID() + 1, abstractExecutorGroup);
      impl.getMonitorMap().put(abstractExecutorGroup.getThreadID() + 2, abstractExecutorGroup);
      impl.getThreadsMonitoringProcess().mapValidation();

      assertThat((impl.getThreadsMonitoringProcess().getResourceManagerStats().getNumThreadStuck()))
          .describedAs("ThreadMonitor monitoring process should identify three stuck threads.")
          .isEqualTo(3);

      threadMonitoring.endMonitor();
      impl.getThreadsMonitoringProcess().mapValidation();

      assertThat((impl.getThreadsMonitoringProcess().getResourceManagerStats().getNumThreadStuck()))
          .describedAs("ThreadMonitor monitoring process should identify two stuck threads.")
          .isEqualTo(2);
    }
  }
}
