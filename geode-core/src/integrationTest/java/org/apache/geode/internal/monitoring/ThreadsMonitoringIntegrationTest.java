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
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;

/**
 *
 * @since Geode 1.5
 */
public class ThreadsMonitoringIntegrationTest {

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
    Properties nonDefault = new Properties();
    nonDefault.put(ConfigurationProperties.MCAST_PORT, "0");
    nonDefault.put(ConfigurationProperties.LOCATORS, "");
    nonDefault.put(ConfigurationProperties.THREAD_MONITOR_ENABLED, "true");
    nonDefault.put(ConfigurationProperties.THREAD_MONITOR_TIME_LIMIT, "30000");

    cache = (InternalCache) new CacheFactory(nonDefault).create();
  }

  @Test
  public void testThreadsMonitoringWorkflow() {

    DistributionManager distributionManager = cache.getDistributionManager();
    assertThat(distributionManager).isNotNull();
    ThreadsMonitoring threadMonitoring = distributionManager.getThreadMonitoring();
    assertThat(threadMonitoring).isNotNull();

    assertThat(threadMonitoring).isInstanceOf(ThreadsMonitoringImpl.class);
    ThreadsMonitoringImpl impl = ((ThreadsMonitoringImpl) threadMonitoring);

    impl.getTimer().cancel();

    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should be false.")
        .isFalse();

    threadMonitoring.startMonitor(ThreadsMonitoring.Mode.FunctionExecutor);

    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should still be false.")
        .isFalse();

    AbstractExecutor abstractExecutor =
        impl.getMonitorMap().get(Thread.currentThread().getId());
    abstractExecutor.setStartTime(abstractExecutor.getStartTime()
        - cache.getInternalDistributedSystem().getConfig().getThreadMonitorTimeLimit() - 1);

    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should now be true.")
        .isTrue();
    assertThat((impl.getThreadsMonitoringProcess().getResourceManagerStats().getNumThreadStuck()))
        .describedAs("ThreadMonitor monitoring process should identify one stuck thread.")
        .isEqualTo(1);

    threadMonitoring.endMonitor();

    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should now be false.")
        .isFalse();

    assertThat((impl.getThreadsMonitoringProcess().getResourceManagerStats().getNumThreadStuck()))
        .describedAs("ThreadMonitor monitoring process should identify no stuck threads.")
        .isEqualTo(0);
  }

  @Test
  public void verifySuspendResumeFunctionCorrectly() {

    DistributionManager distributionManager = cache.getDistributionManager();
    assertThat(distributionManager).isNotNull();
    ThreadsMonitoring threadMonitoring = distributionManager.getThreadMonitoring();
    assertThat(threadMonitoring).isNotNull();
    final int monitorTimeLimit =
        cache.getInternalDistributedSystem().getConfig().getThreadMonitorTimeLimit();

    assertThat(threadMonitoring).isInstanceOf(ThreadsMonitoringImpl.class);
    ThreadsMonitoringImpl impl = ((ThreadsMonitoringImpl) threadMonitoring);

    impl.getTimer().cancel();

    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should be false.")
        .isFalse();

    AbstractExecutor executor =
        threadMonitoring.createAbstractExecutor(ThreadsMonitoring.Mode.P2PReaderExecutor);

    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should still be false.")
        .isFalse();

    threadMonitoring.register(executor);
    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should still be false.")
        .isFalse();
    assertThat(executor.getStartTime()).isNotZero();

    executor.setStartTime(executor.getStartTime() - monitorTimeLimit - 1);
    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should now be true.")
        .isTrue();
    assertThat((impl.getThreadsMonitoringProcess().getResourceManagerStats().getNumThreadStuck()))
        .describedAs("ThreadMonitor monitoring process should identify one stuck thread.")
        .isEqualTo(1);

    executor.suspendMonitoring();
    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should still be false.")
        .isFalse();

    executor.resumeMonitoring();
    assertThat(executor.getStartTime()).isZero();
    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should still be false.")
        .isFalse();
    assertThat(executor.getStartTime()).isNotZero();

    executor.setStartTime(executor.getStartTime() - monitorTimeLimit - 1);
    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should now be true.")
        .isTrue();
    assertThat((impl.getThreadsMonitoringProcess().getResourceManagerStats().getNumThreadStuck()))
        .describedAs("ThreadMonitor monitoring process should identify one stuck thread.")
        .isEqualTo(1);

    impl.unregister(executor);
    assertThat(impl.getThreadsMonitoringProcess().mapValidation())
        .describedAs("ThreadMonitor monitoring process map validation should still be false.")
        .isFalse();
  }
}
