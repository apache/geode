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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Regression tests for stopping a JMX Manager process launched with {@link LocatorLauncher}.
 *
 * <p>
 * Confirms fix for <bold>GEODE-528: Locator not stopping correctly if jmx-manager-port=0</bold>
 *
 * <p>
 * Refactored from LocatorLauncherAssemblyIntegrationTest which used to be in geode-assembly.
 */
public class LocatorLauncherJmxManagerLocalRegressionTest
    extends LocatorLauncherIntegrationTestCase {

  /**
   * Using Awaitility will increase total thread count by 1: ConditionAwaiter$ConditionPoller.
   */
  private static final int AWAITILITY_USAGE_THREAD_COUNT = 1;

  private Set<Thread> initialThreads;
  private int jmxManagerPort;

  @Before
  public void setUpLocatorLauncherJmxManagerLocalIntegrationTest() throws Exception {
    disconnectFromDS();
    System.setProperty(ProcessType.PROPERTY_TEST_PREFIX, getUniqueName() + "-");

    int[] ports = getRandomAvailableTCPPorts(3);
    this.defaultLocatorPort = ports[0];
    this.nonDefaultLocatorPort = ports[1];
    this.jmxManagerPort = ports[2];

    this.initialThreads = Thread.getAllStackTraces().keySet();
  }

  @Test
  public void locatorWithZeroJmxPortCleansUpWhenStopped() throws Exception {
    startLocator(newBuilder().setDeletePidFileOnStop(true).setMemberName(getUniqueName())
        .setPort(this.defaultLocatorPort).setRedirectOutput(false)
        .setWorkingDirectory(getWorkingDirectoryPath()).set(LOG_LEVEL, "config")
        .set(ENABLE_CLUSTER_CONFIGURATION, "false").set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_START, "true").set(JMX_MANAGER_PORT, "0"));

    stopLocator();

    assertDeletionOf(getPidFile());
    assertThatThreadsStopped();
  }

  @Test
  public void locatorWithNonZeroJmxPortCleansUpWhenStopped() throws Exception {
    startLocator(newBuilder().setDeletePidFileOnStop(true).setMemberName(getUniqueName())
        .setPort(this.defaultLocatorPort).setRedirectOutput(false)
        .setWorkingDirectory(getWorkingDirectoryPath()).set(LOG_LEVEL, "config")
        .set(ENABLE_CLUSTER_CONFIGURATION, "false").set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_START, "true").set(JMX_MANAGER_PORT, String.valueOf(jmxManagerPort)));

    stopLocator();

    assertDeletionOf(getPidFile());
    assertThatThreadsStopped();
  }

  private void assertThatThreadsStopped() {
    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(currentThreadCount())
            .isLessThanOrEqualTo(initialThreadCountPlusAwaitility()));
  }

  private int currentThreadCount() {
    return Thread.getAllStackTraces().keySet().size();
  }

  private int initialThreadCountPlusAwaitility() {
    return initialThreads.size() + AWAITILITY_USAGE_THREAD_COUNT;
  }
}
