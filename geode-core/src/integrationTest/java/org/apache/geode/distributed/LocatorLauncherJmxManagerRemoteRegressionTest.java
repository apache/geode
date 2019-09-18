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

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.net.AvailablePortHelper;

/**
 * Regression tests for stopping a JMX Manager process launched with {@code LocatorLauncher}.
 *
 * <p>
 * Confirms fix for <bold>Locator not stopping correctly if jmx-manager-port=0</bold>
 *
 * <p>
 * Refactored from LocatorLauncherAssemblyIntegrationTest which used to be in geode-assembly.
 */
public class LocatorLauncherJmxManagerRemoteRegressionTest
    extends LocatorLauncherRemoteIntegrationTestCase {

  private final AvailablePortHelper availablePortHelper = AvailablePortHelper.create();

  private int jmxManagerPort;

  @Before
  public void before() throws Exception {
    int[] ports = availablePortHelper.getRandomAvailableTCPPorts(3);
    defaultLocatorPort = ports[0];
    nonDefaultLocatorPort = ports[1];
    jmxManagerPort = ports[2];
  }

  @Test
  public void locatorProcessWithZeroJmxPortExitsWhenStopped() {
    givenRunningLocator(
        addJvmArgument("-D" + JMX_MANAGER + "=true")
            .addJvmArgument("-D" + JMX_MANAGER_START + "=true")
            .addJvmArgument("-D" + JMX_MANAGER_PORT + "=0"));

    new LocatorLauncher.Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .stop();

    assertStopOf(getLocatorProcess());
  }

  @Test
  public void locatorProcessWithNonZeroJmxPortExitsWhenStopped() {
    givenRunningLocator(
        addJvmArgument("-D" + JMX_MANAGER + "=true")
            .addJvmArgument("-D" + JMX_MANAGER_START + "=true")
            .addJvmArgument("-D" + JMX_MANAGER_PORT + "=" + jmxManagerPort));

    new LocatorLauncher.Builder()
        .setWorkingDirectory(getWorkingDirectoryPath())
        .build()
        .stop();

    assertStopOf(getLocatorProcess());
  }
}
