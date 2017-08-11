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
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Regression tests for stopping a JMX Manager process launched with {@link LocatorLauncher}.
 *
 * <p>
 * Confirms fix for <bold>GEODE-528: Locator not stopping correctly if jmx-manager-port=0</bold>
 *
 * <p>
 * Refactored from LocatorLauncherAssemblyIntegrationTest which used to be in geode-assembly.
 */
@Category(IntegrationTest.class)
public class LocatorLauncherJmxManagerRemoteRegressionTest
    extends LocatorLauncherRemoteIntegrationTestCase {

  private int jmxManagerPort;

  @Before
  public void before() throws Exception {
    int[] ports = getRandomAvailableTCPPorts(3);
    this.defaultLocatorPort = ports[0];
    this.nonDefaultLocatorPort = ports[1];
    this.jmxManagerPort = ports[2];
  }

  @Test
  public void locatorProcessWithZeroJmxPortExitsWhenStopped() throws Exception {
    givenRunningLocator(addJvmArgument("-D" + JMX_MANAGER + "=true")
        .addJvmArgument("-D" + JMX_MANAGER_START + "=true")
        .addJvmArgument("-D" + JMX_MANAGER_PORT + "=0"));

    new LocatorLauncher.Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertStopOf(getLocatorProcess());
  }

  @Test
  public void locatorProcessWithNonZeroJmxPortExitsWhenStopped() throws Exception {
    givenRunningLocator(addJvmArgument("-D" + JMX_MANAGER + "=true")
        .addJvmArgument("-D" + JMX_MANAGER_START + "=true")
        .addJvmArgument("-D" + JMX_MANAGER_PORT + "=" + jmxManagerPort));

    new LocatorLauncher.Builder().setWorkingDirectory(getWorkingDirectoryPath()).build().stop();

    assertStopOf(getLocatorProcess());
  }
}
