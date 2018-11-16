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

import static org.apache.geode.distributed.AbstractLauncher.Status.STOPPED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_CONFIGURATION_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.distributed.internal.InternalConfigurationPersistenceService.CLUSTER_CONFIG_DISK_DIR_PREFIX;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Abstract base class for integration tests of {@link LocatorLauncher}.
 *
 * @since GemFire 8.0
 */
public abstract class LocatorLauncherIntegrationTestCase extends LauncherIntegrationTestCase {

  protected volatile int defaultLocatorPort;
  protected volatile int nonDefaultLocatorPort;
  protected volatile LocatorLauncher launcher;

  private volatile File clusterConfigDirectory;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Before
  public void setUpAbstractLocatorLauncherIntegrationTestCase() throws Exception {
    System.setProperty(GEMFIRE_PREFIX + MCAST_PORT, Integer.toString(0));

    clusterConfigDirectory =
        temporaryFolder.newFolder(CLUSTER_CONFIG_DISK_DIR_PREFIX + getUniqueName());

    int[] ports = getRandomAvailableTCPPorts(2);
    defaultLocatorPort = ports[0];
    nonDefaultLocatorPort = ports[1];
    System.setProperty(TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, String.valueOf(defaultLocatorPort));
  }

  @After
  public void tearDownAbstractLocatorLauncherIntegrationTestCase() throws Exception {
    if (launcher != null) {
      launcher.stop();
    }
  }

  @Override
  protected ProcessType getProcessType() {
    return ProcessType.LOCATOR;
  }

  @Override
  protected void givenEmptyWorkingDirectory() {
    File[] files = getWorkingDirectory().listFiles();
    assertThat(files).hasSize(1);
    assertThat(files[0]).isDirectory().isEqualTo(getClusterConfigDirectory());
  }

  protected LocatorLauncher givenLocatorLauncher() {
    return givenLocatorLauncher(newBuilder());
  }

  private LocatorLauncher givenLocatorLauncher(final Builder builder) {
    return builder.build();
  }

  protected LocatorLauncher givenRunningLocator() {
    return givenRunningLocator(newBuilder());
  }

  protected LocatorLauncher givenRunningLocator(final Builder builder) {
    return awaitStart(builder);
  }

  protected LocatorLauncher awaitStart(final LocatorLauncher launcher) {
    GeodeAwaitility.await().untilAsserted(() -> assertThat(isLauncherOnline()).isTrue());
    return launcher;
  }

  protected Locator getLocator() {
    return launcher.getLocator();
  }

  /**
   * Returns a new Builder with helpful defaults for safe testing. If you need a Builder in a test
   * without any of these defaults then simply use {@code new Builder()} instead.
   */
  protected Builder newBuilder() {
    return new Builder().setMemberName(getUniqueName()).setRedirectOutput(true)
        .setWorkingDirectory(getWorkingDirectoryPath())
        .set(CLUSTER_CONFIGURATION_DIR, getClusterConfigDirectoryPath())
        .set(DISABLE_AUTO_RECONNECT, "true").set(LOG_LEVEL, "config").set(MCAST_PORT, "0");
  }

  protected LocatorLauncher startLocator() {
    return awaitStart(newBuilder());
  }

  protected LocatorLauncher startLocator(final Builder builder) {
    return awaitStart(builder);
  }

  protected void stopLocator() {
    assertThat(launcher.stop().getStatus()).isEqualTo(STOPPED);
  }

  private LocatorLauncher awaitStart(final Builder builder) {
    launcher = builder.build();
    assertThat(launcher.start().getStatus()).isEqualTo(Status.ONLINE);
    return awaitStart(launcher);
  }

  private File getClusterConfigDirectory() {
    return clusterConfigDirectory;
  }

  private String getClusterConfigDirectoryPath() {
    try {
      return clusterConfigDirectory.getCanonicalPath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean isLauncherOnline() {
    LocatorState locatorState = launcher.status();
    assertNotNull(locatorState);
    return Status.ONLINE.equals(locatorState.getStatus());
  }
}
