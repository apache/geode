/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagerMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

import static org.junit.Assert.*;

/**
 * These tests are part of assembly as they require the REST war file to be present.
 *
 */
@Category(IntegrationTest.class)
public class LocatorLauncherAssemblyJUnitTest extends AbstractLocatorLauncherJUnitTestCase {

  @Before
  public final void setUpLocatorLauncherLocalTest() throws Exception {
    disconnectFromDS();
    System.setProperty(ProcessType.TEST_PREFIX_PROPERTY, getUniqueName() + "-");
  }

  @After
  public final void tearDownLocatorLauncherLocalTest() throws Exception {
    disconnectFromDS();
  }

  /*
   * This test addresses GEODE-528
   */
  @Test
  public void testLocatorStopsWhenJmxPortIsZero() throws Throwable {
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath();

    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(false)
        .setWorkingDirectory(rootFolder)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false")
        .set(DistributionConfig.JMX_MANAGER_NAME, "true")
        .set(DistributionConfig.JMX_MANAGER_START_NAME, "true")
        .set(DistributionConfig.JMX_MANAGER_PORT_NAME, "0");

    performTest(builder);
  }

  /*
   * This test addresses GEODE-528
   */
  @Test
  public void testLocatorStopsWhenJmxPortIsNonZero() throws Throwable {
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath();
    final int jmxPort = AvailablePortHelper.getRandomAvailableTCPPorts(1)[0];

    final Builder builder = new Builder().setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(false)
        .setWorkingDirectory(rootFolder)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false")
        .set(DistributionConfig.JMX_MANAGER_NAME, "true")
        .set(DistributionConfig.JMX_MANAGER_START_NAME, "true")
        .set(DistributionConfig.JMX_MANAGER_PORT_NAME, Integer.toString(jmxPort));

    performTest(builder);
  }

  private void performTest(Builder builder) {
    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    LocatorLauncher dirLauncher = null;
    int initialThreadCount = Thread.activeCount();

    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue("Pid file " + this.pidFile.getCanonicalPath().toString() + " should exist", this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);

      dirLauncher = new Builder().setWorkingDirectory(builder.getWorkingDirectory()).build();
      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());

      // Stop the manager
      Cache cache = CacheFactory.getAnyInstance();
      ManagerMXBean managerBean = ManagementService.getManagementService(cache).getManagerMXBean();
      managerBean.stop();

      // stop the locator
      final LocatorLauncher.LocatorState locatorState = dirLauncher.stop();
      assertNotNull(locatorState);
      assertEquals(Status.STOPPED, locatorState.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      // verify the PID file was deleted
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    int finalThreadCount = Integer.MAX_VALUE;

    // Spin for up to 5 seconds waiting for threads to finish
    for (int i = 0; i < 50 && finalThreadCount > initialThreadCount; i++) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        // ignored
      }
      finalThreadCount = Thread.activeCount();
    }

    assertEquals(initialThreadCount, finalThreadCount);
  }
}
