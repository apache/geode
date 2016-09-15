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
package org.apache.geode.distributed;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagerMXBean;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

import static org.junit.Assert.*;

import static org.apache.geode.distributed.ConfigurationProperties.*;

/**
 * These tests are part of assembly as they require the REST war file to be present.
 */
@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class LocatorLauncherAssemblyIntegrationTest extends AbstractLocatorLauncherIntegrationTestCase {

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
        .set(LOG_LEVEL, "config")
        .set(ENABLE_CLUSTER_CONFIGURATION, "false")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_START, "true")
        .set(JMX_MANAGER_PORT, "0");

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
        .set(LOG_LEVEL, "config")
        .set(ENABLE_CLUSTER_CONFIGURATION, "false")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_START, "true")
        .set(JMX_MANAGER_PORT, Integer.toString(jmxPort));

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
