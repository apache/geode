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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.membership.MembershipEvent;
import org.apache.geode.management.membership.MembershipListener;
import org.apache.geode.management.membership.UniversalMembershipListenerAdapter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class ServerLauncherDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  public static class TestManagementListener extends UniversalMembershipListenerAdapter {

    public static boolean crashed = false;

    @Override
    public void memberCrashed(MembershipEvent event) {
      crashed = true;
    }
  }

  @Test
  public void ensureCleanShutdownFromInProcessServerLauncher() throws Exception {
    MemberVM locator = cluster.startLocatorVM(0);

    // Start a server who will be a lead and thus have a weight of 15. If we don't do this and the
    // test fails with just a single server crashing, the locator will declare a split-brain and
    // shut itself down.
    cluster.startServerVM(1, locator.getPort());

    locator.invoke(() -> {
      MembershipListener listener = new TestManagementListener();
      Cache cache = ClusterStartupRule.getCache();
      ManagementService managementService = ManagementService.getExistingManagementService(cache);
      managementService.addMembershipListener(listener);
    });

    launchServer(locator.getPort());

    Thread.sleep(5000);

    assertThat(locator.invoke(() -> TestManagementListener.crashed)).isFalse();
  }

  private void launchServer(int port) throws Exception {
    Path javaBin = Paths.get(System.getProperty("java.home"), "bin", "java");

    String serverLauncherClass = ServerLauncherDUnitTestHelper.class.getName();
    logger.info("Running java class " + serverLauncherClass);

    ProcessBuilder pBuilder = new ProcessBuilder();
    pBuilder.directory(tempDir.newFolder());
    pBuilder.command(javaBin.toString(), "-classpath", System.getProperty("java.class.path"),
        serverLauncherClass, port + "");

    pBuilder.redirectErrorStream(true);
    Process process = pBuilder.start();

    ByteArrayOutputStream result = new ByteArrayOutputStream();
    BufferedInputStream bais = new BufferedInputStream(process.getInputStream());

    byte[] buffer = new byte[4096];
    int n;
    while ((n = bais.read(buffer)) > 0) {
      result.write(buffer, 0, n);
    }

    if (process.waitFor() != 0) {
      logger.error(result.toString());
    }
  }
}
