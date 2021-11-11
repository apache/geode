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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;


@Category({GfshTest.class})
public class ShutdownCommandDUnitTest implements
    Serializable {

  private static final String LOCATOR_NAME = "locator";
  private static final String SERVER1_NAME = "server1";
  private static final String SERVER2_NAME = "server2";
  private static final AtomicReference<LocatorLauncher> LOCATOR_LAUNCHER = new AtomicReference<>();
  private static final AtomicReference<ServerLauncher> SERVER_LAUNCHER = new AtomicReference<>();

  private VM locator;
  private VM server1;
  private VM server2;

  private String locatorString;
  private int locatorPort;
  private int locatorJmxPort;
  private int locatorHttpPort;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();


  @Before
  public void setup() throws Exception {
    locator = getVM(0);
    server1 = getVM(1);
    server2 = getVM(2);

    File locatorDir = temporaryFolder.newFolder(LOCATOR_NAME);
    File server1Dir = temporaryFolder.newFolder(SERVER1_NAME);
    File server2Dir = temporaryFolder.newFolder(SERVER2_NAME);

    int[] ports = getRandomAvailableTCPPorts(3);
    locatorPort = ports[0];
    locatorJmxPort = ports[1];
    locatorHttpPort = ports[2];

    locatorString = "localhost[" + locatorPort + "]";

    locator.invoke(
        () -> startLocator(locatorDir, locatorPort, locatorJmxPort, locatorHttpPort));
    server1.invoke(() -> startServer(SERVER1_NAME, server1Dir, locatorString));
    server2.invoke(() -> startServer(SERVER2_NAME, server2Dir, locatorString));

    gfsh.connectAndVerify(locatorJmxPort, PortType.jmxManager);
  }

  @Test
  public void testShutdownServers() {
    String command = "shutdown";

    gfsh.executeAndAssertThat(command).statusIsSuccess().containsOutput("Shutdown is triggered");

    for (VM vm : toArray(server1, server2)) {
      vm.invoke(() -> verifyNotConnected(SERVER_LAUNCHER.get().getCache()));
    }

    gfsh.executeAndAssertThat("list members").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("locator");
  }

  @Test
  public void testShutdownAll() {
    String command = "shutdown --include-locators=true";

    gfsh.executeAndAssertThat(command).statusIsSuccess().containsOutput("Shutdown is triggered");
    server1.invoke(() -> verifyNotConnected(SERVER_LAUNCHER.get().getCache()));
    server2.invoke(() -> verifyNotConnected(SERVER_LAUNCHER.get().getCache()));
    locator.invoke(() -> verifyNotConnected(LOCATOR_LAUNCHER.get().getCache()));
  }

  private void verifyNotConnected(Cache cache) {
    await().untilAsserted(() -> assertThat(cache.getDistributedSystem().isConnected()).isFalse());
  }

  private void startLocator(File directory, int port, int jmxPort, int httpPort) {
    LOCATOR_LAUNCHER.set(new LocatorLauncher.Builder()
        .setMemberName(LOCATOR_NAME)
        .setPort(port)
        .setWorkingDirectory(directory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, httpPort + "")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(directory, LOCATOR_NAME + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build());

    LOCATOR_LAUNCHER.get().start();

    await().untilAsserted(() -> {
      InternalLocator locator = (InternalLocator) LOCATOR_LAUNCHER.get().getLocator();
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });
  }

  private void startServer(String name, File workingDirectory, String locator) {
    SERVER_LAUNCHER.set(new ServerLauncher.Builder()
        .setDisableDefaultServer(true)
        .setMemberName(name)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, locator)
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build());

    SERVER_LAUNCHER.get().start();
  }
}
