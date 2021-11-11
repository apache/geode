/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.rest;

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@SuppressWarnings("serial")
public class ManagementRequestLoggingDistributedTest implements Serializable {

  private static LocatorLauncher locatorLauncher;
  private static ServerLauncher serverLauncher;

  private VM locatorVM;
  private VM serverVM;

  private String locatorName;
  private String serverName;
  private File locatorDir;
  private File serverDir;
  private int httpPort;
  private int jmxManagerPort;
  private int locatorPort;

  private transient ClusterManagementService service;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() throws Exception {
    locatorVM = getVM(0);
    serverVM = getVM(1);

    locatorName = "locator1";
    serverName = "server1";
    locatorDir = temporaryFolder.newFolder(locatorName);
    serverDir = temporaryFolder.newFolder(serverName);
    int[] ports = getRandomAvailableTCPPorts(2);
    httpPort = ports[0];
    jmxManagerPort = ports[1];

    locatorPort = locatorVM.invoke(this::startLocator);
    serverVM.invoke(this::startServer);

    service = new ClusterManagementServiceBuilder()
        .setPort(httpPort)
        .build();
  }

  @After
  public void tearDown() {
    locatorVM.invoke(() -> {
      if (locatorLauncher != null) {
        locatorLauncher.stop();
        locatorLauncher = null;
      }
    });

    serverVM.invoke(() -> {
      if (serverLauncher != null) {
        serverLauncher.stop();
        serverLauncher = null;
      }
    });
  }

  @Test
  public void checkRequestsAreLogged() {
    Region regionConfig = new Region();
    regionConfig.setName("customers");
    regionConfig.setType(RegionType.REPLICATE);

    ClusterManagementResult result = service.create(regionConfig);

    assertThat(result.isSuccessful()).isTrue();

    locatorVM.invoke(() -> {
      Path logFile = locatorDir.toPath().resolve(locatorName + ".log");
      String logContents = readFileToString(logFile.toFile(), defaultCharset());

      // Note: the following is kind of fragile. Remove experimental when it goes away.
      // Also feel free to change the following to use regex or substrings

      assertThat(logContents)
          .containsSubsequence(
              "Management Request:",
              " POST[url=/management/v1/regions];",
              " user=null;",
              " payload={")
          .containsSubsequence(
              "Management Response:",
              " Status=201;",
              " response={");
    });
  }

  private int startLocator() {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder();
    builder.setMemberName(locatorName);
    builder.setWorkingDirectory(locatorDir.getAbsolutePath());
    builder.setPort(0);
    builder.set(HTTP_SERVICE_PORT, String.valueOf(httpPort));
    builder.set(JMX_MANAGER_PORT, String.valueOf(jmxManagerPort));
    builder.set(LOG_LEVEL, "debug");

    locatorLauncher = builder.build();
    locatorLauncher.start();

    return locatorLauncher.getPort();
  }

  private void startServer() {
    ServerLauncher.Builder builder = new ServerLauncher.Builder();
    builder.setMemberName(serverName);
    builder.setWorkingDirectory(serverDir.getAbsolutePath());
    builder.setServerPort(0);
    builder.set(LOCATORS, "localHost[" + locatorPort + "]");

    serverLauncher = builder.build();
    serverLauncher.start();
  }
}
