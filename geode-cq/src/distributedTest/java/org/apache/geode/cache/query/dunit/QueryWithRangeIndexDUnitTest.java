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

package org.apache.geode.cache.query.dunit;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEMFIRE_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@SuppressWarnings("ALL")
public class QueryWithRangeIndexDUnitTest implements Serializable {

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private static final String locatorName = "locator";
  private static final String serverName = "server";

  private File locatorDir;
  private File serverDir;

  private int locatorPort;
  private int locatorJmxPort;
  private int serverPort;

  private String locators;

  private VM server;

  private static final String regionName = "exampleRegion";

  @Before
  public void setUp() throws Exception {
    VM locator = getVM(0);
    server = getVM(1);

    locatorDir = temporaryFolder.newFolder(locatorName);
    serverDir = temporaryFolder.newFolder(serverName);

    int[] port = getRandomAvailableTCPPorts(3);
    locatorPort = port[0];
    locatorJmxPort = port[1];
    serverPort = port[2];

    locators = "localhost[" + locatorPort + "]";

    locator.invoke(() -> startLocator(locatorDir, locatorPort, locatorJmxPort));

    gfsh.connectAndVerify(locatorJmxPort, GfshCommandRule.PortType.jmxManager);

    server.invoke(() -> startServer(serverDir, serverPort, locators));
  }

  @Test
  public void testQueryWithWildcardAndIndexOnAttributeFromHashMap() {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=PARTITION")
        .statusIsSuccess();

    server.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      QueryService cacheQS = cache.getQueryService();
      cacheQS.createIndex("IdIndex", "value.positions['SUN']",
          SEPARATOR + regionName + ".entrySet");
      Region<Integer, Portfolio> region =
          cache.getRegion(regionName);

      for (int i = 1; i < 10001; i++) {
        Portfolio p1 = new Portfolio(i, i);
        p1.positions = new HashMap<>();
        p1.positions.put("IBM", "something");
        if (i == 1) {
          p1.positions.put("SUN", "something");
        } else {
          p1.positions.put("SUN", "some");
        }
        region.put(i, p1);
      }
    });

    String query = "query --query=\"<trace> select e.key, e.value from " +
        SEPARATOR + regionName + ".entrySet e where e.value.positions['SUN'] like 'somethin%'\"";

    String cmdResult = String.valueOf(gfsh.executeAndAssertThat(query).getResultModel());
    assertThat(cmdResult).contains("\"Rows\":\"1\"");
    assertThat(cmdResult).contains("indexesUsed(1):IdIndex(Results: 10000)");
  }

  private static void startLocator(File workingDirectory, int locatorPort,
      int jmxPort) {
    LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
        .setMemberName(locatorName)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .build();

    locatorLauncher.start();

    await().untilAsserted(() -> {
      InternalLocator locator = (InternalLocator) locatorLauncher.getLocator();
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });
  }

  private static void startServer(File workingDirectory, int serverPort,
      String locators) {
    System.setProperty(GEMFIRE_PREFIX + "Query.INDEX_THRESHOLD_SIZE", "10000");
    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setDeletePidFileOnStop(Boolean.TRUE)
        .setMemberName(serverName)
        .setServerPort(serverPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, locators)
        .build();

    serverLauncher.start();
  }
}
