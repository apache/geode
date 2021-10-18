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
import static org.mockito.Mockito.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Java6Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class QueryWithRangeIndexDUnitTest implements Serializable {

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private String locatorName, serverName;

  private File locatorDir, serverDir;

  private int locatorPort, locatorJmxPort, serverPort;

  private String locators;

  private static final LocatorLauncher DUMMY_LOCATOR = mock(LocatorLauncher.class);

  private static final AtomicReference<LocatorLauncher> LOCATOR =
      new AtomicReference<>(DUMMY_LOCATOR);

  private static final ServerLauncher DUMMY_SERVER = mock(ServerLauncher.class);

  private static final AtomicReference<ServerLauncher> SERVER =
      new AtomicReference<>(DUMMY_SERVER);

  private VM locator, server;

  private String regionName;

  @Before
  public void setUp() throws Exception {
    locator = getVM(0);
    server = getVM(1);

    locatorName = "locator";
    serverName = "server";
    regionName = "exampleRegion";

    locatorDir = temporaryFolder.newFolder(locatorName);
    serverDir = temporaryFolder.newFolder(serverName);

    int[] port = getRandomAvailableTCPPorts(3);
    locatorPort = port[0];
    locatorJmxPort = port[1];
    serverPort = port[2];

    locators = "localhost[" + locatorPort + "]";

    locator.invoke(() -> startLocator(locatorName, locatorDir, locatorPort, locatorJmxPort));

    gfsh.connectAndVerify(locatorJmxPort, GfshCommandRule.PortType.jmxManager);

    server.invoke(() -> startServer(serverName, serverDir, serverPort, locators));
  }

  @Test
  public void testQueryWithWildcardAndIndexOnAttributeFromHashMap() {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=PARTITION")
        .statusIsSuccess();

    server.invoke(() -> {
      QueryService cacheQS = GemFireCacheImpl.getInstance().getQueryService();
      cacheQS.createIndex("IdIndex", "value.positions['SUN']",
          SEPARATOR + regionName + ".entrySet");
      Region<Integer, Portfolio> region =
          GemFireCacheImpl.getInstance().getRegion(regionName);
      FunctionService.onRegion(region).execute(new MyFunction());
      await().untilAsserted(() -> assertThat(region.size()).isEqualTo(10000));
    });

    String query = "query --query=\"<trace> select e.key, e.value from " +
        SEPARATOR + regionName + ".entrySet e where e.value.positions['SUN'] like 'somethin%'\"";

    String cmdResult = String.valueOf(gfsh.executeAndAssertThat(query).getResultModel());
    assertThat(cmdResult).contains("\"Rows\":\"1\"");
    assertThat(cmdResult).contains("indexesUsed(1):IdIndex(Results: 10000)");
  }

  private static void startLocator(String name, File workingDirectory, int locatorPort,
      int jmxPort) {
    LOCATOR.set(new LocatorLauncher.Builder()
        .setMemberName(name)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .build());

    LOCATOR.get().start();

    await().untilAsserted(() -> {
      InternalLocator locator = (InternalLocator) LOCATOR.get().getLocator();
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });
  }

  private static void startServer(String name, File workingDirectory, int serverPort,
      String locators) {
    System.setProperty(GEMFIRE_PREFIX + "Query.INDEX_THRESHOLD_SIZE", "10000");
    SERVER.set(new ServerLauncher.Builder()
        .setDeletePidFileOnStop(Boolean.TRUE)
        .setMemberName(name)
        .setServerPort(serverPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, locators)
        .build());

    SERVER.get().start();
  }

  public static class MyFunction implements Function, DataSerializable {
    @Override
    public void execute(FunctionContext context) {
      Java6Assertions.assertThat(context).isInstanceOf(RegionFunctionContext.class);
      PartitionedRegion region = (PartitionedRegion) ((RegionFunctionContext) context).getDataSet();
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
    }

    @Override
    public void fromData(DataInput in) {}

    @Override
    public void toData(DataOutput out) {}
  }
}
