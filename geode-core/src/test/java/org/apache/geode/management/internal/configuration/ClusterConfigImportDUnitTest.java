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
 *
 */

package org.apache.geode.management.internal.configuration;

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.Server;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Category(DistributedTest.class)
public class ClusterConfigImportDUnitTest extends ClusterConfigBaseTest {
  private GfshShellConnectionRule gfshConnector;

  public static final ClusterConfig INITIAL_CONFIG = new ClusterConfig(new ConfigGroup("cluster"));

  private Locator locator;

  @Before
  public void before() throws Exception {
    super.before();
    locator = lsRule.startLocatorVM(0, locatorProps);
    INITIAL_CONFIG.verify(locator);

    gfshConnector = new GfshShellConnectionRule(locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();
  }


  @After
  public void after() throws Exception {
    if (gfshConnector != null) {
      gfshConnector.close();
    }
  }


  @Test
  public void testImportWithRunningServerWithData() throws Exception {
    Server server = lsRule.startServerVM(1, serverProps, locator.getPort());
    String regionName = "regionA";
    server.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.cache;
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
      region.put("key", "value");
    });

    CommandResult result = gfshConnector
        .executeCommand("import cluster-configuration --zip-file-name=" + clusterConfigZipPath);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("existing data in regions: " + regionName);
  }

  @Test
  public void testImportWithRunningServer() throws Exception {
    Server server1 = lsRule.startServerVM(1, serverProps, locator.getPort());

    // create a testRegion and verify that after import, this region does not exist anymore
    server1.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.cache;
      cache.createRegionFactory(RegionShortcut.REPLICATE).create("testRegion");
    });

    serverProps.setProperty("groups", "group2");
    Server server2 = lsRule.startServerVM(2, serverProps, locator.getPort());

    // even though we have a region recreated, we can still import since there is no data
    // in the region
    CommandResult result = gfshConnector
        .executeCommand("import cluster-configuration --zip-file-name=" + clusterConfigZipPath);

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK)
        .describedAs(result.getContent().toString());
    assertThat(result.getContent().toString())
        .contains("Successfully applied the imported cluster configuration on server-1");
    assertThat(result.getContent().toString())
        .contains("Successfully applied the imported cluster configuration on server-2");
    new ClusterConfig(CLUSTER).verify(server1);
    new ClusterConfig(CLUSTER, GROUP2).verify(server2);

    // verify "testRegion" does not exist in either server anymore
    server1.invoke(() -> {
      Cache cache = GemFireCacheImpl.getInstance();
      assertThat(cache.getRegion("testRegion")).isNull();
    });
    server2.invoke(() -> {
      Cache cache = GemFireCacheImpl.getInstance();
      assertThat(cache.getRegion("testRegion")).isNull();
    });
  }

  @Test
  public void testImportClusterConfig() throws Exception {
    CommandResult result = gfshConnector
        .executeCommand("import cluster-configuration --zip-file-name=" + clusterConfigZipPath);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    // Make sure that a backup of the old clusterConfig was created
    assertThat(locator.getWorkingDir().listFiles())
        .filteredOn((File file) -> file.getName().contains("cluster_config")).hasSize(2);

    CONFIG_FROM_ZIP.verify(locator);

    // start server1 with no group
    Server server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    new ClusterConfig(CLUSTER).verify(server1);

    // start server2 in group1
    serverProps.setProperty(GROUPS, "group1");
    Server server2 = lsRule.startServerVM(2, serverProps, locator.getPort());
    new ClusterConfig(CLUSTER, GROUP1).verify(server2);

    // start server3 in group1 and group2
    serverProps.setProperty(GROUPS, "group1,group2");
    Server server3 = lsRule.startServerVM(3, serverProps, locator.getPort());
    new ClusterConfig(CLUSTER, GROUP1, GROUP2).verify(server3);
  }

  @Test
  public void testImportWithMultipleLocators() throws Exception {
    locatorProps.setProperty(LOCATORS, "localhost[" + locator.getPort() + "]");
    Locator locator1 = lsRule.startLocatorVM(1, locatorProps);

    locatorProps.setProperty(LOCATORS,
        "localhost[" + locator.getPort() + "],localhost[" + locator1.getPort() + "]");
    Locator locator2 = lsRule.startLocatorVM(2, locatorProps);

    CommandResult result = gfshConnector
        .executeCommand("import cluster-configuration --zip-file-name=" + clusterConfigZipPath);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    CONFIG_FROM_ZIP.verify(locator);
    REPLICATED_CONFIG_FROM_ZIP.verify(locator1);
    REPLICATED_CONFIG_FROM_ZIP.verify(locator2);
  }

  @Test
  public void testExportClusterConfig() throws Exception {
    Server server1 = lsRule.startServerVM(1, serverProps, locator.getPort());

    CommandResult result =
        gfshConnector.executeCommand("create region --name=myRegion --type=REPLICATE");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    ConfigGroup cluster = new ConfigGroup("cluster").regions("myRegion");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(locator);

    Path exportedZipPath = lsRule.getTempFolder().getRoot().toPath().resolve("exportedCC.zip");
    result = gfshConnector
        .executeCommand("export cluster-configuration --zip-file-name=" + exportedZipPath);
    System.out.println(result.getContent());

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    File exportedZip = exportedZipPath.toFile();
    assertThat(exportedZip).exists();

    Set<String> actualZipEnries =
        new ZipFile(exportedZip).stream().map(ZipEntry::getName).collect(Collectors.toSet());


    ConfigGroup exportedClusterGroup = cluster.configFiles("cluster.xml", "cluster.properties");
    ClusterConfig expectedExportedClusterConfig = new ClusterConfig(exportedClusterGroup);

    Set<String> expectedZipEntries = new HashSet<>();
    for (ConfigGroup group : expectedExportedClusterConfig.getGroups()) {
      String groupDir = group.getName() + File.separator;

      for (String jarOrXmlOrPropFile : group.getAllFiles()) {
        expectedZipEntries.add(groupDir + jarOrXmlOrPropFile);
      }
    }

    assertThat(actualZipEnries).isEqualTo(expectedZipEntries);
  }

}
