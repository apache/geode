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
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
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
public class ClusterConfigImportDUnitTest extends ClusterConfigTestBase {

  private static final ClusterConfig INITIAL_CONFIG = new ClusterConfig(new ConfigGroup("cluster"));

  private MemberVM locatorVM;

  @Rule
  public GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  @Before
  public void before() throws Exception {
    locatorVM = lsRule.startLocatorVM(0, locatorProps);
    INITIAL_CONFIG.verify(locatorVM);

    gfshConnector.connect(locatorVM);
    assertThat(gfshConnector.isConnected()).isTrue();
  }

  @Test
  public void testImportWithRunningServerWithRegion() throws Exception {
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locatorVM.getPort());
    // create another server as well
    MemberVM server2 = lsRule.startServerVM(2, serverProps, locatorVM.getPort());
    String regionName = "regionA";
    server1.invoke(() -> {
      // this region will be created on both servers, but we should only be getting the name once.
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    });

    CommandResult result = gfshConnector
        .executeCommand("import cluster-configuration --zip-file-name=" + clusterConfigZipPath);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("existing regions: " + regionName);
  }

  @Test
  public void testImportWithRunningServer() throws Exception {
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locatorVM.getPort());

    serverProps.setProperty("groups", "group2");
    MemberVM server2 = lsRule.startServerVM(2, serverProps, locatorVM.getPort());

    // even though we have a region recreated, we can still import since there is no data
    // in the region
    CommandResult result = gfshConnector
        .executeCommand("import cluster-configuration --zip-file-name=" + clusterConfigZipPath);
    assertThat(result.getContent().toString())
        .contains("Successfully applied the imported cluster configuration on server-1");
    assertThat(result.getContent().toString())
        .contains("Successfully applied the imported cluster configuration on server-2");
    new ClusterConfig(CLUSTER).verify(server1);
    new ClusterConfig(CLUSTER, GROUP2).verify(server2);
  }

  @Test
  public void testImportClusterConfig() throws Exception {
    gfshConnector.executeAndVerifyCommand(
        "import cluster-configuration --zip-file-name=" + clusterConfigZipPath);

    // Make sure that a backup of the old clusterConfig was created
    assertThat(locatorVM.getWorkingDir().listFiles())
        .filteredOn((File file) -> file.getName().contains("cluster_config")).hasSize(2);

    CONFIG_FROM_ZIP.verify(locatorVM);

    // start server1 with no group
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locatorVM.getPort());
    new ClusterConfig(CLUSTER).verify(server1);

    // start server2 in group1
    serverProps.setProperty(GROUPS, "group1");
    MemberVM server2 = lsRule.startServerVM(2, serverProps, locatorVM.getPort());
    new ClusterConfig(CLUSTER, GROUP1).verify(server2);

    // start server3 in group1 and group2
    serverProps.setProperty(GROUPS, "group1,group2");
    MemberVM server3 = lsRule.startServerVM(3, serverProps, locatorVM.getPort());
    new ClusterConfig(CLUSTER, GROUP1, GROUP2).verify(server3);
  }

  @Test
  public void testImportWithMultipleLocators() throws Exception {
    locatorProps.setProperty(LOCATORS, "localhost[" + locatorVM.getPort() + "]");
    MemberVM locator1 = lsRule.startLocatorVM(1, locatorProps);

    locatorProps.setProperty(LOCATORS,
        "localhost[" + locatorVM.getPort() + "],localhost[" + locator1.getPort() + "]");
    MemberVM locator2 = lsRule.startLocatorVM(2, locatorProps);

    gfshConnector.executeAndVerifyCommand(
        "import cluster-configuration --zip-file-name=" + clusterConfigZipPath);

    CONFIG_FROM_ZIP.verify(locatorVM);
    REPLICATED_CONFIG_FROM_ZIP.verify(locator1);
    REPLICATED_CONFIG_FROM_ZIP.verify(locator2);
  }

  @Test
  public void testExportWithAbsolutePath() throws Exception {
    Path exportedZipPath =
        lsRule.getTempFolder().getRoot().toPath().resolve("exportedCC.zip").toAbsolutePath();

    testExportClusterConfig(exportedZipPath.toString());
  }

  @Test
  public void testExportWithRelativePath() throws Exception {
    testExportClusterConfig("tmp/exportedCC.zip");
  }

  public void testExportClusterConfig(String zipFilePath) throws Exception {
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locatorVM.getPort());

    gfshConnector.executeAndVerifyCommand("create region --name=myRegion --type=REPLICATE");

    ConfigGroup cluster = new ConfigGroup("cluster").regions("myRegion");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(locatorVM);

    gfshConnector
        .executeAndVerifyCommand("export cluster-configuration --zip-file-name=" + zipFilePath);

    File exportedZip = new File(zipFilePath);
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
