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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.SnapshotTestUtil;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class DiskStoreCommandsDUnitTest {

  private static final String REGION_1 = "REGION1";
  private static final String DISKSTORE = "DISKSTORE";
  private static final String GROUP = "GROUP1";

  @Rule
  public LocatorServerStartupRule rule = new LocatorServerStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private void createDiskStoreAndRegion(MemberVM jmxManager, int serverCount) {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=%s --dir=%s --group=%s",
        DISKSTORE, DISKSTORE, GROUP));

    List<String> diskStores =
        IntStream.rangeClosed(1, serverCount).mapToObj(x -> DISKSTORE).collect(Collectors.toList());
    gfsh.executeAndAssertThat("list disk-stores").statusIsSuccess()
        .tableHasColumnWithValuesContaining("Disk Store Name", diskStores.toArray(new String[0]));

    jmxManager.waitTillDiskstoreIsReady(DISKSTORE, serverCount);

    gfsh.executeAndAssertThat(String.format(
        "create region --name=%s --type=REPLICATE_PERSISTENT --disk-store=%s --group=%s --eviction-action=overflow-to-disk",
        REGION_1, DISKSTORE, GROUP)).statusIsSuccess();
  }

  @Test
  public void testMissingDiskStore() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);

    MemberVM locator = rule.startLocatorVM(0);
    MemberVM server1 = rule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = rule.startServerVM(2, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    createDiskStoreAndRegion(locator, 2);

    server1.invoke(() -> {
      Cache cache = LocatorServerStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    gfsh.executeAndAssertThat("show missing-disk-stores")
        .containsOutput("No missing disk store found");

    server1.invoke(LocatorServerStartupRule::stopMemberInThisVM);

    server2.invoke(() -> {
      Cache cache = LocatorServerStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "C");
    });

    server2.invoke(LocatorServerStartupRule::stopMemberInThisVM);

    props.setProperty("log-file", "");
    rule.startServerVMAsync(server1.getVM().getId(), props, locator.getPort());

    locator.waitTillDiskstoreIsReady(DISKSTORE, 1);

    gfsh.executeAndAssertThat("show missing-disk-stores").statusIsSuccess()
        .containsOutput("Missing Disk Stores", "No missing colocated region found");

    List<Object> diskstoreIDs = gfsh.getCommandResult().getColumnValues("Disk Store ID");
    assertThat(diskstoreIDs.size()).isEqualTo(1);

    gfsh.executeAndAssertThat("revoke missing-disk-store --id=" + diskstoreIDs.get(0))
        .statusIsSuccess().containsOutput("Missing disk store successfully revoked");

    locator.waitTillRegionsAreReadyOnServers("/" + REGION_1, 1);

    server1.invoke(() -> {
      Cache cache = LocatorServerStartupRule.getCache();
      Region<String, String> r = cache.getRegion(REGION_1);
      assertThat(r.get("A")).isEqualTo("B");
    });
  }

  @Test
  public void testDescribeOfflineDiskStoreCommand() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);
    MemberVM server1 = rule.startServerAsJmxManager(0, props);

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(() -> {
      Cache cache = LocatorServerStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    gfsh.executeAndAssertThat("show missing-disk-stores")
        .containsOutput("No missing disk store found");

    server1.invoke(LocatorServerStartupRule::stopMemberInThisVM);

    String diskDirs = new File(server1.getWorkingDir(), DISKSTORE).getAbsolutePath();
    gfsh.executeAndAssertThat(
        "validate offline-disk-store --name=" + DISKSTORE + " --disk-dirs=" + diskDirs)
        .statusIsSuccess()
        .containsOutput("Total number of region entries in this disk store is: 1");
  }

  @Test
  public void testExportOfflineDiskStore() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);
    MemberVM server1 = rule.startServerAsJmxManager(0, props);

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(() -> {
      Cache cache = LocatorServerStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    gfsh.executeAndAssertThat("show missing-disk-stores")
        .containsOutput("No missing disk store found");

    server1.invoke(LocatorServerStartupRule::stopMemberInThisVM);

    String diskDirs = new File(server1.getWorkingDir(), DISKSTORE).getAbsolutePath();
    File exportDir = tempDir.newFolder();
    gfsh.executeAndAssertThat("export offline-disk-store --name=" + DISKSTORE + " --disk-dirs="
        + diskDirs + " --dir=" + exportDir.getAbsolutePath()).statusIsSuccess()
        .containsOutput("Exported all regions from disk store DISKSTORE");

    Map<String, String> entries = new HashMap<>();
    entries.put("A", "B");
    SnapshotTestUtil.checkSnapshotEntries(exportDir, entries, DISKSTORE, REGION_1);
  }

  private boolean diskStoreExistsInClusterConfig(MemberVM jmxManager) {
    boolean result = jmxManager.invoke(() -> {
      ClusterConfigurationService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
      String xmlFromConfig;
      xmlFromConfig = sharedConfig.getConfiguration(GROUP).getCacheXmlContent();
      return xmlFromConfig.contains(DISKSTORE);
    });

    return result;
  }

  @Test
  public void testCannotDestroyDiskStoreInUse() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);

    MemberVM locator = rule.startLocatorVM(0);
    MemberVM server1 = rule.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    createDiskStoreAndRegion(locator, 1);

    gfsh.executeAndAssertThat(
        String.format("destroy disk-store --name=%s --group=%s", DISKSTORE, GROUP)).statusIsError()
        .containsOutput("Disk store is currently in use by these regions");
    assertThat(diskStoreExistsInClusterConfig(locator)).isTrue();
  }

  @Test
  public void testDestroyUpdatesSharedConfig() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);

    MemberVM locator = rule.startLocatorVM(0);
    MemberVM server1 = rule.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(locator, 1);

    gfsh.executeAndAssertThat("destroy region --name=" + REGION_1).statusIsSuccess();
    gfsh.executeAndAssertThat(
        String.format("destroy disk-store --name=%s --group=%s", DISKSTORE, GROUP))
        .statusIsSuccess();
    assertThat(diskStoreExistsInClusterConfig(locator)).isFalse();
  }


  @Test
  public void testBackupDiskStore() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);
    MemberVM locator = rule.startLocatorVM(0);
    MemberVM server1 = rule.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    createDiskStoreAndRegion(locator, 1);

    server1.invoke(() -> {
      Cache cache = LocatorServerStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    String backupDir = tempDir.newFolder().getCanonicalPath();
    String diskDirs = new File(server1.getWorkingDir(), DISKSTORE).getAbsolutePath();
    gfsh.executeAndAssertThat(
        String.format("backup disk-store --dir=%s --baseline-dir=%s", diskDirs, backupDir))
        .statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "locator-0", "server-1");
  }

  @Test
  public void destroyDiskStoreIsIdempotent() throws Exception {
    MemberVM locator = rule.startLocatorVM(0);
    MemberVM server1 = rule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat(
        String.format("create disk-store --name=%s --dir=%s", DISKSTORE, DISKSTORE))
        .statusIsSuccess();

    gfsh.executeAndAssertThat(String.format("destroy disk-store --name=%s --if-exists", DISKSTORE))
        .statusIsSuccess();
    gfsh.executeAndAssertThat(String.format("destroy disk-store --name=%s --if-exists", DISKSTORE))
        .statusIsSuccess();
  }
}
