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
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.SnapshotTestUtil;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({PersistenceTest.class})
public class DiskStoreCommandsDUnitTest {

  private static final String REGION_1 = "REGION1";
  private static final String DISKSTORE = "DISKSTORE";
  private static final String GROUP = "GROUP1";

  @Rule
  public ClusterStartupRule rule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private void createDiskStoreAndRegion(MemberVM jmxManager, int serverCount) {
    gfsh.executeAndAssertThat(String.format(
        "create disk-store --name=%s --dir=%s --group=%s --auto-compact=false --compaction-threshold=99 --max-oplog-size=1 --allow-force-compaction=true",
        DISKSTORE, DISKSTORE, GROUP));

    List<String> diskStores =
        IntStream.rangeClosed(1, serverCount).mapToObj(x -> DISKSTORE).collect(Collectors.toList());
    gfsh.executeAndAssertThat("list disk-stores").statusIsSuccess()
        .tableHasColumnWithValuesContaining("Disk Store Name", diskStores.toArray(new String[0]));

    jmxManager.waitUntilDiskStoreIsReadyOnExactlyThisManyServers(DISKSTORE, serverCount);

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
      Cache cache = ClusterStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    gfsh.executeAndAssertThat("show missing-disk-stores")
        .containsOutput("No missing disk store found");

    server1.stop(false);

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "C");
    });

    server2.stop(false);

    int locatorPort = locator.getPort();
    server1.invokeAsync(() -> {
      // trying to restart the server asynchronously
      ServerStarterRule serverRule = new ServerStarterRule().withProperties(props)
          .withName("server-1").withConnectionToLocator(locatorPort).withAutoStart();
      ClusterStartupRule.memberStarter = serverRule;
      serverRule.before();
    });

    locator.waitUntilDiskStoreIsReadyOnExactlyThisManyServers(DISKSTORE, 1);

    gfsh.executeAndAssertThat("show missing-disk-stores").statusIsSuccess()
        .containsOutput("Missing Disk Stores", "No missing colocated region found");

    List<String> diskstoreIDs = gfsh.getCommandResult().getTableColumnValues("Disk Store ID");
    assertThat(diskstoreIDs.size()).isEqualTo(1);

    gfsh.executeAndAssertThat("revoke missing-disk-store --id=" + diskstoreIDs.get(0))
        .statusIsSuccess().containsOutput("Missing disk store successfully revoked");

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + REGION_1, 1);

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<String, String> r = cache.getRegion(REGION_1);
      assertThat(r.get("A")).isEqualTo("B");
    });
  }

  @Test
  public void testValidateOfflineDiskStoreCommand() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    gfsh.executeAndAssertThat("show missing-disk-stores")
        .containsOutput("No missing disk store found");

    server1.stop(false);

    String diskDirs = new File(server1.getWorkingDir(), DISKSTORE).getAbsolutePath();
    gfsh.executeAndAssertThat(
        "validate offline-disk-store --name=" + DISKSTORE + " --disk-dirs=" + diskDirs)
        .statusIsSuccess()
        .containsOutput("Total number of region entries in this disk store is: 1");
  }

  @Test
  public void testExportOfflineDiskStore() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    gfsh.executeAndAssertThat("show missing-disk-stores")
        .containsOutput("No missing disk store found");

    server1.stop(false);

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
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      List<DiskStoreType> diskStores = sharedConfig.getCacheConfig(GROUP).getDiskStores();

      return diskStores.size() == 1 && DISKSTORE.equals(diskStores.get(0).getName());
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
      Cache cache = ClusterStartupRule.getCache();
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

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");
      assertThat(config.getDiskStores().size()).isEqualTo(0);
    });

    gfsh.executeAndAssertThat(String.format("destroy disk-store --name=%s --if-exists", DISKSTORE))
        .statusIsSuccess();
  }

  @Test
  public void testCompactDiskStore() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);

    MemberVM locator = rule.startLocatorVM(0);
    MemberVM server1 = rule.startServerVM(1, props, locator.getPort());
    rule.startServerVM(2, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    createDiskStoreAndRegion(locator, 2);

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      // Make sure we have more than 1 log and there is something to compact
      for (int i = 0; i < 10000; i++) {
        r.put(i, "value_" + i);
        r.put(i, "another_value_" + i);
      }
    });

    gfsh.executeAndAssertThat(
        String.format("compact disk-store --name=%s --group=%s", DISKSTORE, GROUP))
        .statusIsSuccess();

    CommandResult cmdResult = gfsh.getCommandResult();
    assertThat(cmdResult.getMapFromSection("server-1").keySet()).contains("UUID", "Host",
        "Directory");
    assertThat(cmdResult.getMapFromSection("server-2").keySet()).contains("UUID", "Host",
        "Directory");
  }

  @Test
  public void testDescribeDiskStoreCommand() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    CommandResult result = gfsh.executeCommand(
        String.format("describe disk-store --member=%s --name=%s", server1.getName(), DISKSTORE));
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    Map<String, String> data =
        result.getMapFromSection(DescribeDiskStoreCommand.DISK_STORE_SECTION);
    assertThat(data.keySet()).contains("Disk Store Name", "Member ID", "Member Name",
        "Allow Force Compaction", "Auto Compaction", "Compaction Threshold", "Max Oplog Size",
        "Queue Size", "Time Interval", "Write Buffer Size", "Disk Usage Warning Percentage",
        "Disk Usage Critical Percentage", "PDX Serialization Meta-Data Stored");

    Map<String, List<String>> directories =
        result.getMapFromTableContent(DescribeDiskStoreCommand.DISK_DIR_SECTION);
    assertThat(directories.get("Disk Directory").size()).isEqualTo(1);

    Map<String, List<String>> regions =
        result.getMapFromTableContent(DescribeDiskStoreCommand.REGION_SECTION);
    assertThat(regions.get("Region Path").size()).isEqualTo(1);
  }

  @Test
  public void testUpgradeOfflineDiskStoreCommandFailsAsExpected() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    // Should not be able to do this on a running system
    String diskDirs = new File(server1.getWorkingDir(), DISKSTORE).getAbsolutePath();
    gfsh.executeAndAssertThat(
        String.format("upgrade offline-disk-store --name=%s --disk-dirs=%s", DISKSTORE, diskDirs))
        .statusIsError().containsOutput("The disk is currently being used by another process");

    server1.stop(false);

    gfsh.executeAndAssertThat(
        String.format("upgrade offline-disk-store --name=%s --disk-dirs=%s", DISKSTORE, diskDirs))
        .statusIsError().containsOutput("This disk store is already at version");
  }

  @Test
  public void revokingUnknownDiskStoreShowsErrorCorrectly() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);

    MemberVM locator = rule.startLocatorVM(0);
    MemberVM server1 = rule.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat("revoke missing-disk-store --id=unknown-diskstore")
        .statusIsError().containsOutput("Unable to find missing disk store to revoke");
  }

  @Test
  public void testAlterOfflineDiskStoreCommandFailsAsExpected() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    String diskDirs = new File(server1.getWorkingDir(), DISKSTORE).getAbsolutePath();
    gfsh.executeAndAssertThat(
        String.format("alter disk-store --name=%s --region=%s --disk-dirs=%s --compressor=foo.Bar",
            DISKSTORE, REGION_1, diskDirs))
        .statusIsError().containsOutput("DISKSTORE: Could not lock");

    server1.stop(false);
    gfsh.disconnect();

    gfsh.executeAndAssertThat(
        String.format(
            "alter disk-store --name=%s --region=INVALID --disk-dirs=%s --compressor=foo.Bar",
            DISKSTORE, diskDirs))
        .statusIsError().containsOutput("The disk store does not contain a region named: /INVALID");
  }
}
