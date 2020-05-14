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

import static org.apache.geode.internal.lang.SystemUtils.CURRENT_DIRECTORY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DiskDirType;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.SnapshotTestUtil;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(PersistenceTest.class)
@RunWith(JUnitParamsRunner.class)
public class DiskStoreCommandsDUnitTest implements Serializable {
  private static final String GROUP = "GROUP1";
  private static final String GROUP2 = "GROUP2";
  private static final String REGION_1 = "REGION1";
  private static final String DISKSTORE = "DISKSTORE";

  @Rule
  public ClusterStartupRule rule = new ClusterStartupRule();

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public transient TemporaryFolder tempDir = new TemporaryFolder();

  private void createDiskStoreAndRegion(MemberVM jmxManager, int serverCount) {
    createDiskStore(jmxManager, serverCount, GROUP);

    gfsh.executeAndAssertThat(String.format(
        "create region --name=%s --type=REPLICATE_PERSISTENT --disk-store=%s --group=%s --eviction-action=overflow-to-disk",
        REGION_1, DISKSTORE, GROUP)).statusIsSuccess();
  }

  @SuppressWarnings("deprecation")
  private void createDiskStore(MemberVM jmxManager, int serverCount, String group) {
    gfsh.executeAndAssertThat(String.format(
        "create disk-store --name=%s --dir=%s --group=%s --auto-compact=false --compaction-threshold=99 --max-oplog-size=1 --allow-force-compaction=true",
        DISKSTORE, DISKSTORE, group))
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    gfsh.executeAndAssertThat("list disk-stores").statusIsSuccess()
        .tableHasColumnWithValuesContaining("Disk Store Name",
            IntStream.rangeClosed(1, serverCount).mapToObj(x -> DISKSTORE).toArray(String[]::new));
  }

  private static SerializableRunnableIF dataProducer() {
    return () -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      Region<String, String> r = cache.getRegion(REGION_1);
      r.put("A", "B");
    };
  }

  @Test
  public void createDuplicateDiskStoreFails() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);

    MemberVM locator = rule.startLocatorVM(0);
    @SuppressWarnings("unused")
    MemberVM server1 = rule.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    createDiskStore(locator, 1, GROUP);

    gfsh.executeAndAssertThat(String.format(
        "create disk-store --name=%s --dir=%s --group=%s --auto-compact=false --compaction-threshold=99 --max-oplog-size=1 --allow-force-compaction=true",
        DISKSTORE, DISKSTORE, GROUP))
        .statusIsError()
        .containsOutput("Error: Disk store DISKSTORE already exists");
  }

  @Test
  public void createDuplicateDiskStoreFailsNoServers() throws Exception {
    MemberVM locator = rule.startLocatorVM(0);

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat(String.format(
        "create disk-store --name=%s --dir=%s --group=%s --auto-compact=false --compaction-threshold=99 --max-oplog-size=1 --allow-force-compaction=true",
        DISKSTORE, DISKSTORE, GROUP))
        .statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void createDuplicateDiskStoreOnDifferentGroups() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);

    MemberVM locator = rule.startLocatorVM(0);
    rule.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator);
    createDiskStore(locator, 1, GROUP);

    props.setProperty("groups", GROUP2);

    rule.startServerVM(2, props, locator.getPort());
    createDiskStore(locator, 1, GROUP2);
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

    server1.invoke(dataProducer());

    gfsh.executeAndAssertThat("show missing-disk-stores")
        .containsOutput("No missing disk store found");

    server1.stop(false);

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      Region<String, String> r = cache.getRegion(REGION_1);
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

    TabularResultModel table =
        gfsh.executeAndAssertThat("show missing-disk-stores").statusIsSuccess()
            .containsOutput("Missing Disk Stores", "No missing colocated region found")
            .hasTableSection().getActual();

    List<String> diskstoreIDs = table.getValuesInColumn("Disk Store ID");
    assertThat(diskstoreIDs.size()).isEqualTo(1);

    gfsh.executeAndAssertThat("revoke missing-disk-store --id=" + diskstoreIDs.get(0))
        .statusIsSuccess().containsOutput("Missing disk store successfully revoked");

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + REGION_1, 1);

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      Region<String, String> r = cache.getRegion(REGION_1);
      assertThat(r.get("A")).isEqualTo("B");
    });
  }

  @Test
  public void testValidateOfflineDiskStoreCommand() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(dataProducer());

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

    server1.invoke(dataProducer());

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
    return jmxManager.invoke(() -> {
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      List<DiskStoreType> diskStores = sharedConfig.getCacheConfig(GROUP).getDiskStores();

      return diskStores.size() == 1 && DISKSTORE.equals(diskStores.get(0).getName());
    });
  }

  @Test
  public void testCannotDestroyDiskStoreInUse() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);

    MemberVM locator = rule.startLocatorVM(0);
    rule.startServerVM(1, props, locator.getPort());

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
    rule.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(locator, 1);

    gfsh.executeAndAssertThat("destroy region --name=" + REGION_1).statusIsSuccess();
    gfsh.executeAndAssertThat(
        String.format("destroy disk-store --name=%s --group=%s", DISKSTORE, GROUP))
        .statusIsSuccess();
    assertThat(diskStoreExistsInClusterConfig(locator)).isFalse();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testBackupDiskStore() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", GROUP);
    MemberVM locator = rule.startLocatorVM(0);
    MemberVM server1 = rule.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    createDiskStoreAndRegion(locator, 1);

    server1.invoke(dataProducer());

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
    rule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat(
        String.format("create disk-store --name=%s --dir=%s", DISKSTORE, DISKSTORE))
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    gfsh.executeAndAssertThat(String.format("destroy disk-store --name=%s --if-exists", DISKSTORE))
        .statusIsSuccess();

    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      InternalConfigurationPersistenceService cc =
          internalLocator.getConfigurationPersistenceService();
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
      assertThat(cache).isNotNull();
      Region<Integer, String> r = cache.getRegion(REGION_1);
      // Make sure we have more than 1 log and there is something to compact
      for (int i = 0; i < 10000; i++) {
        r.put(i, "value_" + i);
        r.put(i, "another_value_" + i);
      }
    });

    CommandResultAssert resultAssert = gfsh.executeAndAssertThat(
        String.format("compact disk-store --name=%s --group=%s", DISKSTORE, GROUP))
        .statusIsSuccess();

    resultAssert.hasDataSection("server-1").hasContent().containsKeys("UUID", "Host", "Directory");
    resultAssert.hasDataSection("server-2").hasContent().containsKeys("UUID", "Host", "Directory");
  }

  @Test
  public void testDescribeDiskStoreCommand() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(dataProducer());

    CommandResultAssert resultAssert = gfsh.executeAndAssertThat(
        String.format("describe disk-store --member=%s --name=%s", server1.getName(), DISKSTORE))
        .statusIsSuccess();
    resultAssert.hasDataSection(DescribeDiskStoreCommand.DISK_STORE_SECTION)
        .hasContent()
        .containsKeys("Disk Store Name", "Member ID", "Member Name",
            "Allow Force Compaction", "Auto Compaction", "Compaction Threshold", "Max Oplog Size",
            "Queue Size", "Time Interval", "Write Buffer Size", "Disk Usage Warning Percentage",
            "Disk Usage Critical Percentage", "PDX Serialization Meta-Data Stored");

    resultAssert.hasTableSection(DescribeDiskStoreCommand.DISK_DIR_SECTION)
        .hasColumn("Disk Directory").hasSize(1);

    resultAssert.hasTableSection(DescribeDiskStoreCommand.REGION_SECTION)
        .hasColumn("Region Path").hasSize(1);
  }

  @Test
  public void testDescribeMissingDiskStoreCommand() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<String, String> r = cache.getRegion(REGION_1);
      r.put("A", "B");
    });

    gfsh.executeAndAssertThat(
        String.format("describe disk-store --member=%s --name=%s", server1.getName(), "UNKNOWN"))
        .statusIsError()
        .containsOutput("A disk store with name 'UNKNOWN' was not found on member 'server-0'.");
  }

  @Test
  public void testUpgradeOfflineDiskStoreCommandFailsAsExpected() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(dataProducer());

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
    rule.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("revoke missing-disk-store --id=unknown-diskstore").statusIsError()
        .containsOutput("Unable to find missing disk store to revoke");
  }

  @Test
  public void testAlterOfflineDiskStoreCommandFailsAsExpected() throws Exception {
    MemberVM server1 = rule.startServerVM(0, x -> x.withJMXManager().withProperty("groups", GROUP));

    gfsh.connectAndVerify(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    createDiskStoreAndRegion(server1, 1);

    server1.invoke(dataProducer());

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

  @Test
  @Parameters({"compact offline-disk-store", "describe offline-disk-store",
      "upgrade offline-disk-store", "validate offline-disk-store",
      "alter disk-store --region=testRegion --enable-statistics=true"})
  public void offlineDiskStoreCommandShouldNotCreateFolderIfDiskStoreDoesNotExist(
      String baseCommand) {
    Path nonExistingDiskStorePath =
        Paths.get(tempDir.getRoot().getAbsolutePath() + File.separator + "nonExistingDiskStore");
    assertThat(Files.exists(nonExistingDiskStorePath)).isFalse();
    gfsh.executeAndAssertThat(baseCommand + " --name=" + DISKSTORE + " --disk-dirs="
        + nonExistingDiskStorePath.toAbsolutePath().toString()).statusIsError()
        .containsOutput("Could not find disk-dirs:");
    assertThat(Files.exists(nonExistingDiskStorePath)).isFalse();
  }

  @Test
  @Parameters(method = "getDiskDirNames")
  public void validateDiskStoreDiskDirectoryPath(String diskDirectoryName)
      throws Exception {
    // Start locator and server
    MemberVM locator = rule.startLocatorVM(0);
    MemberVM server = rule.startServerVM(1, locator.getPort());

    // Connect gfsh
    gfsh.connectAndVerify(locator);

    // Create a disk store with the input disk-dir name
    gfsh.executeAndAssertThat(
        String.format("create disk-store --name=%s --dir=%s", DISKSTORE, diskDirectoryName))
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    // Verify the server defines the disk store with the disk-dir path
    server.invoke(() -> verifyDiskStoreInServer(DISKSTORE, diskDirectoryName));

    // Verify the cluster config stores the disk store with the input disk-dir path
    locator.invoke(() -> verifyDiskStoreInClusterConfiguration(diskDirectoryName));

    // Stop and start the server
    rule.stop(1);
    server = rule.startServerVM(2, locator.getPort());

    // Verify the server still defines the disk store with the disk-dir path
    server.invoke(() -> verifyDiskStoreInServer(DISKSTORE, diskDirectoryName));
  }

  // Invoked via JUnit params
  @SuppressWarnings("unused")
  private String[] getDiskDirNames() throws IOException {
    tempDir.create();
    return new String[] {tempDir.newFolder(DISKSTORE).getAbsolutePath(), DISKSTORE};
  }

  private void verifyDiskStoreInServer(String diskStoreName, String diskDirectoryName) {
    DiskStore diskStore = ClusterStartupRule.getCache().findDiskStore(diskStoreName);
    assertThat(diskStore).isNotNull();
    File[] diskDirs = diskStore.getDiskDirs();
    assertThat(diskDirs.length).isEqualTo(1);
    File diskDir = diskDirs[0];
    String absoluteDiskDirectoryName = diskDirectoryName.startsWith(File.separator)
        ? diskDirectoryName : CURRENT_DIRECTORY + File.separator + diskDirectoryName;
    assertThat(diskDir.getAbsolutePath()).isEqualTo(absoluteDiskDirectoryName);
  }

  private void verifyDiskStoreInClusterConfiguration(String absoluteDirectoryName) {
    InternalLocator internalLocator = ClusterStartupRule.getLocator();
    InternalConfigurationPersistenceService cc =
        internalLocator.getConfigurationPersistenceService();
    CacheConfig config = cc.getCacheConfig("cluster");
    List<DiskStoreType> diskStores = config.getDiskStores();
    assertThat(diskStores.size()).isEqualTo(1);
    DiskStoreType diskStore = diskStores.get(0);
    List<DiskDirType> diskDirs = diskStore.getDiskDirs();
    assertThat(diskDirs.size()).isEqualTo(1);
    DiskDirType diskDir = diskDirs.get(0);
    assertThat(diskDir.getContent()).isEqualTo(absoluteDirectoryName);
  }
}
