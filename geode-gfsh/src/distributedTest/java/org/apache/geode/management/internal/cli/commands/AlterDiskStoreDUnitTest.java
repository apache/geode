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
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;

@Category({PersistenceTest.class})
public class AlterDiskStoreDUnitTest {
  private static final String regionName = "region1";
  private static final String diskStoreName = "disk-store1";
  private static final String diskDirName = "diskStoreDir";
  private static final String aKey = "key1";

  private static MemberVM locator;
  private static MemberVM server1;

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void setupCluster() throws Exception {

    File diskStoreDirFile = new File(getDiskDirPathString());

    if (!diskStoreDirFile.exists()) {
      diskStoreDirFile.mkdirs();
    }

    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, locator.getPort());
    String diskDirPathString = getDiskDirPathString();
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createDiskStoreFactory().setDiskDirs(new File[] {new File(diskDirPathString)})
          .setMaxOplogSize(1).setAllowForceCompaction(true).setAutoCompact(false)
          .create(diskStoreName);

      EvictionAttributes ea =
          EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK);

      RegionFactory<String, String> regionFactory = cache.createRegionFactory();
      Region<String, String> region = regionFactory.setDiskStoreName(diskStoreName)
          .setDiskSynchronous(true).setDataPolicy(DataPolicy.PERSISTENT_REPLICATE)
          .setScope(Scope.DISTRIBUTED_ACK).setEvictionAttributes(ea).create(regionName);
      region.put(aKey, "value");
    });
  }

  @Test
  public void diskStoreIsLockedWhileMemberIsAlive() throws Exception {
    gfsh.connectAndVerify(locator.getJmxPort(), PortType.jmxManager);
    String commandString = commandToSetManyVariables();
    gfsh.executeAndAssertThat(commandString).statusIsError().containsOutput("Could not lock",
        "Other JVMs might have created diskstore with same name using the same directory");
  }

  @Test
  public void alterDiskStoreUpdatesValues() throws Exception {
    startupRule.stop(1);

    gfsh.connectAndVerify(locator.getJmxPort(), PortType.jmxManager);
    String commandOne = commandToSetManyVariables();
    gfsh.executeAndAssertThat(commandOne).statusIsSuccess().containsOutput("concurrencyLevel=5",
        "lruAction=local-destroy", "compressor=org.apache.geode.compression.SnappyCompressor",
        "initialCapacity=6");

    String commandTwo = commandToSetCompressorToNone();
    gfsh.executeAndAssertThat(commandTwo).statusIsSuccess().containsOutput("-compressor=none");
  }

  @Test
  public void alterDiskStoreWithRemoveDoesRemoveRegion() throws IOException {
    // Verify to start that there exists the data in the region
    server1.invoke(() -> {
      Region<?, ?> region = ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(region).isNotNull();
      assertThat(region.get(aKey)).isNotNull();
    });

    // Halt the member and "alter" the disk store with the --remove option
    startupRule.stop(1);
    String cmd = commandWithRemoveAndNoOtherOption();
    gfsh.executeAndAssertThat(cmd).statusIsSuccess().containsOutput("The region " + regionName
        + " was successfully removed from the disk store " + diskStoreName);

    // Restart the member and see that the disk store / region is gone.
    startupRule.startServerVM(1, locator.getPort());
    server1.invoke(() -> {
      Region<?, ?> region = ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(region).isNull();
    });
  }

  private String getDiskDirPathString() throws IOException {
    return gfsh.getWorkingDir().toPath().resolve(diskDirName).toFile().getCanonicalPath();
  }

  private String commandWithRemoveAndNoOtherOption() throws IOException {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, diskStoreName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, regionName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, getDiskDirPathString());
    csb.addOption(CliStrings.ALTER_DISK_STORE__REMOVE, "true");
    return csb.toString();
  }

  private String commandToSetCompressorToNone() throws IOException {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, diskStoreName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, regionName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, getDiskDirPathString());
    csb.addOption(CliStrings.ALTER_DISK_STORE__COMPRESSOR, "none");
    return csb.toString();
  }

  private String commandToSetManyVariables() throws IOException {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, diskStoreName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, regionName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, getDiskDirPathString());
    csb.addOption(CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL, "5");
    csb.addOption(CliStrings.ALTER_DISK_STORE__INITIAL__CAPACITY, "6");
    csb.addOption(CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ACTION, "local-destroy");
    csb.addOption(CliStrings.ALTER_DISK_STORE__COMPRESSOR,
        "org.apache.geode.compression.SnappyCompressor");
    csb.addOption(CliStrings.ALTER_DISK_STORE__STATISTICS__ENABLED, "true");
    return csb.toString();
  }
}
