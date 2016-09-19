/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.LogWriterUtils.*;
import static org.apache.geode.test.dunit.Wait.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.SnapshotTestUtil;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

/**
 * The DiskStoreCommandsDUnitTest class is a distributed test suite of test cases for testing the disk store commands
 * that are part of Gfsh. </p>
 * @see org.apache.geode.management.internal.cli.commands.DiskStoreCommands
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class DiskStoreCommandsDUnitTest extends CliCommandTestBase {

  final List<String> filesToBeDeleted = new CopyOnWriteArrayList<String>();

  @Test
  public void testMissingDiskStore() {
    final String regionName = "testShowMissingDiskStoreRegion";

    setUpJmxManagerOnVm0ThenConnect(null);

    final VM vm0 = Host.getHost(0).getVM(0);
    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "VM" + vm1.getPid();
    final String diskStoreName = "DiskStoreCommandsDUnitTest";

    // Default setup creates a cache in the Manager, now create a cache in VM1
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm1Name);
        getSystem(localProps);
        Cache cache = getCache();
      }
    });

    // Create a disk store and region in the Manager (VM0) and VM1 VMs
    for (final VM vm : (new VM[] { vm0, vm1 })) {
      final String vmName = "VM" + vm.getPid();
      vm.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();

          File diskStoreDirFile = new File(diskStoreName + vm.getPid());
          diskStoreDirFile.mkdirs();

          DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
          diskStoreFactory.setDiskDirs(new File[] { diskStoreDirFile });
          diskStoreFactory.setMaxOplogSize(1);
          diskStoreFactory.setAllowForceCompaction(true);
          diskStoreFactory.setAutoCompact(false);
          diskStoreFactory.create(regionName);

          RegionFactory regionFactory = cache.createRegionFactory();
          regionFactory.setDiskStoreName(regionName);
          regionFactory.setDiskSynchronous(true);
          regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          regionFactory.setScope(Scope.DISTRIBUTED_ACK);
          regionFactory.create(regionName);
        }
      });
    }

    // Add data to the region
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        region.put("A", "B");
      }
    });

    // Make sure that everything thus far is okay and there are no missing disk stores
    CommandResult cmdResult = executeCommand(CliStrings.SHOW_MISSING_DISK_STORE);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No missing disk store found"));

    // Close the region in the Manager (VM0) VM
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        region.close();
      }
    });

    // Add data to VM1 and then close the region
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        region.put("A", "C");
        region.close();
      }
    });

    // Add the region back to the Manager (VM0) VM
    vm0.invokeAsync(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();

        RegionFactory regionFactory = cache.createRegionFactory();
        regionFactory.setDiskStoreName(regionName);
        regionFactory.setDiskSynchronous(true);
        regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        regionFactory.setScope(Scope.DISTRIBUTED_ACK);
        try {
          regionFactory.create(regionName);
        } catch (DistributedSystemDisconnectedException e) {
          // okay to ignore
        }
      }
    });

    // Wait for the region in the Manager (VM0) to come online
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        WaitCriterion waitCriterion = new WaitCriterion() {
          public boolean done() {
            Cache cache = getCache();
            PersistentMemberManager memberManager = ((GemFireCacheImpl) cache).getPersistentMemberManager();
            return !memberManager.getWaitingRegions().isEmpty();
          }

          public String description() {
            return "Waiting for another persistent member to come online";
          }
        };
        waitForCriterion(waitCriterion, 70000, 100, true);
      }
    });

    // Validate that there is a missing disk store on VM1
    cmdResult = executeCommand(CliStrings.SHOW_MISSING_DISK_STORE);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    String stringResult = commandResultToString(cmdResult);
    System.out.println("command result=" + stringResult);
    assertEquals(5, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Disk Store ID.*Host.*Directory"));
    assertTrue(stringContainsLine(stringResult, ".*" + diskStoreName + vm1.getPid()));

    // Extract the id from the returned missing disk store
    String line = getLineFromString(stringResult, 4);
    assertFalse(line.contains("---------"));
    StringTokenizer resultTokenizer = new StringTokenizer(line);
    String id = resultTokenizer.nextToken();

    // Remove the missing disk store and validate the result
    cmdResult = executeCommand("revoke missing-disk-store --id=" + id);
    assertNotNull(cmdResult);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("Missing disk store successfully revoked"));

    // Do our own cleanup so that the disk store directories can be removed
    super.destroyDefaultSetup();
    for (final VM vm : (new VM[] { vm0, vm1 })) {
      final String vmName = "VM" + vm.getPid();
      vm.invoke(new SerializableRunnable() {
        public void run() {
          try {
            FileUtil.delete((new File(diskStoreName + vm.getPid())));
          } catch (IOException iex) {
            // There's nothing else we can do
          }
        }
      });
    }
  }

  @Test
  public void testMissingDiskStoreCommandWithColocation() {
    final String regionName = "testShowPersistentRecoveryFailuresRegion";
    final String childName = "childRegion";

    setUpJmxManagerOnVm0ThenConnect(null);

    final VM vm0 = Host.getHost(0).getVM(0);
    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "VM" + vm1.getPid();
    final String diskStoreName = "DiskStoreCommandsDUnitTest";

    // Default setup creates a cache in the Manager, now create a cache in VM1
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm1Name);
        getSystem(localProps);
        Cache cache = getCache();
      }
    });

    // Create a disk store and region in the Manager (VM0) and VM1 VMs
    for (final VM vm : (new VM[]{vm0, vm1})) {
      final String vmName = "VM" + vm.getPid();
      vm.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();

          File diskStoreDirFile = new File(diskStoreName + vm.getPid());
          diskStoreDirFile.mkdirs();

          DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
          diskStoreFactory.setDiskDirs(new File[]{diskStoreDirFile});
          diskStoreFactory.setMaxOplogSize(1);
          diskStoreFactory.setAllowForceCompaction(true);
          diskStoreFactory.setAutoCompact(false);
          diskStoreFactory.create(regionName);
          diskStoreFactory.create(childName);

          RegionFactory regionFactory = cache.createRegionFactory();
          regionFactory.setDiskStoreName(regionName);
          regionFactory.setDiskSynchronous(true);
          regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          regionFactory.create(regionName);

          PartitionAttributes pa = new PartitionAttributesFactory().setColocatedWith(regionName).create();
          RegionFactory childRegionFactory = cache.createRegionFactory();
          childRegionFactory.setPartitionAttributes(pa);
          childRegionFactory.setDiskStoreName(childName);
          childRegionFactory.setDiskSynchronous(true);
          childRegionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          childRegionFactory.create(childName);
        }
      });
    }

    // Add data to the region
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        region.put("A", "a");
        region.put("B", "b");
      }
    });

    // Make sure that everything thus far is okay and there are no missing disk stores
    CommandResult cmdResult = executeCommand(CliStrings.SHOW_MISSING_DISK_STORE);
    System.out.println("command result=\n" + commandResultToString(cmdResult));

    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(cmdResult.toString(), commandResultToString(cmdResult).contains("No missing disk store found"));

    // Close the regions in the Manager (VM0) VM
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(childName);
        region.close();
        region = cache.getRegion(regionName);
        region.close();
      }
    });

    // Add data to VM1 and then close the region
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        Region childRegion = cache.getRegion(childName);
        PartitionedRegion parentRegion = (PartitionedRegion)(cache.getRegion(regionName));
        try {
          parentRegion.put("A", "C");
        } catch (Exception e) {
          //Ignore any exception on the put
        }
        childRegion.close();
        parentRegion.close();
      }
    });

    SerializableRunnable restartParentRegion = new SerializableRunnable("Restart parent region on") {
      public void run() {
        Cache cache = getCache();

        RegionFactory regionFactory = cache.createRegionFactory();
        regionFactory.setDiskStoreName(regionName);
        regionFactory.setDiskSynchronous(true);
        regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        try {
          regionFactory.create(regionName);
        } catch (Exception e) {
          // okay to ignore
        }
      }
    };

    SerializableRunnable restartChildRegion = new SerializableRunnable("Restart child region") {
      public void run() {
        Cache cache = getCache();

        PartitionAttributes pa = new PartitionAttributesFactory().setColocatedWith(regionName).create();
        RegionFactory regionFactory = cache.createRegionFactory();
        regionFactory.setPartitionAttributes(pa);
        regionFactory.setDiskStoreName(childName);
        regionFactory.setDiskSynchronous(true);
        regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        try {
          regionFactory.create(childName);
        } catch (Exception e) {
          // okay to ignore
          e.printStackTrace();
        }
      }
    };

    // Add the region back to the Manager (VM0) VM
    AsyncInvocation async0 = vm0.invokeAsync(restartParentRegion);
    AsyncInvocation async1 = vm1.invokeAsync(restartParentRegion);

    // Wait for the region in the Manager (VM0) to come online
    vm0.invoke(new SerializableRunnable("WaitForRegionInVm0") {
      public void run() {
        WaitCriterion waitCriterion = new WaitCriterion() {
          public boolean done() {
            Cache cache = getCache();
            PersistentMemberManager memberManager = ((GemFireCacheImpl) cache).getPersistentMemberManager();
            return !memberManager.getWaitingRegions().isEmpty();
          }

          public String description() {
            return "Waiting for another persistent member to come online";
          }
        };
        try {
          waitForCriterion(waitCriterion, 5000, 100, true);
        } catch (AssertionError ae) {
          // Ignore. waitForCriterion is expected to timeout in this test
        }
      }
    });

    // Validate that there is a missing disk store on VM1
    try {
      cmdResult = executeCommand(CliStrings.SHOW_MISSING_DISK_STORE);
      assertNotNull("Expect command result != null", cmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      String stringResult = commandResultToString(cmdResult);
      System.out.println("command result=\n" + stringResult);
      // Expect 2 result sections with header lines and 4 information lines in the first section
      assertEquals(6, countLinesInString(stringResult, false));
      assertTrue(stringContainsLine(stringResult, "Host.*Distributed Member.*Parent Region.*Missing Colocated Region"));
      assertTrue(stringContainsLine(stringResult, ".*" + regionName + ".*" + childName));

      AsyncInvocation async0b = vm0.invokeAsync(restartChildRegion);
      try {
        async0b.get(5000, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        // Expected timeout - Region recovery is still waiting on vm1 child region and disk-store to come online
      }

      cmdResult = executeCommand(CliStrings.SHOW_MISSING_DISK_STORE);
      assertNotNull("Expect command result != null", cmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      stringResult = commandResultToString(cmdResult);
      System.out.println("command result=\n" + stringResult);

      // Extract the id from the returned missing disk store
      String line = getLineFromString(stringResult, 4);
      assertFalse(line.contains("---------"));
      StringTokenizer resultTokenizer = new StringTokenizer(line);
      String id = resultTokenizer.nextToken();

      AsyncInvocation async1b = vm1.invokeAsync(restartChildRegion);
      try {
        async1b.get(5000, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        e.printStackTrace();
      }
      cmdResult = executeCommand(CliStrings.SHOW_MISSING_DISK_STORE);
      assertNotNull("Expect command result != null", cmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      stringResult = commandResultToString(cmdResult);
      System.out.println("command result=\n" + stringResult);

    } finally {
      // Verify that the invokeAsync thread terminated
      try {
        async0.get(10000, TimeUnit.MILLISECONDS);
        async1.get(10000, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        fail("Unexpected timeout waitiong for invokeAsync threads to terminate: " + e.getMessage());
      }
    }

    // Do our own cleanup so that the disk store directories can be removed
    super.destroyDefaultSetup();
    for (final VM vm : (new VM[]{vm0, vm1})) {
      final String vmName = "VM" + vm.getPid();
      vm.invoke(new SerializableRunnable() {
        public void run() {
          try {
            FileUtil.delete((new File(diskStoreName + vm.getPid())));
          } catch (IOException iex) {
            // There's nothing else we can do
          }
        }
      });
    }
  }

  @Test
  public void testDescribeOfflineDiskStore() {
    setUpJmxManagerOnVm0ThenConnect(null);

    final File diskStoreDir = new File(new File(".").getAbsolutePath(), "DiskStoreCommandDUnitDiskStores");
    diskStoreDir.mkdir();
    this.filesToBeDeleted.add(diskStoreDir.getAbsolutePath());

    final String diskStoreName1 = "DiskStore1";
    final String region1 = "Region1";
    final String region2 = "Region2";

    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        final Cache cache = getCache();

        DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
        diskStoreFactory.setDiskDirs(new File[] { diskStoreDir });
        final DiskStore diskStore1 = diskStoreFactory.create(diskStoreName1);
        assertNotNull(diskStore1);

        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        regionFactory.setDiskStoreName(diskStoreName1);
        regionFactory.setDiskSynchronous(true);
        regionFactory.create(region1);

        regionFactory.setCompressor(SnappyCompressor.getDefaultInstance());
        regionFactory.create(region2);

        cache.close();
        assertTrue(new File(diskStoreDir, "BACKUP" + diskStoreName1 + ".if").exists());
      }
    });

    CommandResult cmdResult = executeCommand("describe offline-disk-store --name=" + diskStoreName1 + " --disk-dirs=" + diskStoreDir.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, ".*/" + region1 + ": -lru=none -concurrencyLevel=16 -initialCapacity=16 -loadFactor=0.75 -offHeap=false -compressor=none -statisticsEnabled=false"));
    assertTrue(stringContainsLine(stringResult, ".*/" + region2 + ": -lru=none -concurrencyLevel=16 -initialCapacity=16 -loadFactor=0.75 -offHeap=false -compressor=org.apache.geode.compression.SnappyCompressor -statisticsEnabled=false"));

    cmdResult = executeCommand("describe offline-disk-store --name=" + diskStoreName1 + " --disk-dirs=" + diskStoreDir.getAbsolutePath() + " --region=/" + region1);
    stringResult = commandResultToString(cmdResult);
    assertEquals(2, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, ".*/" + region1 + ": .*"));
    assertFalse(stringContainsLine(stringResult, ".*/" + region2 + ": .*"));
  }

  @Test
  public void testOfflineDiskStorePdxCommands() {
    final Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(START_LOCATOR, "localhost[" + AvailablePortHelper.getRandomAvailableTCPPort() + "]");

    final File diskStoreDir = new File(new File(".").getAbsolutePath(), "DiskStoreCommandDUnitDiskStores");
    diskStoreDir.mkdir();
    this.filesToBeDeleted.add(diskStoreDir.getAbsolutePath());

    final String diskStoreName1 = "DiskStore1";
    final String region1 = "Region1";

    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        final Cache cache = new CacheFactory(props).setPdxPersistent(true).setPdxDiskStore(diskStoreName1).create();

        DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
        diskStoreFactory.setDiskDirs(new File[] { diskStoreDir });
        final DiskStore diskStore1 = diskStoreFactory.create(diskStoreName1);
        assertNotNull(diskStore1);

        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        regionFactory.setDiskStoreName(diskStoreName1);
        regionFactory.setDiskSynchronous(true);
        Region r1 = regionFactory.create(region1);
        r1.put("key-1", new PortfolioPdx(1));

        cache.close();
        assertTrue(new File(diskStoreDir, "BACKUP" + diskStoreName1 + ".if").exists());
      }
    });

    CommandResult cmdResult = executeCommand("describe offline-disk-store --name=" + diskStoreName1 + " --disk-dirs=" + diskStoreDir.getAbsolutePath() + " --pdx=true");
    String stringResult = commandResultToString(cmdResult);
    assertTrue(stringContainsLine(stringResult, ".*PDX Types.*"));
    assertTrue(stringContainsLine(stringResult, ".*org\\.apache\\.geode\\.cache\\.query\\.data\\.PortfolioPdx.*"));
    assertTrue(stringContainsLine(stringResult, ".*org\\.apache\\.geode\\.cache\\.query\\.data\\.PositionPdx.*"));
    assertTrue(stringContainsLine(stringResult, ".*PDX Enums.*"));
    assertTrue(stringContainsLine(stringResult, ".*org\\.apache\\.geode\\.cache\\.query\\.data\\.PortfolioPdx\\$Day.*"));
  }

  @Test
  public void testValidateDiskStore() {
    setUpJmxManagerOnVm0ThenConnect(null);

    final File diskStoreDir = new File(new File(".").getAbsolutePath(), "DiskStoreCommandDUnitDiskStores");
    diskStoreDir.mkdir();
    this.filesToBeDeleted.add(diskStoreDir.getAbsolutePath());

    final String diskStoreName1 = "DiskStore1";
    final String region1 = "Region1";
    final String region2 = "Region2";

    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        final Cache cache = getCache();

        DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
        diskStoreFactory.setDiskDirs(new File[] { diskStoreDir });
        final DiskStore diskStore1 = diskStoreFactory.create(diskStoreName1);
        assertNotNull(diskStore1);

        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        regionFactory.setDiskStoreName(diskStoreName1);
        regionFactory.setDiskSynchronous(true);
        regionFactory.create(region1);
        regionFactory.create(region2);

        cache.close();
        assertTrue(new File(diskStoreDir, "BACKUP" + diskStoreName1 + ".if").exists());
      }
    });
    String command = "validate offline-disk-store --name=" + diskStoreName1 + " --disk-dirs=" + diskStoreDir.getAbsolutePath();
    getLogWriter().info("testValidateDiskStore command: " + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testValidateDiskStore cmdResult is stringResult " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(stringResult.contains("Total number of region entries in this disk store is"));

    } else {
      getLogWriter().info("testValidateDiskStore cmdResult is null");
      fail("Did not get CommandResult in testValidateDiskStore");
    }
  }

  @Test
  public void testExportOfflineDiskStore() throws Exception {
    setUpJmxManagerOnVm0ThenConnect(null);

    final File diskStoreDir = new File(new File(".").getAbsolutePath(), "DiskStoreCommandDUnitDiskStores");
    diskStoreDir.mkdir();
    this.filesToBeDeleted.add(diskStoreDir.getAbsolutePath());
    final File exportDir = new File(new File(".").getAbsolutePath(), "DiskStoreCommandDUnitExport");
    exportDir.mkdir();
    this.filesToBeDeleted.add(exportDir.getAbsolutePath());

    final String diskStoreName1 = "DiskStore1";
    final String region1 = "Region1";
    final String region2 = "Region2";
    final Map<String, String> entries = new HashMap<String, String>();
    entries.put("key1", "value1");
    entries.put("key2", "value2");

    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        final Cache cache = getCache();

        DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
        diskStoreFactory.setDiskDirs(new File[] { diskStoreDir });
        final DiskStore diskStore1 = diskStoreFactory.create(diskStoreName1);
        assertNotNull(diskStore1);

        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        regionFactory.setDiskStoreName(diskStoreName1);
        regionFactory.setDiskSynchronous(true);
        Region r1 = regionFactory.create(region1);
        r1.putAll(entries);
        Region r2 = regionFactory.create(region2);
        r2.putAll(entries);

        cache.close();
        assertTrue(new File(diskStoreDir, "BACKUP" + diskStoreName1 + ".if").exists());
      }
    });
    String command = "export offline-disk-store --name=" + diskStoreName1 + " --disk-dirs=" + diskStoreDir.getAbsolutePath() + " --dir=" + exportDir;
    getLogWriter().info("testExportDiskStore command" + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      String stringResult = commandResultToString(cmdResult);
      SnapshotTestUtil.checkSnapshotEntries(exportDir, entries, diskStoreName1, region1);
      SnapshotTestUtil.checkSnapshotEntries(exportDir, entries, diskStoreName1, region2);

    } else {
      getLogWriter().info("testExportOfflineDiskStore cmdResult is null");
      fail("Did not get CommandResult in testExportOfflineDiskStore");
    }
  }

  /**
   * Asserts that creating and destroying disk stores correctly updates the shared configuration.
   */
  @Test
  public void testCreateDestroyUpdatesSharedConfig() {
    disconnectAllFromDS();

    final String groupName = "testDiskStoreSharedConfigGroup";
    final String diskStoreName = "testDiskStoreSharedConfigDiskStore";

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {

        final File locatorLogFile = new File("locator-" + locatorPort + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(NAME, "Locator");
        locatorProps.setProperty(MCAST_PORT, "0");
        locatorProps.setProperty(LOG_LEVEL, "fine");
        locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locatorPort, locatorLogFile, null, locatorProps);

          WaitCriterion wc = new WaitCriterion() {
            @Override
            public boolean done() {
              return locator.isSharedConfigurationRunning();
            }

            @Override
            public String description() {
              return "Waiting for shared configuration to be started";
            }
          };
          waitForCriterion(wc, 5000, 500, true);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }
      }
    });

    // Start the default manager
    Properties managerProps = new Properties();
    managerProps.setProperty(MCAST_PORT, "0");
    managerProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    setUpJmxManagerOnVm0ThenConnect(managerProps);

    // Create a cache in VM 1
    final File diskStoreDir = new File(new File(".").getAbsolutePath(), diskStoreName);
    this.filesToBeDeleted.add(diskStoreDir.getAbsolutePath());
    VM vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        diskStoreDir.mkdirs();

        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(GROUPS, groupName);
        getSystem(localProps);
        assertNotNull(getCache());
      }
    });

    // Test creating the disk store
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStoreName);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__GROUP, groupName);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskStoreDir.getAbsolutePath());
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the disk store exists in the shared config
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        String xmlFromConfig;
        try {
          xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
          assertTrue(xmlFromConfig.contains(diskStoreName));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });

    //Restart the cache and make sure it has the diskstore
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        getCache().close();
        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort+"]");
        localProps.setProperty(GROUPS, groupName);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);

        GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
        Collection<DiskStoreImpl> diskStoreList = gfc.listDiskStores();
        assertNotNull(diskStoreList);
        assertFalse(diskStoreList.isEmpty());
        assertTrue(diskStoreList.size() == 1);

        for (DiskStoreImpl diskStore : diskStoreList) {
          assertTrue(diskStore.getName().equals(diskStoreName));
          break;
        }
        return null;
      }
    });

    // Test destroying the disk store
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStoreName);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__GROUP, groupName);
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the disk store was removed from the shared config
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        String xmlFromConfig;
        try {
          xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
          assertFalse(xmlFromConfig.contains(diskStoreName));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });


    //Restart the cache and make sure it DOES NOT have the diskstore
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        getCache().close();
        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort+"]");
        localProps.setProperty(GROUPS, groupName);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);
        GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
        Collection<DiskStoreImpl> diskStores = gfc.listDiskStores();
        assertNotNull(diskStores);
        assertTrue(diskStores.isEmpty());
        return null;
      }
    });
  }


  /**
   * 1) Create a disk-store in a member, get the disk-dirs. 2) Close the member. 3) Execute the command. 4) Restart the
   * member. 5) Check if the disk-store is altered.
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testAlterDiskStore() throws ClassNotFoundException, IOException {
    final String regionName = "region1";
    final String diskStoreName = "disk-store1";
    final String diskDirName = "diskStoreDir";
    final File diskStoreDir = new File(diskDirName);
    diskStoreDir.deleteOnExit();

    if (!diskStoreDir.exists()) {
      diskStoreDir.mkdir();
    }

    final String diskDirPath = diskStoreDir.getCanonicalPath();
    final VM vm1 = Host.getHost(0).getVM(1);


    vm1.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        getSystem();
        Region region = createParRegWithPersistence(regionName, diskStoreName, diskDirPath);
        region.put("a", "QWE");
        return region.put("b", "ASD");
      }
    });
    //Close the cache and all the connections , so the disk-store can be altered
    disconnectAllFromDS();

    //Now do the command execution
    setUpJmxManagerOnVm0ThenConnect(null);
    Gfsh gfshInstance = Gfsh.getCurrentInstance();

    if (gfshInstance == null) {
      fail("In testAlterDiskStore command gfshInstance is null");
    }

    gfshInstance.setDebug(true);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, diskStoreName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, regionName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, diskDirPath);
    csb.addOption(CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL, "5");
    csb.addOption(CliStrings.ALTER_DISK_STORE__INITIAL__CAPACITY, "6");
    csb.addOption(CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ACTION, "local-destroy");
    csb.addOption(CliStrings.ALTER_DISK_STORE__COMPRESSOR, "org.apache.geode.compression.SnappyCompressor");
    csb.addOption(CliStrings.ALTER_DISK_STORE__STATISTICS__ENABLED, "true");

    String commandString = csb.getCommandString();

    commandString.trim();

    CommandResult cmdResult = executeCommand(commandString);
    String resultString = commandResultToString(cmdResult);
    getLogWriter().info("#SB command output : \n" + resultString);
    assertEquals(true, Result.Status.OK.equals(cmdResult.getStatus()));
    assertEquals(true, resultString.contains("concurrencyLevel=5"));
    assertEquals(true, resultString.contains("lruAction=local-destroy"));
    assertEquals(true, resultString.contains("compressor=org.apache.geode.compression.SnappyCompressor"));
    assertEquals(true, resultString.contains("initialCapacity=6"));

    csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, diskStoreName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, regionName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, diskDirPath);
    csb.addOption(CliStrings.ALTER_DISK_STORE__COMPRESSOR, "none");

    cmdResult = executeCommand(csb.getCommandString().trim());
    resultString = commandResultToString(cmdResult);
    assertEquals(true, Result.Status.OK.equals(cmdResult.getStatus()));
    assertTrue(stringContainsLine(resultString, "-compressor=none"));

    //Alter DiskStore with remove option
    csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, diskStoreName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, regionName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, diskDirPath);
    csb.addOption(CliStrings.ALTER_DISK_STORE__REMOVE, "true");

    commandString = csb.getCommandString();

    commandString.trim();

    cmdResult = executeCommand(commandString);
    resultString = commandResultToString(cmdResult);
    getLogWriter().info("command output : \n" + resultString);
    assertEquals(true, Result.Status.OK.equals(cmdResult.getStatus()));

    Object postDestroyValue = vm1.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        getSystem();
        Region region = createParRegWithPersistence(regionName, diskStoreName, diskDirPath);
        return region.get("a");
      }
    });
    assertNull(postDestroyValue);

    csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, diskStoreName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, regionName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, diskDirPath);
    csb.addOption(CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL, "5");
    csb.addOption(CliStrings.ALTER_DISK_STORE__REMOVE, "true");


    commandString = csb.getCommandString();
    commandString.trim();

    cmdResult = executeCommand(commandString);
    resultString = commandResultToString(cmdResult);
    getLogWriter().info("Alter DiskStore with wrong remove option  : \n" + resultString);
    assertEquals(true, Result.Status.ERROR.equals(cmdResult.getStatus()));

    filesToBeDeleted.add(diskDirName);
  }

  @Test
  public void testBackupDiskStoreBackup() throws IOException {
    final String regionName = "region1";
    final String fullBackUpName = "fullBackUp";
    final String controllerName = "controller";
    final String vm1Name = "vm1";
    final String diskStoreName = "diskStore";
    final String controllerDiskDirName = "controllerDiskDir";
    final String vm1DiskDirName = "vm1DiskDir";
    final String incrementalBackUpName = "incrementalBackUp";
    final VM manager = Host.getHost(0).getVM(0);
    final VM vm1 = Host.getHost(0).getVM(1);
    setUpJmxManagerOnVm0ThenConnect(null);


    File controllerDiskDir = new File(controllerDiskDirName);
    controllerDiskDir.mkdir();
    final String controllerDiskDirPath = controllerDiskDir.getCanonicalPath();
    filesToBeDeleted.add(controllerDiskDirPath);

    File vm1DiskDir = new File(vm1DiskDirName);
    vm1DiskDir.mkdir();
    final String vm1DiskDirPath = vm1DiskDir.getCanonicalPath();
    filesToBeDeleted.add(vm1DiskDirPath);

    File fullBackupDir = new File(fullBackUpName);
    fullBackupDir.mkdir();
    final String fullBackupDirPath = fullBackupDir.getCanonicalPath();
    filesToBeDeleted.add(fullBackupDirPath);

    Properties props = new Properties();
    props.setProperty(NAME, controllerName);

    getSystem(props);

    manager.invoke(new SerializableRunnable() {
      public void run() {
        Region region = createParRegWithPersistence(regionName, diskStoreName, controllerDiskDirPath);
        region.put("A", "1");
        region.put("B", "2");
      }
    });

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm1Name);
        getSystem(localProps);

        Cache cache = getCache();
        Region region = createParRegWithPersistence(regionName, diskStoreName, vm1DiskDirPath);
      }
    });


    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.BACKUP_DISK_STORE);
    csb.addOption(CliStrings.BACKUP_DISK_STORE__DISKDIRS, fullBackupDirPath);
    String commandString = csb.toString();

    CommandResult cmdResult = executeCommand(commandString);
    String resultAsString = commandResultToString(cmdResult);
    getLogWriter().info("Result from full backup : \n" + resultAsString);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertEquals(true, resultAsString.contains("Manager"));
    assertEquals(true, resultAsString.contains(vm1Name));


    vm1.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        Region region = getCache().getRegion(regionName);
        //Add some data to the region
        region.put("F", "231");
        region.put("D", "ew");
      }
    });

    File incrementalBackUpDir = new File(incrementalBackUpName);
    incrementalBackUpDir.mkdir();

    //Perform an incremental backup
    final String incrementalBackUpDirPath = incrementalBackUpDir.getCanonicalPath();
    filesToBeDeleted.add(incrementalBackUpDirPath);

    csb = new CommandStringBuilder(CliStrings.BACKUP_DISK_STORE);
    csb.addOption(CliStrings.BACKUP_DISK_STORE__DISKDIRS, incrementalBackUpDirPath);
    csb.addOption(CliStrings.BACKUP_DISK_STORE__BASELINEDIR, fullBackupDirPath);

    cmdResult = executeCommand(csb.toString());
    resultAsString = commandResultToString(cmdResult);
    getLogWriter().info("Result from incremental backup : \n" + resultAsString);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertEquals(true, resultAsString.contains("Manager"));
    assertEquals(true, resultAsString.contains(vm1Name));
  }

  @Category(FlakyTest.class) // GEODE-1206: random ports, BindException
  @Test
  public void testCreateDiskStore() {
    final String diskStore1Name = "testCreateDiskStore1";
    final String diskStore2Name = "testCreateDiskStore2";

    Properties localProps = new Properties();
    localProps.setProperty(GROUPS, "Group0");
    setUpJmxManagerOnVm0ThenConnect(localProps);

    CommandResult cmdResult = executeCommand(CliStrings.LIST_DISK_STORE);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No Disk Stores Found"));

    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "VM" + vm1.getPid();
    final File diskStore1Dir1 = new File(new File(".").getAbsolutePath(), diskStore1Name + ".1");
    this.filesToBeDeleted.add(diskStore1Dir1.getAbsolutePath());
    final File diskStore1Dir2 = new File(new File(".").getAbsolutePath(), diskStore1Name + ".2");
    this.filesToBeDeleted.add(diskStore1Dir2.getAbsolutePath());
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        diskStore1Dir1.mkdirs();
        diskStore1Dir2.mkdirs();

        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm1Name);
        localProps.setProperty(GROUPS, "Group1");
        getSystem(localProps);
        getCache();
      }
    });

    final VM vm2 = Host.getHost(0).getVM(2);
    final String vm2Name = "VM" + vm2.getPid();
    final File diskStore2Dir = new File(new File(".").getAbsolutePath(), diskStore2Name);
    this.filesToBeDeleted.add(diskStore2Dir.getAbsolutePath());
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        diskStore2Dir.mkdirs();

        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm2Name);
        localProps.setProperty(GROUPS, "Group2");
        getSystem(localProps);
        getCache();
      }
    });

    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStore1Name);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION, "true");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__AUTO_COMPACT, "false");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD, "67");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE, "355");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__QUEUE_SIZE, "5321");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__TIME_INTERVAL, "2023");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE, "3110");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskStore1Dir1.getAbsolutePath() + "#1452637463");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskStore1Dir2.getAbsolutePath());
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Success"));

    // Verify that the disk store was created on the correct member
    cmdResult = executeCommand(CliStrings.LIST_DISK_STORE);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*" + diskStore1Name + " .*"));
    assertFalse(stringContainsLine(stringResult, vm2Name + ".*" + diskStore1Name + " .*"));

    // Verify that the disk store files were created in the correct directory.
    assertEquals(diskStore1Dir1.listFiles().length, 2);

    // Verify that all of the attributes of the disk store were set correctly.
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESCRIBE_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.DESCRIBE_DISK_STORE__MEMBER, vm1Name);
    commandStringBuilder.addOption(CliStrings.DESCRIBE_DISK_STORE__NAME, diskStore1Name);
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertTrue(stringContainsLine(stringResult, "Allow Force Compaction.*Yes"));
    assertTrue(stringContainsLine(stringResult, "Auto Compaction.*No"));
    assertTrue(stringContainsLine(stringResult, "Compaction Threshold.*67"));
    assertTrue(stringContainsLine(stringResult, "Max Oplog Size.*355"));
    assertTrue(stringContainsLine(stringResult, "Queue Size.*5321"));
    assertTrue(stringContainsLine(stringResult, "Time Interval.*2023"));
    assertTrue(stringContainsLine(stringResult, "Write Buffer Size.*3110"));
    assertTrue(stringContainsLine(stringResult, ".*" + diskStore1Name + ".1 .*1452637463"));
    assertTrue(stringContainsLine(stringResult, ".*" + diskStore1Name + ".2 .*" + Integer.MAX_VALUE));

    commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStore2Name);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__GROUP, "Group2");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskStore2Dir.getAbsolutePath());
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*Success"));

    // Verify that the second disk store was created correctly.
    cmdResult = executeCommand(CliStrings.LIST_DISK_STORE);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*" + diskStore1Name + " .*"));
    assertFalse(stringContainsLine(stringResult, vm2Name + ".*" + diskStore1Name + " .*"));
    assertFalse(stringContainsLine(stringResult, vm1Name + ".*" + diskStore2Name + " .*"));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + diskStore2Name + " .*"));
  }

  @Test
  public void testDestroyDiskStore() {
    final String diskStore1Name = "testDestroyDiskStore1";
    final String diskStore2Name = "testDestroyDiskStore2";
    final String region1Name = "testDestroyDiskStoreRegion1";
    final String region2Name = "testDestroyDiskStoreRegion2";

    Properties localProps = new Properties();
    localProps.setProperty(GROUPS, "Group0");
    setUpJmxManagerOnVm0ThenConnect(localProps);

    CommandResult cmdResult = executeCommand(CliStrings.LIST_DISK_STORE);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No Disk Stores Found"));

    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "VM" + vm1.getPid();
    final File diskStore1Dir1 = new File(new File(".").getAbsolutePath(), diskStore1Name + ".1");
    this.filesToBeDeleted.add(diskStore1Dir1.getAbsolutePath());
    final File diskStore2Dir1 = new File(new File(".").getAbsolutePath(), diskStore2Name + ".1");
    this.filesToBeDeleted.add(diskStore2Dir1.getAbsolutePath());
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        diskStore1Dir1.mkdirs();
        diskStore2Dir1.mkdirs();

        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm1Name);
        localProps.setProperty(GROUPS, "Group1,Group2");
        getSystem(localProps);
        Cache cache = getCache();

        DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
        diskStoreFactory.setDiskDirs(new File[] { diskStore1Dir1 });
        diskStoreFactory.create(diskStore1Name);

        diskStoreFactory.setDiskDirs(new File[] { diskStore2Dir1 });
        diskStoreFactory.create(diskStore2Name);
      }
    });

    final VM vm2 = Host.getHost(0).getVM(2);
    final String vm2Name = "VM" + vm2.getPid();
    final File diskStore1Dir2 = new File(new File(".").getAbsolutePath(), diskStore1Name + ".2");
    this.filesToBeDeleted.add(diskStore1Dir2.getAbsolutePath());
    final File diskStore2Dir2 = new File(new File(".").getAbsolutePath(), diskStore2Name + ".2");
    this.filesToBeDeleted.add(diskStore2Dir2.getAbsolutePath());
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        diskStore1Dir2.mkdirs();
        diskStore2Dir2.mkdirs();

        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm2Name);
        localProps.setProperty(GROUPS, "Group2");
        getSystem(localProps);
        Cache cache = getCache();

        DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
        diskStoreFactory.setDiskDirs(new File[] { diskStore1Dir2 });
        diskStoreFactory.create(diskStore1Name);

        RegionFactory regionFactory = cache.createRegionFactory();
        regionFactory.setDiskStoreName(diskStore1Name);
        regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        regionFactory.create(region1Name);
        regionFactory.create(region2Name);

        diskStoreFactory.setDiskDirs(new File[] { diskStore2Dir2 });
        diskStoreFactory.create(diskStore2Name);
      }
    });

    // TEST DELETING ON 1 MEMBER

    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStore1Name);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__GROUP, "Group1");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Success"));

    // Verify that the disk store was destroyed on the correct member
    cmdResult = executeCommand(CliStrings.LIST_DISK_STORE);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(5, countLinesInString(stringResult, false));
    assertFalse(stringContainsLine(stringResult, vm1Name + ".*" + diskStore1Name + " .*"));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + diskStore1Name + " .*"));

    // Verify that the disk store files were deleted from the correct directory.
    assertEquals(0, diskStore1Dir1.listFiles().length);
    assertEquals(4, diskStore1Dir2.listFiles().length);

    // TEST DELETING ON 2 MEMBERS

    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStore2Name);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__GROUP, "Group2");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Success"));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*Success"));

    // Verify that the disk store was destroyed on the correct member
    cmdResult = executeCommand(CliStrings.LIST_DISK_STORE);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertFalse(stringContainsLine(stringResult, vm1Name + ".*" + diskStore2Name + " .*"));
    assertFalse(stringContainsLine(stringResult, vm2Name + ".*" + diskStore2Name + " .*"));

    // Verify that the disk store files were deleted from the correct directories.
    assertEquals(0, diskStore2Dir1.listFiles().length);
    assertEquals(0, diskStore2Dir2.listFiles().length);

    // TEST FOR DISK STORE IN USE

    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStore1Name);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__GROUP, "Group2");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Disk store not found on this member"));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + region1Name + ".*" + region2Name + ".*"));

    // TEST DELETING ON ALL MEMBERS

    vm2.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();

        Region region = cache.getRegion(region1Name);
        region.destroyRegion();

        region = cache.getRegion(region2Name);
        region.destroyRegion();
      }
    });

    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStore1Name);
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(5, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, "Manager.*Disk store not found on this member"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Disk store not found on this member"));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*Success"));

    // Verify that there are no disk stores left.
    cmdResult = executeCommand(CliStrings.LIST_DISK_STORE);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No Disk Stores Found"));

    // Verify that the disk store files were deleted from the correct directory.
    assertEquals(0, diskStore1Dir2.listFiles().length);
  }

  private Region<?, ?> createParRegWithPersistence(String regionName, String diskStoreName, String diskDirName) {
    Cache cache = getCache();
    File diskStoreDirFile = new File(diskDirName);

    if (!diskStoreDirFile.exists()) {
      diskStoreDirFile.mkdirs();
    }

    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] { diskStoreDirFile });
    diskStoreFactory.setMaxOplogSize(1);
    diskStoreFactory.setAllowForceCompaction(true);
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.create(diskStoreName);

    /****
     * Eviction Attributes
     */
    EvictionAttributes ea = EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK);

    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);
    regionFactory.setEvictionAttributes(ea);

    return regionFactory.create(regionName);
  }

  @Override
  protected final void preTearDownCliCommandTestBase() throws Exception {
    try {
      deleteFiles();
    } catch (IOException ex) {
      // This sometimes throws a DirectoryNotEmptyException. The only reason I can see for this is that additional
      // files are being written into the directory while it is being deleted (recursively). So let's just try one more
      // time.
      try {
        deleteFiles();
      } catch (IOException e) {
        getLogWriter().error("Unable to delete file", e);
      }
    }
    this.filesToBeDeleted.clear();
  }

  private void deleteFiles() throws IOException {
    for (String path : this.filesToBeDeleted) {
      FileUtil.delete(new File(path));
    }

  }
}
