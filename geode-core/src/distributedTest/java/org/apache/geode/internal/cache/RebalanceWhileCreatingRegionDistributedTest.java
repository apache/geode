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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.backup.BackupOperation;
import org.apache.geode.internal.cache.partitioned.RemoveBucketMessage;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedBlackboard;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class RebalanceWhileCreatingRegionDistributedTest implements Serializable {

  public static final String DISK_STORE_NAME = "diskStore1";
  public static final int DURATION = 30000;
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedBlackboard blackboard = new DistributedBlackboard();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule(50);

  private static final Logger logger = LogService.getLogger();

  public static final String BEFORE_REMOVE_BUCKET_MESSAGE = "Before_RemoveBucketMessage";

  public static final String AFTER_CREATE_PROXY_REGION = "After_CreateProxyRegion";

  private File backupBaseDir;

  @Test
  public void testOnlineBackup() throws InterruptedException {
    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start servers
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM server3 = cluster.startServerVM(3, locatorPort);
    MemberVM server4 = cluster.startServerVM(3, locatorPort);

    String regionName = testName.getMethodName();

    // Create regions in each server
    server1.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT));
    server2.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT));
    server3.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT));
    server4.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT));

    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> doConcurrentEntryOperations());
    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> doConcurrentEntryOperations());
    AsyncInvocation asyncInvocation3 = server3.invokeAsync(() -> doConcurrentEntryOperations());
    AsyncInvocation asyncInvocation4 = server4.invokeAsync(() -> doOnlineBackup());

    asyncInvocation1.get();
    asyncInvocation2.get();
    asyncInvocation3.get();
    BackupStatus backupStatus = (BackupStatus) asyncInvocation4.get();
    assertThat(backupStatus.getBackedUpDiskStores()).hasSize(4);
    assertThat(backupStatus.getOfflineDiskStores()).isEmpty();
    validateBackupComplete();

    server1.stop();
    server2.stop();
    server3.stop();
    server4.stop();

    // TODO: copy backup files to disk store dirs

    cluster.startServerVM(1, locatorPort);
    cluster.startServerVM(2, locatorPort);
    cluster.startServerVM(3, locatorPort);
    server4 = cluster.startServerVM(3, locatorPort);

    server4.invoke(() -> {
      Region region = getCache().getRegion(regionName);
      verify_bucket_copies(region, 2);
    });
  }

  private void validateBackupComplete() {
    Pattern pattern = Pattern.compile(".*INCOMPLETE.*");
    File[] files = backupBaseDir.listFiles((dir, name) -> pattern.matcher(name).matches());

    assertThat(files).isNotNull().hasSize(0);
  }

  private void doConcurrentEntryOperations() {
    executorServiceRule.submit(() -> {
      long threadId = Thread.currentThread().getId();
      int opId = (int) threadId % 2;
      switch (opId) {
        case 0:
          doPutAll();
          break;
        case 1:
          doDestroy();
          break;
        default: {
          throw new Exception("Unknown operation");
        }
      }
    });
  }

  private void doPutAll() {
    String regionName = testName.getMethodName();
    Region region = getCache().getRegion(regionName);
    Map map = new HashMap<>();
    IntStream.range(0, 10).forEach(i -> map.put(i, i));
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < DURATION) {
      region.putAll(map);
    }
  }

  private void doDestroy() {
    String regionName = testName.getMethodName();
    Region region = getCache().getRegion(regionName);
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < DURATION) {
      IntStream.range(0, 10).forEach(i -> {
        try {
          region.destroy(i);
        } catch (EntryNotFoundException entryNotFoundException) {
          // ignore
        }
      });
    }
  }

  private BackupStatus doOnlineBackup() throws IOException {
    backupBaseDir = temporaryFolder.newFolder("backupDir");
    return new BackupOperation(getCache().getDistributionManager(), getCache())
        .backupAllMembers(backupBaseDir.toString(), null);
  }

  private boolean verify_bucket_copies(Region aRegion, int numExtraCopies) throws Exception {
    PartitionedRegion pr = (PartitionedRegion) aRegion;
    int totalBuckets = pr.getTotalNumberOfBuckets();
    int expectedNumCopies = numExtraCopies + 1;
    int verifyBucketCopiesBucketId = -1;
    StringBuilder verifyBucketCopiesErrStr = new StringBuilder();
    while (true) {
      verifyBucketCopiesBucketId++;
      if (verifyBucketCopiesBucketId >= totalBuckets) {
        break; // we have verified all buckets
      }
      List listOfMaps = null;
      try {
        listOfMaps = pr.getAllBucketEntries(verifyBucketCopiesBucketId);
      } catch (ForceReattemptException e) {
        e.printStackTrace(); // TODO
      }

      // check that we have the correct number of copies of each bucket
      // listOfMaps could be size 0; this means we have no entries in this particular bucket
      int size = listOfMaps.size();
      if (size == 0) {
        continue;
      }
      if (numExtraCopies != -1) {
        if (size != expectedNumCopies) {
          verifyBucketCopiesErrStr
              .append("For bucketId " + verifyBucketCopiesBucketId + ", expected "
                  + expectedNumCopies + " bucket copies, but have " + listOfMaps.size() + "\n");
        }
      } // else we don't know how many copies to expect

      // Check that all copies of the buckets have the same data
      if (listOfMaps.size() > 1) {
        Object firstMap = listOfMaps.get(0);
        for (int j = 1; j < listOfMaps.size(); j++) {
          Object aMap = listOfMaps.get(j);
          verifyBucketCopiesErrStr.append(compareBucketMaps(firstMap, aMap));
        }

      }
    }

    if (verifyBucketCopiesErrStr.length() != 0) {
      throw new Exception(verifyBucketCopiesErrStr.toString());
    }
    return true;
  }

  protected static String compareBucketMaps(Object dump1, Object dump2) {
    BucketDump dump11 = (BucketDump) dump1;
    BucketDump dump21 = (BucketDump) dump2;

    StringBuilder aStr = new StringBuilder();

    compareRVVs(aStr, dump11, dump21);

    Map<Object, Object> map1 = dump11.getValues();
    Map<Object, VersionTag> versions1 = dump11.getVersions();
    String map1LogStr = getBucketMapStr(map1);

    Map<Object, Object> map2 = dump21.getValues();
    Map<Object, VersionTag> versions2 = dump21.getVersions();
    String map2LogStr = getBucketMapStr(map2);

    if (map1.size() != map2.size()) {
      aStr.append("Bucket map <" + map1LogStr + "> is size " + map1.size() + " and bucket map <"
          + map2LogStr + "> is size " + map2.size() + "\n");
    }
    Iterator it = map1.keySet().iterator();
    while (it.hasNext()) {
      Object key = it.next();
      VersionTag version = versions1.get(key);
      Object value = map1.get(key);
      if (map2.containsKey(key)) {
        VersionTag map2Version = versions2.get(key);
        Object map2Value = map2.get(key);
        try {
          verifyValue(key, value, map2Value);
          verifyVersionTags(version, map2Version);
        } catch (Exception e) {
          String version1Str = version == null ? "" : " version " + version;
          String version2Str = map2Version == null ? "" : " version " + map2Version;
          aStr.append("Bucket map <" + map1LogStr + "> has key " + key + ", value "
              + value + version1Str + ", but bucket map <" + map2LogStr
              + "> has key " + key + ", value " + map2Value + version2Str
              + "; " + e.getMessage() + "\n");
        }
      } else {
        aStr.append("Bucket map <" + map1LogStr + "> contains key " + key + ", but bucket map <"
            + map2LogStr + "> does not contain key " + key + "\n");
      }
    }

    // We have verified that every key/value in map1 is also in map2.
    // Now look for any keys in map2 that are not in map1.
    Set map1Keys = map1.keySet();
    Set map2Keys = new HashSet(map2.keySet());
    map2Keys.removeAll(map1Keys);
    if (map2Keys.size() != 0) {
      aStr.append("Found extra keys in bucket map <" + map2LogStr
          + ">, that were not found in bucket map <" + map1LogStr + ">: " + map2Keys + "\n");
    }

    return aStr.toString();
  }


  private static void compareRVVs(StringBuilder aStr, BucketDump dump1, BucketDump dump2) {
    RegionVersionVector rvv1 = dump1.getRvv();
    RegionVersionVector rvv2 = dump2.getRvv();
    if (rvv1 == null) {
      if (rvv2 != null) {
        aStr.append(dump2 + " has an RVV, but " + dump1 + " does not");
      }
    } else {
      if (rvv2 == null) {
        aStr.append(dump1 + " has an RVV, but " + dump2 + " does not");
      } else {
        Map<VersionSource, RegionVersionHolder> rvv2Members =
            new HashMap<VersionSource, RegionVersionHolder>(rvv1.getMemberToVersion());
        Map<VersionSource, RegionVersionHolder> rvv1Members =
            new HashMap<VersionSource, RegionVersionHolder>(rvv1.getMemberToVersion());
        for (Map.Entry<VersionSource, RegionVersionHolder> entry : rvv1Members.entrySet()) {
          VersionSource memberId = entry.getKey();
          RegionVersionHolder versionHolder1 = entry.getValue();
          RegionVersionHolder versionHolder2 = rvv2Members.remove(memberId);
          if (versionHolder2 != null) {
            if (!versionHolder1.equals(versionHolder2)) {
              aStr.append(dump1 + " RVV does not match RVV for " + dump2 + "\n");
              aStr.append("RVV for " + dump1 + ":" + versionHolder1 + "\n");
              aStr.append("RVV for " + dump2 + ":" + versionHolder2 + "\n");
            }

          } else {
            // Don't fail the test if rvv1 has member that were not present in rvv2.
            // It's possible that rvv1 has an old member that rvv1 does not, and rvv1
            // has not GC'd that member from the RVV.
          }

        }

        // Don't fail the test if rvv2 has member that were not present in rvv1.
        // It's possible that rvv2 has an old member that rvv1 does not, and rvv2
        // has not GC'd that member from the RVV.
      }
    }
  }

  private static String getBucketMapStr(Map<Object, Object> bucketMap) {
    String bucketMapStr = bucketMap.toString();
    StringBuilder reducedStr = new StringBuilder();
    int index = bucketMapStr.indexOf("{");
    if (index < 0) {
      return bucketMapStr;
    }
    reducedStr.append(bucketMapStr.substring(0, index));
    index = bucketMapStr.lastIndexOf("}");
    if (index < 0) {
      return bucketMapStr;
    }
    reducedStr.append(bucketMapStr.substring(index + 1, bucketMapStr.length()));
    return reducedStr.toString();
  }

  protected static void verifyValue(Object key, Object obj1, Object obj2) throws Exception {
    if (obj1 != null && obj2 != null) {
      verifyMyValue(key, obj1, obj2);
    } else if (obj1 == null) {
      if (obj2 != null) {
        throw new Exception(obj1 + " is not equal to " + obj2);
      }
    } else if (obj2 == null) {
      if (obj1 != null) {
        throw new Exception(obj2 + " is not equal to " + obj1);
      }
    } else {
      throw new Exception("Something wrong: " + obj1 + " is not equal to " + obj2);
    }
  }

  public static void verifyMyValue(Object key, Object expectedValue, Object valueToCheck)
      throws Exception {
    if (valueToCheck == null) {
      if (expectedValue != null) {
        throw new Exception(
            "For key " + key + ", expected value to be " + expectedValue
                + ", but it is " + valueToCheck);
      }
    } else if (!valueToCheck.equals(expectedValue)) {
      throw new Exception("For key " + key + ", expected value to be "
          + expectedValue + ", but it is "
          + valueToCheck);
    } else {
      throw new Exception("Expected value for key " + key
          + " to be " + expectedValue + ", but it is " + valueToCheck);
    }
  }

  private static void verifyVersionTags(VersionTag version, VersionTag version2) throws Exception {
    boolean equal = version == null && version2 == null
        || version != null & version2 != null && version.equals(version2);
    if (!equal) {
      throw new Exception("Version tag mismatch");
    }
  }

  @Test
  public void testRebalanceDuringRegionCreation() throws Exception {
    // Init Blackboard
    blackboard.initBlackboard();

    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start servers
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM accessor = cluster.startServerVM(4, locatorPort);

    // Add DistributionMessageObserver
    String regionName = testName.getMethodName();
    Stream.of(server1, server2, accessor)
        .forEach(server -> server.invoke(() -> addDistributionMessageObserver(regionName)));

    // Create regions in each server
    server1.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION));
    server2.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION));

    // Asynchronously wait to create the proxy region in the accessor
    AsyncInvocation asyncInvocation =
        accessor.invokeAsync(() -> waitToCreateProxyRegion(regionName));

    // Connect client1
    ClientVM client1 =
        cluster.startClientVM(5, c -> c.withServerConnection(server1.getPort(), server2.getPort()));

    // Do puts
    client1.invoke(() -> {
      Region<Integer, Integer> region =
          ClusterStartupRule.clientCacheRule.createProxyRegion(regionName);
      IntStream.range(0, 3).forEach(i -> region.put(i, i));
    });

    // Start server3
    MemberVM server3 = cluster.startServerVM(3, locatorPort);

    // Create region in server3
    server3.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION));

    // Add DistributionMessageObserver to server3
    server3.invoke(() -> addDistributionMessageObserver(regionName));

    // Rebalance
    server1.invoke(() -> getCache().getResourceManager().createRebalanceFactory()
        .start().getResults());

    // Stop server3
    server3.invoke(() -> getCache().close());

    // Connect client to accessor
    ClientVM client2 =
        cluster.startClientVM(6, c -> c.withServerConnection(accessor.getPort())
            .withCacheSetup(cf -> cf.setPoolReadTimeout(20000)));

    // Do puts
    client2.invoke(() -> {
      Region<Integer, Integer> region =
          ClusterStartupRule.clientCacheRule.createProxyRegion(regionName);
      IntStream.range(0, 3).forEach(i -> region.put(i, i));
    });

    asyncInvocation.get();
    accessor.invoke(() -> {
      Region region = getCache().getRegion(regionName);
      IntStream.range(3, 6).forEach(i -> region.put(i, i));
      assertThat(region.size()).isEqualTo(6);
      IntStream.range(0, 6).forEach(i -> assertThat(region.get(i)).isEqualTo(i));
    });
  }

  @Test
  public void testMoveSingleBucketDuringRegionCreation() throws Exception {
    // Init Blackboard
    blackboard.initBlackboard();

    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start servers
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM accessor = cluster.startServerVM(3, locatorPort);

    // Add DistributionMessageObserver
    String regionName = testName.getMethodName();
    Stream.of(server1, server2, accessor)
        .forEach(server -> server.invoke(() -> addDistributionMessageObserver(regionName)));

    // Create regions in each server
    InternalDistributedMember source = server1.invoke(() -> {
      createSingleBucketRegion(regionName, RegionShortcut.PARTITION);
      Region<Integer, Integer> region =
          getCache().getRegion(regionName);
      region.put(123, 123);
      PartitionedRegionDataStore partitionedRegionDataStore =
          ((PartitionedRegion) region).getDataStore();
      // Make sure server1 has the primary bucket
      assertThat(partitionedRegionDataStore).isNotNull();
      assertThat(partitionedRegionDataStore.getNumberOfPrimaryBucketsManaged()).isEqualTo(1);
      return InternalDistributedSystem.getAnyInstance().getDistributedMember();
    });

    InternalDistributedMember destination = server2.invoke(() -> {
      createSingleBucketRegion(regionName, RegionShortcut.PARTITION);
      Region<Integer, Integer> region =
          getCache().getRegion(regionName);
      PartitionedRegionDataStore partitionedRegionDataStore =
          ((PartitionedRegion) region).getDataStore();
      // Make sure server2 does not have primary bucket
      assertThat(partitionedRegionDataStore).isNotNull();
      assertThat(partitionedRegionDataStore.getNumberOfPrimaryBucketsManaged()).isEqualTo(0);
      return InternalDistributedSystem.getAnyInstance().getDistributedMember();
    });

    // Asynchronously wait to create the proxy region in the accessor
    AsyncInvocation asyncInvocation = accessor.invokeAsync(() -> {
      waitToCreateSingleBucketProxyRegion(regionName);
    });

    // Move the primary bucket from server1 to server2 and close the cache in the end
    server2.invoke(() -> {
      PartitionedRegion partitionedRegion =
          (PartitionedRegion) getCache().getRegion(regionName);
      PartitionedRegionDataStore partitionedRegionDataStore = partitionedRegion.getDataStore();
      // Simulate rebalance operation by calling moveBucket()
      partitionedRegionDataStore.moveBucket(0, source, true);
      getCache().close();
    });

    asyncInvocation.get();

    // Make sure the accessor knows that the primary bucket has moved to server2
    accessor.invoke(() -> {
      PartitionedRegion pr =
          (PartitionedRegion) getCache().getRegion(regionName);
      assertThat(pr.getRegionAdvisor().getBucket(0).getBucketAdvisor().getProfile(source))
          .isNull();
      assertThat(pr.getRegionAdvisor().getBucket(0).getBucketAdvisor().getProfile(destination))
          .isNull();
    });
  }

  private void createRegion(String regionName, RegionShortcut shortcut) throws IOException {
    DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
    diskStoreAttributes.timeInterval = 2000;
    diskStoreAttributes.queueSize = 20;
    DiskStoreFactory diskStoreFactory =
        getCache().createDiskStoreFactory(diskStoreAttributes);
    diskStoreFactory.setDiskDirs(
        new File[] {temporaryFolder.newFolder(DISK_STORE_NAME)});
    diskStoreFactory.create(DISK_STORE_NAME);
    PartitionAttributesFactory<Integer, Integer> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setRedundantCopies(2);
    RegionFactory<Integer, Integer> regionFactory =
        getCache().createRegionFactory(shortcut);
    regionFactory.setDiskStoreName(DISK_STORE_NAME).setDiskSynchronous(false)
        .setPartitionAttributes(partitionAttributesFactory.create());
    regionFactory.create(regionName);
  }

  private void createSingleBucketRegion(String regionName, RegionShortcut shortcut) {
    PartitionAttributesFactory<Integer, Integer> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(0);
    paf.setTotalNumBuckets(1);
    if (shortcut.isProxy()) {
      paf.setLocalMaxMemory(0);
    }

    RegionFactory<Integer, Integer> rf =
        getCache().createRegionFactory(shortcut);
    rf.setPartitionAttributes(paf.create());

    rf.create(regionName);
  }

  private void waitToCreateProxyRegion(String regionName) throws Exception {
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion about to wait for Before_RemoveBucketMessage gate");
    // Wait after RemoveBucketMessage is sent due to rebalance or moveBucket()
    blackboard.waitForGate(BEFORE_REMOVE_BUCKET_MESSAGE);
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion done wait for Before_RemoveBucketMessage gate");
    createRegion(regionName, RegionShortcut.PARTITION_PROXY);
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion about to signal After_CreateProxyRegion gate");
    blackboard.signalGate(AFTER_CREATE_PROXY_REGION);
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion done signal After_CreateProxyRegion gate");
  }

  private void waitToCreateSingleBucketProxyRegion(String regionName) throws Exception {
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion about to wait for Before_RemoveBucketMessage gate");
    // Wait after RemoveBucketMessage is sent due to rebalance or moveBucket()
    blackboard.waitForGate(BEFORE_REMOVE_BUCKET_MESSAGE);
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion done wait for Before_RemoveBucketMessage gate");
    createSingleBucketRegion(regionName, RegionShortcut.PARTITION_PROXY);
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion about to signal After_CreateProxyRegion gate");
    blackboard.signalGate(AFTER_CREATE_PROXY_REGION);
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion done signal After_CreateProxyRegion gate");
  }

  private void addDistributionMessageObserver(String regionName) {
    DistributionMessageObserver.setInstance(new TestDistributionMessageObserver(regionName));
  }

  class TestDistributionMessageObserver extends DistributionMessageObserver {

    private final String regionName;

    public TestDistributionMessageObserver(String regionName) {
      this.regionName = regionName;
    }

    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof RemoveBucketMessage) {
        logger.info(
            "TestDistributionMessageObserver.beforeProcessMessage about to signal Before_RemoveBucketMessage gate");
        // When processing RemoveBucketMessage, it will create DestroyRegionMessage.
        // At this time, the partitioned region has not been created on the accessor.
        // Therefore, DistributionAdvisor doesn't have PartitionProfile from the accessor.
        // If the recipients of DestroyRegionMessage is calculated based on DistributionAdvisor,
        // the accessor will miss DestroyRegionMessage.
        blackboard.signalGate(BEFORE_REMOVE_BUCKET_MESSAGE);
        logger.info(
            "TestDistributionMessageObserver.beforeProcessMessage done signal Before_RemoveBucketMessage gate");
      }
    }

    public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof DestroyRegionOperation.DestroyRegionMessage) {
        DestroyRegionOperation.DestroyRegionMessage drm =
            (DestroyRegionOperation.DestroyRegionMessage) message;
        if (drm.regionPath.contains(regionName)) {
          logger.info(
              "TestDistributionMessageObserver.beforeSendMessage about to wait for After_CreateProxyRegion gate regionName={}",
              drm.regionPath);
          try {
            // When processing RemoveBucketMessage, it will create DestroyRegionMessage.
            // At this time, the partitioned region has not been created on the accessor.
            // Therefore, DistributionAdvisor doesn't have PartitionProfile from the accessor.
            // If the recipients of DestroyRegionMessage is calculated based on DistributionAdvisor,
            // the accessor will miss DestroyRegionMessage.
            // We also don't want to send DestroyRegionMessage too early before the accessor has
            // actually start creating the partitioned region.
            // Otherwise, the accessor will not have the bucket profile to be removed.
            blackboard.waitForGate(AFTER_CREATE_PROXY_REGION);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          logger.info(
              "TestDistributionMessageObserver.beforeSendMessage done wait for After_CreateProxyRegion gate regionName={}",
              drm.regionPath);
        }
      }
    }
  }
}
