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
package org.apache.geode.internal.cache.partitioned.fixed;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.geode.cache.FixedPartitionAttributes.createFixedPartition;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedRule.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedCloseableReference;
import org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule;
import org.apache.geode.test.dunit.rules.DistributedMap;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.PartitioningTest;
import org.apache.geode.test.junit.rules.RandomRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(PartitioningTest.class)
@SuppressWarnings("serial")
public class FixedPartitioningHADistributedTest implements Serializable {

  private static final String REGION_NAME = "theRegion";

  private static final int PARTITIONS = 5;
  private static final int PARTITION_BUCKETS = 10;
  private static final int COUNT = PARTITIONS * PARTITION_BUCKETS;

  private static final int THREADS = 10;

  private static final int LOCAL_MAX_MEMORY = 10;
  private static final int REDUNDANT_COPIES = 2;
  private static final int TOTAL_NUM_BUCKETS = COUNT;

  private static final FixedPartitionAttributes[] PRIMARY = {
      createFixedPartition("Partition-1", true, 10),
      createFixedPartition("Partition-2", true, 10),
      createFixedPartition("Partition-3", true, 10),
      createFixedPartition("Partition-4", true, 10),
      createFixedPartition("Partition-5", true, 10)};

  private static final FixedPartitionAttributes[] SECONDARY = {
      createFixedPartition("Partition-1", false, 10),
      createFixedPartition("Partition-2", false, 10),
      createFixedPartition("Partition-3", false, 10),
      createFixedPartition("Partition-4", false, 10),
      createFixedPartition("Partition-5", false, 10)};

  private static final Map<Integer, PartitionBucket> BUCKET_TO_PARTITION =
      initialize(new HashMap<>());

  private VM accessor1VM;
  private VM server1VM;
  private VM server2VM;
  private VM server3VM;
  private VM server4VM;
  private VM server5VM;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedCloseableReference<ServerLauncher> serverLauncher =
      new DistributedCloseableReference<>();
  @Rule
  public DistributedCloseableReference<AtomicBoolean> doPuts =
      new DistributedCloseableReference<>();
  @Rule
  public DistributedMap<VM, List<FixedPartitionAttributes>> fpaMap = new DistributedMap<>();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  @Rule
  public RandomRule randomRule = new RandomRule();
  @Rule
  public DistributedExecutorServiceRule executorServiceRule = new DistributedExecutorServiceRule();

  @Before
  public void setUp() {
    accessor1VM = getController();
    server1VM = getVM(0);
    server2VM = getVM(1);
    server3VM = getVM(2);
    server4VM = getVM(3);
    server5VM = getVM(4);

    fpaMap.put(accessor1VM, emptyList());
    fpaMap.put(server1VM, asList(PRIMARY[0], SECONDARY[1]));
    fpaMap.put(server2VM, asList(PRIMARY[1], SECONDARY[2]));
    fpaMap.put(server3VM, asList(PRIMARY[2], SECONDARY[3]));
    fpaMap.put(server4VM, asList(PRIMARY[3], SECONDARY[4]));
    fpaMap.put(server5VM, asList(PRIMARY[4], SECONDARY[0]));

    for (VM vm : asList(server1VM, server2VM, server3VM, server4VM, server5VM)) {
      vm.invoke(() -> startServer(vm, "datastore" + vm.getId(), LOCAL_MAX_MEMORY));
    }

    accessor1VM.invoke(() -> startServer(accessor1VM, "accessor1"));

    for (VM vm : asList(accessor1VM, server1VM, server2VM, server3VM, server4VM, server5VM)) {
      vm.invoke(() -> doPuts.set(new AtomicBoolean(true)));
    }

  }

  @Test
  public void recoversAfterBouncingOneDatastore() throws Exception {
    accessor1VM.invoke(() -> {
      Region<Object, Object> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      for (int i = 1; i <= COUNT; i++) {
        region.put(BUCKET_TO_PARTITION.get(i), "value-" + i);
      }

      validateBucketsAreFullyRedundant();
    });

    for (VM vm : asList(server1VM, server2VM, server3VM, server4VM, server5VM)) {
      vm.invoke(() -> dumpBucketMetadata(vm.getId(), "BEFORE"));
    }

    for (VM vm : asList(accessor1VM, server1VM, server2VM, server3VM, server4VM, server5VM)) {
      vm.invoke(() -> {
        Region<Object, Object> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
        for (int i = 0; i < THREADS; i++) {
          executorServiceRule.submit(() -> {
            while (doPuts.get().get()) {
              int bucketId = randomRule.nextInt(1, COUNT);
              region.put(BUCKET_TO_PARTITION.get(bucketId), "value-" + (100 + bucketId));
            }
          });
        }
      });
    }

    for (VM vm : serversToBounce()) {
      vm
          .bounceForcibly()
          .invoke(() -> startServer(vm, "datastore" + vm.getId(), 10));
    }

    Thread.sleep(2000);

    for (VM vm : asList(accessor1VM, server1VM, server2VM, server3VM, server4VM, server5VM)) {
      vm.invoke(() -> doPuts.get().set(false));
    }

    for (VM vm : asList(accessor1VM, server1VM, server2VM, server3VM, server4VM, server5VM)) {
      vm.invoke(() -> {
        await().until(() -> executorServiceRule.getThreads().isEmpty());
      });
    }

    for (VM vm : asList(server1VM, server2VM, server3VM, server4VM, server5VM)) {
      vm.invoke(() -> dumpBucketMetadata(vm.getId(), "AFTER"));
    }

    server2VM.invoke(() -> {
      Region<Object, Object> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      PartitionedRegion partitionedRegion = (PartitionedRegion) region;

      // RegionAdvisor regionAdvisor = partitionedRegion.getRegionAdvisor();
      // assertThat(regionAdvisor.getBucketRedundancy(20)).isEqualTo(REDUNDANT_COPIES);

      PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
      // assertThat(dataStore.getAllLocalBucketIds()).contains(20);
      //
      // assertThat(dataStore.getAllLocalPrimaryBucketIds()).contains(20);
    });
  }

  private void startServer(VM vm, String name) {
    startServer(vm, name, 0);
  }

  private void startServer(VM vm, String name, int localMaxMemory) {
    serverLauncher.set(new ServerLauncher.Builder()
        .set(getDistributedSystemProperties())
        .set(MEMBER_TIMEOUT, "2000")
        .set(SERIALIZABLE_OBJECT_FILTER, getClass().getName() + '*')
        .setDisableDefaultServer(true)
        .setMemberName(name)
        .setWorkingDirectory(folder(name).getAbsolutePath())
        .build())
        .get()
        .start();

    PartitionAttributesFactory<PartitionBucket, Object> partitionAttributesFactory =
        new PartitionAttributesFactory<PartitionBucket, Object>()
            .setLocalMaxMemory(localMaxMemory)
            .setPartitionResolver(new PartitionBucketResolver())
            .setRedundantCopies(REDUNDANT_COPIES)
            .setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    List<FixedPartitionAttributes> fpaList = fpaMap.get(vm);
    if (isNotEmpty(fpaList)) {
      fpaList.forEach(fpa -> partitionAttributesFactory.addFixedPartitionAttributes(fpa));
    }

    serverLauncher.get().getCache()
        .createRegionFactory(PARTITION)
        .setPartitionAttributes(partitionAttributesFactory.create())
        .create(REGION_NAME);
  }

  private void dumpBucketMetadata(int vmId, String when) {
    Region<Object, Object> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;

    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    System.out.println("KIRK:" + when + ": server" + vmId + " contains bucket ids " +
        dataStore.getAllLocalBuckets());
  }

  private List<VM> serversToBounce() {
    List<VM> serversToBounce = new ArrayList<>();
    List<VM> servers = asList(server1VM, server2VM, server3VM, server4VM, server5VM);
    for (int i = 0; i < 2; i++) {
      VM vm = randomRule.next(servers);
      serversToBounce.add(vm);
      servers.remove(vm);
    }
    return serversToBounce;
  }

  private void validateBucketsAreFullyRedundant() {
    Region<Object, Object> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    RegionAdvisor regionAdvisor = partitionedRegion.getRegionAdvisor();

    for (int i = 0; i < TOTAL_NUM_BUCKETS; i++) {
      assertThat(regionAdvisor.getBucketRedundancy(i)).isEqualTo(REDUNDANT_COPIES);
    }
  }

  private File folder(String name) {
    File folder = new File(temporaryFolder.getRoot(), name);
    if (!folder.exists()) {
      assertThat(folder.mkdirs()).isTrue();
    }
    return folder;
  }

  private static Map<Integer, PartitionBucket> initialize(Map<Integer, PartitionBucket> map) {
    for (int i = 1; i <= COUNT; i++) {
      map.put(i, new PartitionBucket(i));
    }
    return map;
  }

  public static class PartitionBucket implements Serializable {

    private final int partitionBucket;
    private final String partitionName;

    private PartitionBucket(int partitionBucket) {
      this.partitionBucket = partitionBucket;
      int partition = (partitionBucket + PARTITION_BUCKETS - 1) / PARTITION_BUCKETS;
      assertThat(partition > 0 && partition < PARTITIONS + 1);
      partitionName = "Partition-" + partition;
    }

    String getPartitionName() {
      return partitionName;
    }

    @Override
    public String toString() {
      return "Partition-" + partitionName + "-Bucket-" + partitionBucket;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PartitionBucket) {
        return partitionBucket == ((PartitionBucket) obj).partitionBucket;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return partitionBucket;
    }
  }

  public static class PartitionBucketResolver
      implements FixedPartitionResolver<PartitionBucket, Object>, Serializable {

    @Override
    public String getName() {
      return getClass().getName();
    }

    @Override
    public String getPartitionName(EntryOperation<PartitionBucket, Object> opDetails,
        Set<String> targetPartitions) {
      return opDetails.getKey().getPartitionName();
    }

    @Override
    public PartitionBucket getRoutingObject(EntryOperation<PartitionBucket, Object> opDetails) {
      return opDetails.getKey();
    }
  }
}
