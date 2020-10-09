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
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.FixedPartitionAttributes.createFixedPartition;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedRule.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
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
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedCloseableReference;
import org.apache.geode.test.dunit.rules.DistributedMap;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.PartitioningTest;
import org.apache.geode.test.junit.rules.RandomRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(PartitioningTest.class)
@SuppressWarnings("serial")
public class FixedPartitioningHADistributedTest implements Serializable {

  private static final String REGION_NAME = "prDS";

  private static final int MEMORY_ACCESSOR = 0;
  private static final int MEMORY_DATASTORE = 10;
  private static final int REDUNDANT_COPIES = 2;
  private static final int TOTAL_NUM_BUCKETS = 8;

  private static final FixedPartitionAttributes Q1_PRIMARY =
      createFixedPartition("Quarter1", true, 1);
  private static final FixedPartitionAttributes Q2_PRIMARY =
      createFixedPartition("Quarter2", true, 3);
  private static final FixedPartitionAttributes Q3_PRIMARY =
      createFixedPartition("Quarter3", true, 1);
  private static final FixedPartitionAttributes Q4_PRIMARY =
      createFixedPartition("Quarter4", true, 3);

  private static final FixedPartitionAttributes Q1_SECONDARY =
      createFixedPartition("Quarter1", false, 1);
  private static final FixedPartitionAttributes Q2_SECONDARY =
      createFixedPartition("Quarter2", false, 3);
  private static final FixedPartitionAttributes Q3_SECONDARY =
      createFixedPartition("Quarter3", false, 1);
  private static final FixedPartitionAttributes Q4_SECONDARY =
      createFixedPartition("Quarter4", false, 3);

  private VM accessor1VM;
  private VM server1VM;
  private VM server2VM;
  private VM server3VM;
  private VM server4VM;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedCloseableReference<ServerLauncher> serverLauncher =
      new DistributedCloseableReference<>();
  @Rule
  public DistributedMap<VM, List<FixedPartitionAttributes>> fpaMap = new DistributedMap<>();
  @Rule
  public RandomRule randomRule = new RandomRule();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    accessor1VM = getController();
    server1VM = getVM(0);
    server2VM = getVM(1);
    server3VM = getVM(2);
    server4VM = getVM(3);

    fpaMap.put(accessor1VM, emptyList());
    fpaMap.put(server1VM, asList(Q1_PRIMARY, Q2_SECONDARY, Q3_SECONDARY));
    fpaMap.put(server2VM, asList(Q2_PRIMARY, Q3_SECONDARY, Q4_SECONDARY));
    fpaMap.put(server3VM, asList(Q3_PRIMARY, Q4_SECONDARY, Q1_SECONDARY));
    fpaMap.put(server4VM, asList(Q4_PRIMARY, Q1_SECONDARY, Q2_SECONDARY));

    for (VM vm : asList(server1VM, server2VM, server3VM, server4VM)) {
      vm.invoke(() -> startServer(vm, ServerType.DATASTORE));
    }

    accessor1VM.invoke(() -> startServer(accessor1VM, ServerType.ACCESSOR));
  }

  @Test
  public void recoversAfterBouncingOneDatastore() {
    accessor1VM.invoke(() -> {
      Region<Object, Object> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      for (int i = 0; i < 12 * 10; i++) {
        int index = i % 12 + 1;
        if (index < 1 || index > 12) {
          throw new Error("index is " + index + " for i " + i);
        }
        Month month = Month.month(index);
        region.put(month, "value" + i);
      }

      assertThat(region.size()).isEqualTo(12);
      validateBucketsAreFullyRedundant();
    });

    VM haServerVM = randomRule.next(server1VM, server2VM, server3VM, server4VM);
    haServerVM.bounceForcibly();

    haServerVM.invoke(() -> {
      startServer(haServerVM, ServerType.DATASTORE);

      serverLauncher.get().getCache().getResourceManager()
          .createRestoreRedundancyOperation()
          .includeRegions(singleton(REGION_NAME))
          .start()
          .get(getTimeout().toMinutes(), MINUTES);

      validateBucketsAreFullyRedundant();
    });

    accessor1VM.invoke(() -> validateBucketsAreFullyRedundant());
  }

  private void startServer(VM vm, ServerType serverType) {
    String name = serverType.name(vm.getId());
    serverLauncher.set(new ServerLauncher.Builder()
        .set(getDistributedSystemProperties())
        .set(SERIALIZABLE_OBJECT_FILTER, getClass().getName() + '*')
        .setDisableDefaultServer(true)
        .setMemberName(name)
        .setWorkingDirectory(folder(name).getAbsolutePath())
        .build());

    serverLauncher.get().start();

    PartitionAttributesFactory<Month, Object> partitionAttributesFactory =
        new PartitionAttributesFactory<Month, Object>()
            .setLocalMaxMemory(serverType.localMaxMemory())
            .setPartitionResolver(new QuarterFixedPartitionResolver())
            .setRedundantCopies(REDUNDANT_COPIES)
            .setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    List<FixedPartitionAttributes> fpaList = fpaMap.get(vm);
    if (CollectionUtils.isNotEmpty(fpaList)) {
      fpaList.forEach(fpa -> partitionAttributesFactory.addFixedPartitionAttributes(fpa));
    }

    serverLauncher.get().getCache()
        .createRegionFactory(PARTITION)
        .setPartitionAttributes(partitionAttributesFactory.create())
        .create(REGION_NAME);
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

  private enum ServerType {
    ACCESSOR(MEMORY_ACCESSOR, id -> "accessor1"),
    DATASTORE(MEMORY_DATASTORE, id -> "datastore" + id);

    private final int localMaxMemory;
    private final Function<Integer, String> nameFunction;

    ServerType(int localMaxMemory, Function<Integer, String> nameFunction) {
      this.localMaxMemory = localMaxMemory;
      this.nameFunction = nameFunction;
    }

    int localMaxMemory() {
      return localMaxMemory;
    }

    String name(int id) {
      return nameFunction.apply(id);
    }
  }

  public static class Month implements Serializable {

    private static final Month JAN = new Month(1);
    private static final Month FEB = new Month(2);
    private static final Month MAR = new Month(3);
    private static final Month APR = new Month(4);
    private static final Month MAY = new Month(5);
    private static final Month JUN = new Month(6);
    private static final Month JUL = new Month(7);
    private static final Month AUG = new Month(8);
    private static final Month SEP = new Month(9);
    private static final Month OCT = new Month(10);
    private static final Month NOV = new Month(11);
    private static final Month DEC = new Month(12);

    private static final Month[] months =
        {JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC};

    public static Month month(int month) {
      return months[month - 1];
    }

    private final int month;
    private final String quarter;

    private Month(int month) {
      this.month = month;
      switch (month) {
        case 1:
        case 2:
        case 3:
          quarter = "Quarter1";
          break;
        case 4:
        case 5:
        case 6:
          quarter = "Quarter2";
          break;
        case 7:
        case 8:
        case 9:
          quarter = "Quarter3";
          break;
        case 10:
        case 11:
        case 12:
          quarter = "Quarter4";
          break;
        default:
          throw new IllegalArgumentException("Invalid month " + month);
      }
    }

    String getQuarter() {
      return quarter;
    }

    @Override
    public String toString() {
      switch (month) {
        case 1:
          return "January";
        case 2:
          return "February";
        case 3:
          return "March";
        case 4:
          return "April";
        case 5:
          return "May";
        case 6:
          return "June";
        case 7:
          return "July";
        case 8:
          return "August";
        case 9:
          return "September";
        case 10:
          return "October";
        case 11:
          return "November";
        case 12:
          return "December";
        default:
          return "No month like this";
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Month) {
        return month == ((Month) obj).month;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return month;
    }
  }

  public static class QuarterFixedPartitionResolver
      implements FixedPartitionResolver<Month, Object>, Serializable {

    @Override
    public String getName() {
      return getClass().getName();
    }

    @Override
    public String getPartitionName(EntryOperation<Month, Object> opDetails,
        Set<String> targetPartitions) {
      return opDetails.getKey().getQuarter();
    }

    @Override
    public Month getRoutingObject(EntryOperation<Month, Object> opDetails) {
      return opDetails.getKey();
    }
  }
}
