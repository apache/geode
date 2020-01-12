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

import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.PartitionedRegionDataStore.BucketVisitor;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class PRCustomPartitioningDistributedTest implements Serializable {

  private static final int TOTAL_NUM_BUCKETS = 7;

  private List<Date> listOfKeysInVM1;
  private List<Date> listOfKeysInVM2;
  private List<Date> listOfKeysInVM3;
  private List<Date> listOfKeysInVM4;

  private String regionName;

  private VM datastoreVM0;
  private VM datastoreVM1;
  private VM datastoreVM2;
  private VM accessorVM3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() {
    datastoreVM0 = getVM(0);
    datastoreVM1 = getVM(1);
    datastoreVM2 = getVM(2);
    accessorVM3 = getVM(3);

    regionName = "PR1";

    listOfKeysInVM1 = new ArrayList<>();
    listOfKeysInVM2 = new ArrayList<>();
    listOfKeysInVM3 = new ArrayList<>();
    listOfKeysInVM4 = new ArrayList<>();
  }

  @Test
  public void testPartitionedRegionOperationsCustomPartitioning() {
    datastoreVM0.invoke(() -> createPartitionedRegionWithPartitionResolver());
    datastoreVM1.invoke(() -> createPartitionedRegionWithPartitionResolver());
    datastoreVM2.invoke(() -> createPartitionedRegionWithPartitionResolver());
    accessorVM3.invoke(() -> createPartitionedRegionAccessorWithPartitionResolver());

    datastoreVM0.invoke(() -> doPutOperations(regionName, 2100, listOfKeysInVM1));
    datastoreVM1.invoke(() -> doPutOperations(regionName, 2200, listOfKeysInVM2));
    datastoreVM2.invoke(() -> doPutOperations(regionName, 2300, listOfKeysInVM3));
    accessorVM3.invoke(() -> doPutOperations(regionName, 2400, listOfKeysInVM4));

    datastoreVM0.invoke(() -> verifyKeys(regionName, listOfKeysInVM1));
    datastoreVM1.invoke(() -> verifyKeys(regionName, listOfKeysInVM2));
    datastoreVM2.invoke(() -> verifyKeys(regionName, listOfKeysInVM3));
    accessorVM3.invoke(() -> verifyKeysInAccessor(regionName, listOfKeysInVM4));
  }

  private void doPutOperations(final String regionName, final int century,
      final List<Date> listOfKeys) {
    Calendar calendar = Calendar.getInstance();
    Region<Date, Integer> region = cacheRule.getCache().getRegion(regionName);
    for (int month = 0; month <= 11; month++) {
      int year = (int) (Math.random() * century);
      int date = (int) (Math.random() * 30);

      calendar.set(year, month, date);
      Date key = calendar.getTime();
      listOfKeys.add(key);

      region.put(key, month);
    }
  }

  private void verifyKeysInAccessor(final String regionName, final List<Date> listOfKeys) {
    PartitionedRegion partitionedRegion =
        (PartitionedRegion) cacheRule.getCache().getRegion(regionName);

    for (Object key : listOfKeys) {
      assertThat(containsKeyInSomeBucket(partitionedRegion, (Date) key)).isTrue();
    }

    assertThat(partitionedRegion.getDataStore()).isNull();
  }

  private void verifyKeys(final String regionName, final List<Date> listOfKeys) {
    PartitionedRegion partitionedRegion =
        (PartitionedRegion) cacheRule.getCache().getRegion(regionName);

    for (Date key : listOfKeys) {
      assertThat(containsKeyInSomeBucket(partitionedRegion, key)).isTrue();
    }

    partitionedRegion.getDataStore().visitBuckets(new BucketVisitor() {
      @Override
      public void visit(Integer bucketId, Region region) {
        Set<Object> bucketKeys = partitionedRegion.getBucketKeys(bucketId);
        for (Object key : bucketKeys) {
          EntryOperation<Date, Integer> entryOperation =
              new EntryOperationImpl(partitionedRegion, null, key, null, null);
          PartitionResolver<Date, Integer> partitionResolver =
              partitionedRegion.getPartitionResolver();
          Object routingObject = partitionResolver.getRoutingObject(entryOperation);
          int routingObjectHashCode = routingObject.hashCode() % TOTAL_NUM_BUCKETS;
          assertThat(routingObjectHashCode).isEqualTo(bucketId);
        }
      }
    });
  }

  private void createPartitionedRegionWithPartitionResolver() {
    PartitionAttributesFactory<Date, Integer> paf = new PartitionAttributesFactory<>();
    paf.setPartitionResolver(new MonthBasedPartitionResolver());
    paf.setRedundantCopies(0);
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    RegionFactory<Date, Integer> regionFactory =
        cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);
  }

  private void createPartitionedRegionAccessorWithPartitionResolver() {
    PartitionAttributesFactory<Date, Integer> paf = new PartitionAttributesFactory<>();
    paf.setLocalMaxMemory(0);
    paf.setPartitionResolver(new MonthBasedPartitionResolver());
    paf.setRedundantCopies(0);
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    RegionFactory<Date, Integer> regionFactory =
        cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);
  }

  /**
   * Returns true if the key is found in any buckets.
   */
  private boolean containsKeyInSomeBucket(PartitionedRegion partitionedRegion, Date key) {
    int numberOfBuckets = partitionedRegion.getTotalNumberOfBuckets();
    for (int bucketId = 0; bucketId < numberOfBuckets; bucketId++) {
      if (partitionedRegion.getBucketKeys(bucketId).contains(key)) {
        return true;
      }
    }
    return false;
  }
}
