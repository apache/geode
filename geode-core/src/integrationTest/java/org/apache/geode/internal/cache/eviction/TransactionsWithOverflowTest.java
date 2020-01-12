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
package org.apache.geode.internal.cache.eviction;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.VMLRURegionMap;
import org.apache.geode.test.junit.rules.DiskDirRule;

/**
 * Test for transactional operations on overflowed data
 */
public class TransactionsWithOverflowTest {

  @Rule
  public DiskDirRule diskDirRule = new DiskDirRule();

  @Rule
  public TestName name = new TestName();

  private Cache cache;

  @Test
  public void testPartitionedRegionWithOverflow() {
    Cache cache = getCache();
    String diskStoreName = createDiskStoreAndGetName();
    Region pr = createOverflowPR(cache, diskStoreName);
    for (int i = 0; i < 5; i++) {
      pr.put(i, "value" + i);
    }
    CacheTransactionManager mgr = cache.getCacheTransactionManager();
    mgr.begin();
    pr.destroy(1);
    mgr.commit();
  }

  @Test
  public void verifyThatTransactionalDestroysRemoveFromTheEvictionList() {
    Cache cache = getCache();
    RegionFactory<?, ?> rf = cache.createRegionFactory();
    rf.setDataPolicy(DataPolicy.REPLICATE);
    rf.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.DEFAULT_EVICTION_ACTION));
    rf.setConcurrencyChecksEnabled(false);
    InternalRegion r = (InternalRegion) rf.create(name.getMethodName());
    CacheTransactionManager mgr = cache.getCacheTransactionManager();
    for (int i = 0; i < 2; i++) {
      r.put(i, "value" + i);
      mgr.begin();
      r.destroy(i);
      mgr.commit();
    }

    VMLRURegionMap regionMap = (VMLRURegionMap) r.getRegionMap();
    assertThat(regionMap.getEvictionList().size()).isEqualTo(0);
  }

  @Test
  public void verifyThatTransactionalDestroysRemoveFromExpiration() {
    Cache cache = getCache();
    RegionFactory<?, ?> rf = cache.createRegionFactory();
    rf.setDataPolicy(DataPolicy.REPLICATE);
    rf.setEntryTimeToLive(new ExpirationAttributes(11, ExpirationAction.DESTROY));
    rf.setConcurrencyChecksEnabled(false);
    InternalRegion r = (InternalRegion) rf.create(name.getMethodName());
    CacheTransactionManager mgr = cache.getCacheTransactionManager();
    for (int i = 0; i < 1; i++) {
      r.put(i, "value" + i);
      mgr.begin();
      r.destroy(i);
      mgr.commit();
    }

    Throwable thrown = catchThrowable(() -> r.getEntryExpiryTask(0));
    assertThat(thrown).isExactlyInstanceOf(EntryNotFoundException.class);
  }

  private String createDiskStoreAndGetName() {
    Cache cache = getCache();
    File[] diskDirs = new File[1];
    diskDirs[0] = new File("diskRegionDirs/" + getClass().getCanonicalName());
    diskDirs[0].mkdirs();
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(diskDirs);
    String diskStoreName = getClass().getName();
    diskStoreFactory.create(diskStoreName);
    return diskStoreName;
  }

  private Cache getCache() {
    if (cache == null) {
      Properties props = new Properties();
      props.setProperty(LOCATORS, "");
      props.setProperty(MCAST_PORT, "0");
      cache = new CacheFactory(props).create();
    }
    return cache;
  }

  private Region createOverflowPR(Cache cache, String diskStoreName) {
    RegionFactory rf = cache.createRegionFactory();
    rf.setDataPolicy(DataPolicy.PARTITION);
    rf.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    rf.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(1).create());
    rf.setDiskStoreName(diskStoreName);
    return rf.create(name.getMethodName());
  }
}
