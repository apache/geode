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

package org.apache.geode.internal.cache.lru;

import org.apache.geode.cache.*;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.File;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

/**
 * Test for transactional operations on overflowed data
 */
@Category(IntegrationTest.class)
public class TransactionsWithOverflowTest {

  @Rule
  public TestName name = new TestName();

  private Cache cache;

  private String createDiskStoreAndGetName() {
    Cache cache = getCache();
    File[] diskDirs = new File[1];
    diskDirs[0] = new File("diskRegionDirs/"+getClass().getCanonicalName());
    diskDirs[0].mkdirs();
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(diskDirs);
    String diskStoreName = getClass().getName();
    diskStoreFactory.create(diskStoreName);
    return diskStoreName;
  }

  @Test
  public void testpartitionedRegionWithOverflow() {
    Cache cache = getCache();
    String diskStoreName = createDiskStoreAndGetName();
    Region pr = createOverflowPR(cache, diskStoreName);
    for (int i=0; i<5;i++) {
      pr.put(i, "value"+i);
    }
    CacheTransactionManager mgr = cache.getCacheTransactionManager();
    mgr.begin();
    pr.destroy(1);
    mgr.commit();
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
    rf.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    rf.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(1).create());
    rf.setDiskStoreName(diskStoreName);
    return rf.create(name.getMethodName());
  }
}