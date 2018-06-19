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

package org.apache.geode.internal.cache.tx;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class TransactionsIntegrationTest {

  @Rule
  public TestName name = new TestName();

  private Cache cache;

  @Test
  public void testTxOnInvalidatedEntry() throws InterruptedException {
    Cache cache = getCache();
    Region region = createPR(cache);
    region.put(1L, 1L);
    region.invalidate(1L);
    final AtomicBoolean success = new AtomicBoolean(true);

    final CacheTransactionManager mgr = cache.getCacheTransactionManager();
    final CountDownLatch cdl1 = new CountDownLatch(1);
    final CountDownLatch cdl2 = new CountDownLatch(1);
    final CountDownLatch cdl3 = new CountDownLatch(1);

    Thread t1 = new Thread(new Runnable() {
      public void run() {
        mgr.begin();
        TXStateProxy txsp = ((TXManagerImpl) mgr).internalSuspend();
        cdl3.countDown();
        try {
          cdl1.await();
        } catch (Exception e) {
        }
        ((TXManagerImpl) mgr).resume(txsp);
        region.put(1L, 2L);
        mgr.commit();
        cdl2.countDown();
      }
    });
    t1.setDaemon(true);
    t1.start();

    Thread t2 = new Thread(new Runnable() {
      public void run() {
        try {
          cdl3.await();
        } catch (Exception e) {
        }
        mgr.begin();
        try {
          region.create(1L, 3L);
        } catch (EntryExistsException eee) {
          /* expected */
        }
        TXStateProxy txsp = ((TXManagerImpl) mgr).internalSuspend();
        cdl1.countDown();
        try {
          cdl2.await();
        } catch (Exception e) {
        }
        ((TXManagerImpl) mgr).resume(txsp);
        region.invalidate(1L);
        try {
          mgr.commit();
        } catch (Exception e) {
          success.set(false);
        }
      }
    });
    t2.setDaemon(true);
    t2.start();

    t1.join();
    t2.join();

    assertThat(success.get()).isFalse();
    assertThat(region.get(1L)).isEqualTo(2L);
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

  private Region createPR(Cache cache) {
    RegionFactory rf = cache.createRegionFactory();
    rf.setDataPolicy(DataPolicy.PARTITION);
    rf.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(1).create());
    return rf.create(name.getMethodName());
  }
}
