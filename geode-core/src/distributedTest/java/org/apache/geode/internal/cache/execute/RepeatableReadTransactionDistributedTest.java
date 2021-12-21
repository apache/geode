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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class RepeatableReadTransactionDistributedTest implements Serializable {
  private String uniqueName;
  private String regionName;
  private VM server1;
  private VM server2;
  private final String key = "key";
  private final String originalValue = "originalValue";
  private final String value1 = "value1";
  private final String value2 = "value2";

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();
  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    server1 = getVM(0);
    server2 = getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
  }

  @Test
  public void valuesRepeatableReadDoesNotIncludeTombstones() {
    server1.invoke(() -> createServerRegion(1, false));
    server2.invoke(() -> createServerRegion(1, true));
    server2.invoke(() -> doDestroyOps());
    server2.invoke(() -> doValuesTransactionWithTombstone());
  }

  @Test
  public void keySetRepeatableReadDoesNotIncludeTombstones() {
    server1.invoke(() -> createServerRegion(1, false));
    server2.invoke(() -> createServerRegion(1, true));
    server2.invoke(() -> doDestroyOps());
    server2.invoke(() -> doKeySetTransactionWithTombstone());
  }

  @Test
  public void valuesRepeatableReadIncludesInvalidates() {
    server1.invoke(() -> createServerRegion(1, false));
    server2.invoke(() -> createServerRegion(1, true));
    server2.invoke(() -> doInvalidateOps());
    server2.invoke(() -> doValuesTransactionWithInvalidate());
  }

  @Test
  public void keySetRepeatableReadIncludesInvalidates() {
    server1.invoke(() -> createServerRegion(1, false));
    server2.invoke(() -> createServerRegion(1, true));
    server2.invoke(() -> doInvalidateOps());
    server2.invoke(() -> doKeySetTransactionWithInvalidate());
  }

  private int createServerRegion(int totalNumBuckets, boolean isAccessor) throws Exception {
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    factory.setTotalNumBuckets(totalNumBuckets);
    if (isAccessor) {
      factory.setLocalMaxMemory(0);
    }
    PartitionAttributes partitionAttributes = factory.create();
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(partitionAttributes).create(regionName);

    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void doDestroyOps() {
    Region region = cacheRule.getCache().getRegion(regionName);
    region.put(key, originalValue);
    region.destroy(key); // creates a tombstone
  }

  private void doInvalidateOps() {
    Region region = cacheRule.getCache().getRegion(regionName);
    region.put(key, originalValue);
    region.invalidate(key);
  }

  private void doValuesTransactionWithTombstone() {
    TXManagerImpl txMgr =
        (TXManagerImpl) cacheRule.getCache().getCacheTransactionManager();

    Region region = cacheRule.getCache().getRegion(regionName);
    txMgr.begin(); // tx1
    region.put("key2", "someValue");
    region.values().toArray();
    TransactionId txId = txMgr.suspend();

    txMgr.begin(); // tx2
    region.put(key, value2);
    txMgr.commit();

    txMgr.resume(txId);
    region.put(key, value1);
    txMgr.commit();

    assertThat(region.get(key)).isEqualTo(value1);
  }

  private void doKeySetTransactionWithTombstone() {
    TXManagerImpl txMgr =
        (TXManagerImpl) cacheRule.getCache().getCacheTransactionManager();

    Region region = cacheRule.getCache().getRegion(regionName);
    txMgr.begin(); // tx1
    region.put("key2", "someValue");
    region.keySet().toArray();
    TransactionId txId = txMgr.suspend();

    txMgr.begin(); // tx2
    region.put(key, value2);
    txMgr.commit();

    txMgr.resume(txId);
    region.put(key, value1);
    txMgr.commit();

    assertThat(region.get(key)).isEqualTo(value1);
  }

  private void doValuesTransactionWithInvalidate() {
    TXManagerImpl txMgr =
        (TXManagerImpl) cacheRule.getCache().getCacheTransactionManager();

    Region region = cacheRule.getCache().getRegion(regionName);
    txMgr.begin(); // tx1
    region.put("key2", "someValue");
    region.values().toArray();
    TransactionId txId = txMgr.suspend();

    txMgr.begin(); // tx2
    region.put(key, value2);
    txMgr.commit();

    txMgr.resume(txId);
    region.put(key, value1);
    assertThatThrownBy(() -> txMgr.commit()).isExactlyInstanceOf(CommitConflictException.class);
    assertThat(region.get(key)).isEqualTo(value2);
  }

  private void doKeySetTransactionWithInvalidate() {
    TXManagerImpl txMgr =
        (TXManagerImpl) cacheRule.getCache().getCacheTransactionManager();

    Region region = cacheRule.getCache().getRegion(regionName);
    txMgr.begin(); // tx1
    region.put("key2", "someValue");
    region.keySet().toArray();
    TransactionId txId = txMgr.suspend();

    txMgr.begin(); // tx2
    region.put(key, value2);
    txMgr.commit();

    txMgr.resume(txId);
    region.put(key, value1);
    assertThatThrownBy(() -> txMgr.commit()).isExactlyInstanceOf(CommitConflictException.class);
    assertThat(region.get(key)).isEqualTo(value2);
  }
}
