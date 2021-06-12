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
package org.apache.geode.cache.lucene.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class IndexRepositoryFactoryDistributedTest implements Serializable {
  private static final String INDEX_NAME = "index";
  private static final String REGION_NAME = "region";
  private static final String DEFAULT_FIELD = "text";
  protected int locatorPort;
  protected MemberVM locator, dataStore1, dataStore2;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(5);

  @Before
  public void setUp() {
    locator = cluster.startLocatorVM(0);
    locatorPort = locator.getPort();
    dataStore1 = cluster.startServerVM(1, locatorPort);
    dataStore2 = cluster.startServerVM(2, locatorPort);
  }

  private Cache getCache() {
    Cache cache = ClusterStartupRule.getCache();
    assertThat(cache).isNotNull();

    return cache;
  }

  private void initDataStoreAndLuceneIndex() {
    Cache cache = getCache();
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    luceneService.createIndexFactory().setFields(DEFAULT_FIELD).create(INDEX_NAME, REGION_NAME);

    cache.<Integer, TestObject>createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
        .setPartitionAttributes(new PartitionAttributesFactory<Integer, TestObject>()
            .setTotalNumBuckets(1).create())
        .create(REGION_NAME);
  }

  private void insertEntries() {
    Cache cache = getCache();
    Region<Integer, TestObject> region = cache.getRegion(REGION_NAME);
    IntStream.range(0, 1000).forEach(i -> region.put(i, new TestObject("hello world" + i)));
  }

  private void assertPrimariesAndSecondaries(int primaries, int secondaries) {
    Cache cache = getCache();
    PartitionedRegionDataStore partitionedRegionDataStore =
        ((PartitionedRegion) cache.getRegion(REGION_NAME)).getDataStore();
    assertThat(partitionedRegionDataStore.getAllLocalPrimaryBucketIds().size())
        .isEqualTo(primaries);
    assertThat((partitionedRegionDataStore.getAllLocalBucketIds().size()
        - partitionedRegionDataStore.getAllLocalPrimaryBucketIds().size())).isEqualTo(secondaries);
  }

  private BucketRegion getFileAndChunkBucket() {
    Cache cache = getCache();
    LuceneServiceImpl luceneService = (LuceneServiceImpl) LuceneServiceProvider.get(cache);
    InternalLuceneIndex index =
        (InternalLuceneIndex) luceneService.getIndex(INDEX_NAME, REGION_NAME);
    LuceneIndexForPartitionedRegion indexForPR = (LuceneIndexForPartitionedRegion) index;
    PartitionedRegion fileRegion = indexForPR.getFileAndChunkRegion();

    return PartitionedRepositoryManager.indexRepositoryFactory.getMatchingBucket(fileRegion, 0);
  }

  @Test
  public void lockedBucketShouldPreventPrimaryFromMoving() {
    dataStore1.invoke(this::initDataStoreAndLuceneIndex);
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));
    dataStore1.invoke(this::insertEntries);
    dataStore2.invoke(this::initDataStoreAndLuceneIndex);
    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));

    dataStore1.invoke(() -> {
      Cache cache = getCache();
      LuceneService service = LuceneServiceProvider.get(cache);
      await().untilAsserted(
          () -> assertThat(service.waitUntilFlushed(INDEX_NAME, REGION_NAME, 1, TimeUnit.MINUTES))
              .isTrue());
    });
    dataStore1.invoke(() -> assertPrimariesAndSecondaries(1, 0));
    dataStore2.invoke(() -> assertPrimariesAndSecondaries(0, 1));

    // Lock is already held by server1.
    dataStore1.invoke(() -> {
      BucketRegion fileAndChunkBucket = getFileAndChunkBucket();
      String lockName =
          PartitionedRepositoryManager.indexRepositoryFactory.getLockName(fileAndChunkBucket);
      DistributedLockService lockService =
          PartitionedRepositoryManager.indexRepositoryFactory.getLockService();
      assertThat(lockService.lock(lockName, 10000, -1)).isFalse();
    });

    // Try to become primary on server2, it should fail.
    dataStore2.invoke(() -> {
      BucketRegion fileAndChunkBucket = getFileAndChunkBucket();
      try {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
            () -> assertThat(fileAndChunkBucket.getBucketAdvisor().becomePrimary(false)).isTrue());
        fail("Bucket must not become primary while other member holds the lock.");
      } catch (ConditionTimeoutException ignore) {
      }
    });

    dataStore1.invoke(() -> assertPrimariesAndSecondaries(1, 0));
    dataStore2.invoke(() -> assertPrimariesAndSecondaries(0, 1));
  }

  protected static class TestObject implements DataSerializable {
    private static final long serialVersionUID = 1L;
    private String text;

    public TestObject() {}

    public TestObject(String text) {
      this.text = text + RandomStringUtils.randomAlphanumeric(100, 10000);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((text == null) ? 0 : text.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TestObject other = (TestObject) obj;
      if (text == null) {
        return other.text == null;
      } else {
        return text.equals(other.text);
      }
    }

    @Override
    public String toString() {
      return "TestObject[" + text + "]";
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeUTF(text);
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      text = in.readUTF();
    }
  }
}
