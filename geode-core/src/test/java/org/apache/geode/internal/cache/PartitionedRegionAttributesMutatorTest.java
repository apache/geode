/*
 *
 * * Licensed to the Apache Software Foundation (ASF) under one or more contributor license *
 * agreements. See the NOTICE file distributed with this work for additional information regarding *
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * * "License"); you may not use this file except in compliance with the License. You may obtain a *
 * copy of the License at * * http://www.apache.org/licenses/LICENSE-2.0 * * Unless required by
 * applicable law or agreed to in writing, software distributed under the License * is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express * or implied.
 * See the License for the specific language governing permissions and limitations under * the
 * License. *
 *
 */

package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserverAdapter;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserverHolder;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(IntegrationTest.class)
public class PartitionedRegionAttributesMutatorTest {
  private static final String TEST_REGION_NAME = "testRegion";
  private static final int DEFAULT_WAIT_DURATION = 5;
  private static final TimeUnit DEFAULT_WAIT_UNIT = TimeUnit.SECONDS;

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  private final CountDownLatch mutationMade = new CountDownLatch(1);
  private final CountDownLatch bucketCreated = new CountDownLatch(1);
  private PartitionedRegion pr;

  @Test
  public void testChangeCacheLoaderDuringBucketCreation()
      throws InterruptedException, TimeoutException, ExecutionException {
    createRegion();
    CacheLoader loader = createTestCacheLoader();

    CompletableFuture<Void> createBucket =
        CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
    bucketCreated.await();
    pr.getAttributesMutator().setCacheLoader(loader);
    mutationMade.countDown();
    createBucket.get(DEFAULT_WAIT_DURATION, DEFAULT_WAIT_UNIT);

    getAllBucketRegions(pr).forEach(region -> assertEquals(loader, region.getCacheLoader()));
  }

  @Test
  public void testChangeCustomEntryTtlDuringBucketCreation()
      throws InterruptedException, ExecutionException {
    createRegion();
    CustomExpiry customExpiry = createTestCustomExpiry();

    CompletableFuture<Void> createBucket =
        CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
    bucketCreated.await();
    pr.getAttributesMutator().setCustomEntryTimeToLive(customExpiry);
    mutationMade.countDown();
    createBucket.get();

    getAllBucketRegions(pr)
        .forEach(region -> assertEquals(customExpiry, region.customEntryTimeToLive));
  }

  @Test
  public void testChangeCustomEntryIdleTimeoutDuringBucketCreation()
      throws InterruptedException, ExecutionException {
    createRegion();
    CustomExpiry customExpiry = createTestCustomExpiry();

    CompletableFuture<Void> createBucket =
        CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
    bucketCreated.await();
    pr.getAttributesMutator().setCustomEntryIdleTimeout(customExpiry);
    mutationMade.countDown();
    createBucket.get();

    getAllBucketRegions(pr)
        .forEach(region -> assertEquals(customExpiry, region.customEntryIdleTimeout));
  }

  @Test
  public void testChangeEntryIdleTimeoutDuringBucketCreation()
      throws InterruptedException, ExecutionException {
    createRegionWithFewBuckets();

    CompletableFuture<Void> createBucket =
        CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
    bucketCreated.await();
    ExpirationAttributes expirationAttributes =
        new ExpirationAttributes(1000, ExpirationAction.DESTROY);
    pr.getAttributesMutator().setEntryIdleTimeout(expirationAttributes);
    mutationMade.countDown();
    createBucket.get();

    getAllBucketRegions(pr)
        .forEach(region -> assertEquals(expirationAttributes, region.getEntryIdleTimeout()));
  }

  @Test
  public void testChangeEntryTtlDuringBucketCreation()
      throws InterruptedException, ExecutionException {
    createRegionWithFewBuckets();

    CompletableFuture<Void> createBucket =
        CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
    bucketCreated.await();
    ExpirationAttributes expirationAttributes =
        new ExpirationAttributes(1000, ExpirationAction.DESTROY);
    pr.getAttributesMutator().setEntryTimeToLive(expirationAttributes);
    mutationMade.countDown();
    createBucket.get();

    getAllBucketRegions(pr)
        .forEach(region -> assertEquals(expirationAttributes, region.getEntryTimeToLive()));
  }

  private void createRegion() {
    pr = (PartitionedRegion) server.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setStatisticsEnabled(true).create(TEST_REGION_NAME);
    setRegionObserver();
  }

  private void createRegionWithFewBuckets() {
    PartitionAttributes partitionAttributes =
        new PartitionAttributesFactory().setTotalNumBuckets(5).create();
    pr = (PartitionedRegion) server.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setStatisticsEnabled(true).setPartitionAttributes(partitionAttributes)
        .create(TEST_REGION_NAME);
    setRegionObserver();
  }

  // Adds an observer which will block bucket creation and wait for a loader to be added
  private void setRegionObserver() {
    PartitionedRegionObserverHolder.setInstance(new PartitionedRegionObserverAdapter() {
      @Override
      public void beforeAssignBucket(PartitionedRegion partitionedRegion, int bucketId) {
        try {
          // Indicate that the bucket has been created
          bucketCreated.countDown();

          // Wait for the loader to be added. if the synchronization
          // is correct, this would wait for ever because setting the
          // cache loader will wait for this method. So time out after
          // 1 second, which should be good enough to cause a failure
          // if the synchronization is broken.
          mutationMade.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted");
        }
      }
    });
  }

  private Set<BucketRegion> getAllBucketRegions(PartitionedRegion pr) {
    return pr.getDataStore().getAllLocalBucketRegions();
  }

  private CacheLoader createTestCacheLoader() {
    return new CacheLoader() {
      @Override
      public void close() {}

      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        return null;
      }
    };
  }

  private CustomExpiry createTestCustomExpiry() {
    return new CustomExpiry() {
      @Override
      public ExpirationAttributes getExpiry(Region.Entry entry) {
        return null;
      }

      @Override
      public void close() {

      }
    };
  }
}
