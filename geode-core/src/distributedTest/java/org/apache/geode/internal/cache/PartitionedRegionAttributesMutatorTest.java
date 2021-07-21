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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.CopyHelper;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserverAdapter;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserverHolder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PartitionedRegionAttributesMutatorTest {
  private static final String TEST_REGION_NAME = "testRegion";
  private static final int DEFAULT_WAIT_DURATION = 5;
  private static final TimeUnit DEFAULT_WAIT_UNIT = TimeUnit.SECONDS;

  private static MemberVM locator, server;
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  public void startCluster() {
    Properties gemfireProperties = new Properties();
    locator = cluster.startLocatorVM(0, gemfireProperties);
    server = cluster.startServerVM(1, gemfireProperties, locator.getPort());
  }

  @After
  public void stopCluster() {
    cluster.stop(1);
    cluster.stop(0);
  }

  @Test
  public void testChangeCacheLoaderDuringBucketCreation()
      throws InterruptedException, TimeoutException, ExecutionException {
    startCluster();
    server.invoke(() -> {
      CountDownLatch mutationMade = new CountDownLatch(1);
      CountDownLatch bucketCreated = new CountDownLatch(1);
      PartitionedRegion pr = createRegion(bucketCreated, mutationMade);
      CacheLoader loader = createTestCacheLoader();

      CompletableFuture<Void> createBucket =
          CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
      bucketCreated.await();
      PartitionRegionConfig beforeConfig = getConfig(pr);
      pr.getAttributesMutator().setCacheLoader(loader);
      mutationMade.countDown();
      createBucket.get(DEFAULT_WAIT_DURATION, DEFAULT_WAIT_UNIT);
      getAllBucketRegions(pr).forEach(region -> assertEquals(loader, region.getCacheLoader()));
      PartitionRegionConfig afterConfig = getConfig(pr);
      verifyMetaDataIsOk(beforeConfig, afterConfig);
    });
  }

  private static PartitionRegionConfig getConfig(PartitionedRegion pr) {
    return CopyHelper.copy(pr.getPRRoot().get(pr.getRegionIdentifier()));
  }

  private static void verifyMetaDataIsOk(PartitionRegionConfig beforeConfig,
      PartitionRegionConfig afterConfig) {
    assertEquals(beforeConfig.isColocationComplete(), afterConfig.isColocationComplete());
    assertEquals(beforeConfig.isFirstDataStoreCreated(), afterConfig.isFirstDataStoreCreated());
    assertEquals(beforeConfig.getNodes(), afterConfig.getNodes());
    assertEquals(beforeConfig.getColocatedWith(), afterConfig.getColocatedWith());

    List<InternalDistributedMember> beforeMembers = getMembers(beforeConfig);
    List<InternalDistributedMember> afterMembers = getMembers(afterConfig);
    assertEquals(beforeMembers, afterMembers);
  }

  private static List<InternalDistributedMember> getMembers(PartitionRegionConfig beforeConfig) {
    return beforeConfig.getNodes()
        .stream()
        .map(Node::getMemberId)
        .collect(Collectors.toList());
  }

  @Test
  public void testChangeCacheLoaderWithClusterConfigUpdatesNodeInfo() {
    startCluster();
    server.invoke(() -> {
      PartitionedRegion pr = createRegionSpy();
      CacheLoader loader = createTestCacheLoader();
      PartitionRegionConfig beforeConfig = getConfig(pr);
      pr.getAttributesMutator().setCacheLoader(loader);
      verify(pr).updatePRNodeInformation();
      verify(pr, times(1)).updatePRConfig(any(), anyBoolean());
      PartitionRegionConfig afterConfig = getConfig(pr);
      verifyMetaDataIsOk(beforeConfig, afterConfig);
    });
  }

  @Test
  public void testChangeCacheWriterWithClusterConfigUpdatesNodeInfo() {
    startCluster();
    server.invoke(() -> {
      PartitionedRegion pr = createRegionSpy();
      CacheWriter writer = createTestCacheWriter();
      PartitionRegionConfig beforeConfig = getConfig(pr);
      pr.getAttributesMutator().setCacheWriter(writer);
      verify(pr).updatePRNodeInformation();
      verify(pr, times(1)).updatePRConfig(any(), anyBoolean());
      PartitionRegionConfig afterConfig = getConfig(pr);
      verifyMetaDataIsOk(beforeConfig, afterConfig);
    });
  }

  @Test
  public void testChangeCustomEntryTtlDuringBucketCreation()
      throws InterruptedException, ExecutionException {
    startCluster();
    server.invoke(() -> {
      CountDownLatch mutationMade = new CountDownLatch(1);
      CountDownLatch bucketCreated = new CountDownLatch(1);
      PartitionedRegion pr = createRegion(bucketCreated, mutationMade);
      CustomExpiry customExpiry = createTestCustomExpiry();

      CompletableFuture<Void> createBucket =
          CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
      bucketCreated.await();
      PartitionRegionConfig beforeConfig = getConfig(pr);
      pr.getAttributesMutator().setCustomEntryTimeToLive(customExpiry);
      mutationMade.countDown();
      createBucket.get();

      getAllBucketRegions(pr)
          .forEach(region -> assertEquals(customExpiry, region.customEntryTimeToLive));
      PartitionRegionConfig afterConfig = getConfig(pr);
      verifyMetaDataIsOk(beforeConfig, afterConfig);
    });
  }

  @Test
  public void testChangeCustomEntryIdleTimeoutDuringBucketCreation()
      throws InterruptedException, ExecutionException {
    startCluster();
    server.invoke(() -> {
      CountDownLatch mutationMade = new CountDownLatch(1);
      CountDownLatch bucketCreated = new CountDownLatch(1);
      PartitionedRegion pr = createRegion(bucketCreated, mutationMade);
      CustomExpiry customExpiry = createTestCustomExpiry();

      CompletableFuture<Void> createBucket =
          CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
      bucketCreated.await();
      PartitionRegionConfig beforeConfig = getConfig(pr);
      pr.getAttributesMutator().setCustomEntryIdleTimeout(customExpiry);
      mutationMade.countDown();
      createBucket.get();

      getAllBucketRegions(pr)
          .forEach(region -> assertEquals(customExpiry, region.customEntryIdleTimeout));
      PartitionRegionConfig afterConfig = getConfig(pr);
      verifyMetaDataIsOk(beforeConfig, afterConfig);
    });
  }

  @Test
  public void testChangeEntryIdleTimeoutDuringBucketCreation()
      throws InterruptedException, ExecutionException {
    startCluster();
    server.invoke(() -> {
      CountDownLatch mutationMade = new CountDownLatch(1);
      CountDownLatch bucketCreated = new CountDownLatch(1);
      PartitionedRegion pr = createRegionWithFewBuckets(bucketCreated, mutationMade);

      CompletableFuture<Void> createBucket =
          CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
      bucketCreated.await();
      ExpirationAttributes expirationAttributes =
          new ExpirationAttributes(1000, ExpirationAction.DESTROY);
      PartitionRegionConfig beforeConfig = getConfig(pr);
      pr.getAttributesMutator().setEntryIdleTimeout(expirationAttributes);
      mutationMade.countDown();
      createBucket.get();

      getAllBucketRegions(pr)
          .forEach(region -> assertEquals(expirationAttributes, region.getEntryIdleTimeout()));
      PartitionRegionConfig afterConfig = getConfig(pr);
      verifyMetaDataIsOk(beforeConfig, afterConfig);
    });
  }

  @Test
  public void testChangeEntryTtlDuringBucketCreation()
      throws InterruptedException, ExecutionException {
    startCluster();
    server.invoke(() -> {
      CountDownLatch mutationMade = new CountDownLatch(1);
      CountDownLatch bucketCreated = new CountDownLatch(1);
      PartitionedRegion pr = createRegionWithFewBuckets(bucketCreated, mutationMade);
      CompletableFuture<Void> createBucket =
          CompletableFuture.runAsync(() -> PartitionRegionHelper.assignBucketsToPartitions(pr));
      bucketCreated.await();
      ExpirationAttributes expirationAttributes =
          new ExpirationAttributes(1000, ExpirationAction.DESTROY);
      PartitionRegionConfig beforeConfig = getConfig(pr);
      pr.getAttributesMutator().setEntryTimeToLive(expirationAttributes);
      mutationMade.countDown();
      createBucket.get();

      getAllBucketRegions(pr)
          .forEach(region -> assertEquals(expirationAttributes, region.getEntryTimeToLive()));
      PartitionRegionConfig afterConfig = getConfig(pr);
      verifyMetaDataIsOk(beforeConfig, afterConfig);
    });
  }

  private static PartitionedRegion createRegion(CountDownLatch bucketCreated,
      CountDownLatch mutationMade) {
    PartitionedRegion pr = (PartitionedRegion) ClusterStartupRule.getCache()
        .createRegionFactory(RegionShortcut.PARTITION)
        .setStatisticsEnabled(true).create(TEST_REGION_NAME);
    setRegionObserver(bucketCreated, mutationMade);
    return pr;
  }

  private static PartitionedRegion createRegionSpy() {
    return Mockito
        .spy((PartitionedRegion) ClusterStartupRule.getCache()
            .createRegionFactory(RegionShortcut.PARTITION)
            .setStatisticsEnabled(true).create(TEST_REGION_NAME));
  }

  private static PartitionedRegion createRegionWithFewBuckets(CountDownLatch bucketCreated,
      CountDownLatch mutationMade) {
    PartitionAttributes partitionAttributes =
        new PartitionAttributesFactory().setTotalNumBuckets(5).create();
    PartitionedRegion pr = (PartitionedRegion) ClusterStartupRule.getCache()
        .createRegionFactory(RegionShortcut.PARTITION)
        .setStatisticsEnabled(true).setPartitionAttributes(partitionAttributes)
        .create(TEST_REGION_NAME);
    setRegionObserver(bucketCreated, mutationMade);
    return pr;
  }

  // Adds an observer which will block bucket creation and wait for a loader to be added
  private static void setRegionObserver(CountDownLatch bucketCreated, CountDownLatch mutationMade) {
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

  private static Set<BucketRegion> getAllBucketRegions(PartitionedRegion pr) {
    return pr.getDataStore().getAllLocalBucketRegions();
  }

  private static CacheLoader createTestCacheLoader() {
    return new CacheLoader() {
      @Override
      public void close() {}

      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        return null;
      }
    };
  }

  private static CacheWriter createTestCacheWriter() {
    return new CacheWriter() {
      @Override
      public void beforeUpdate(EntryEvent event) throws CacheWriterException {

      }

      @Override
      public void beforeCreate(EntryEvent event) throws CacheWriterException {

      }

      @Override
      public void beforeDestroy(EntryEvent event) throws CacheWriterException {

      }

      @Override
      public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {

      }

      @Override
      public void beforeRegionClear(RegionEvent event) throws CacheWriterException {

      }

      @Override
      public void close() {}

    };
  }

  private static CustomExpiry createTestCustomExpiry() {
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
