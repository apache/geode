/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.cache.PartitionedRegionHelper.PR_ROOT_REGION_NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingMap;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.PartitionRegionConfig;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.internal.cache.partitioned.BucketId;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class PartitionedRepositoryManagerJUnitTest {

  protected PartitionedRegion userRegion;
  protected PartitionedRegion fileAndChunkRegion;
  protected LuceneSerializer<?> serializer;
  protected PartitionedRegionDataStore userDataStore;
  protected PartitionedRegionDataStore fileDataStore;
  protected PartitionRegionConfig prConfig;
  protected DistributedRegion prRoot;

  protected Map<BucketId, BucketRegion> fileAndChunkBuckets = new HashMap<>();
  protected Map<BucketId, BucketRegion> dataBuckets = new HashMap<>();
  protected LuceneIndexStats indexStats;
  protected FileSystemStats fileSystemStats;
  protected LuceneIndexImpl indexForPR;
  protected PartitionedRepositoryManager repoManager;
  protected GemFireCacheImpl cache;

  private final Set<BucketId> buckets =
      Stream.of(0, 1).map(BucketId::valueOf).collect(Collectors.toSet());

  @Before
  public void setUp() {
    cache = Fakes.cache();
    userRegion = Mockito.mock(PartitionedRegion.class);
    userDataStore = Mockito.mock(PartitionedRegionDataStore.class);
    when(userRegion.getDataStore()).thenReturn(userDataStore);
    when(cache.getRegion(SEPARATOR + "testRegion")).thenReturn(uncheckedCast(userRegion));
    serializer = new HeterogeneousLuceneSerializer();
    DLockService lockService = mock(DLockService.class);
    when(lockService.lock(any(), anyLong(), anyLong())).thenReturn(true);
    when(userRegion.getRegionService()).thenReturn(cache);
    DLockService.addLockServiceForTests(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME,
        lockService);

    createIndexAndRepoManager();
  }

  @After
  public void tearDown() {
    DLockService.removeLockServiceForTests(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
  }

  protected void createIndexAndRepoManager() {
    fileAndChunkRegion = Mockito.mock(PartitionedRegion.class);
    fileDataStore = Mockito.mock(PartitionedRegionDataStore.class);
    when(fileAndChunkRegion.getDataStore()).thenReturn(fileDataStore);
    when(fileAndChunkRegion.getTotalNumberOfBuckets()).thenReturn(113);
    when(fileAndChunkRegion.getFullPath()).thenReturn("FileRegion");
    when(fileAndChunkRegion.getCache()).thenReturn(cache);
    when(fileAndChunkRegion.getRegionIdentifier()).thenReturn("rid");
    indexStats = Mockito.mock(LuceneIndexStats.class);
    fileSystemStats = Mockito.mock(FileSystemStats.class);
    indexForPR = Mockito.mock(LuceneIndexForPartitionedRegion.class);
    when(((LuceneIndexForPartitionedRegion) indexForPR).getFileAndChunkRegion())
        .thenReturn(fileAndChunkRegion);
    when(((LuceneIndexForPartitionedRegion) indexForPR).getFileSystemStats())
        .thenReturn(fileSystemStats);
    when(indexForPR.getIndexStats()).thenReturn(indexStats);
    when(indexForPR.getAnalyzer()).thenReturn(new StandardAnalyzer());
    when(indexForPR.getCache()).thenReturn(cache);
    when(indexForPR.getRegionPath()).thenReturn(SEPARATOR + "testRegion");

    prRoot = Mockito.mock(DistributedRegion.class);
    CacheDistributionAdvisor cda = mock(CacheDistributionAdvisor.class);
    when(prRoot.getDistributionAdvisor()).thenReturn(cda);
    doNothing().when(cda).addMembershipListener(any());
    InternalRegionFactory<Object, Object> regionFactory =
        uncheckedCast(mock(InternalRegionFactory.class));
    when(regionFactory.create(eq(PR_ROOT_REGION_NAME))).thenReturn(uncheckedCast(prRoot));
    when(cache.createInternalRegionFactory(any())).thenReturn(regionFactory);

    prConfig = Mockito.mock(PartitionRegionConfig.class);
    when(prConfig.isColocationComplete()).thenReturn(true);
    when(prRoot.get("rid")).thenReturn(prConfig);
    repoManager = new PartitionedRepositoryManager(indexForPR, serializer,
        Executors.newSingleThreadExecutor());
    repoManager.setUserRegionForRepositoryManager(userRegion);
    repoManager.allowRepositoryComputation();
  }

  @Test
  public void getByKey() throws BucketNotFoundException {

    setUpMockBucket(0);
    setUpMockBucket(1);

    IndexRepositoryImpl repo0 =
        (IndexRepositoryImpl) repoManager.getRepository(userRegion, 0, null);
    IndexRepositoryImpl repo1 =
        (IndexRepositoryImpl) repoManager.getRepository(userRegion, 1, null);
    IndexRepositoryImpl repo113 =
        (IndexRepositoryImpl) repoManager.getRepository(userRegion, 113, null);

    assertNotNull(repo0);
    assertNotNull(repo1);
    assertNotNull(repo113);
    assertEquals(repo0, repo113);
    assertNotEquals(repo0, repo1);

    checkRepository(repo0, BucketId.valueOf(0));
    checkRepository(repo1, BucketId.valueOf(1));
  }

  /**
   * Test what happens when a bucket is destroyed.
   */
  @Test
  public void destroyBucketShouldCreateNewIndexRepository()
      throws BucketNotFoundException {
    final BucketId bucketId = BucketId.valueOf(0);
    setUpMockBucket(0);

    IndexRepositoryImpl repo0 =
        (IndexRepositoryImpl) repoManager.getRepository(userRegion, 0, null);

    assertNotNull(repo0);
    checkRepository(repo0, bucketId);

    BucketRegion dataBucket0 = dataBuckets.get(bucketId);

    // Simulate rebalancing of a bucket by marking the old bucket is destroyed
    // and creating a new bucket
    when(dataBucket0.isDestroyed()).thenReturn(true);
    setUpMockBucket(0);

    IndexRepositoryImpl newRepo0 =
        (IndexRepositoryImpl) repoManager.getRepository(userRegion, 0, null);
    assertNotEquals(repo0, newRepo0);
    checkRepository(newRepo0, bucketId);
    assertTrue(repo0.isClosed());
    assertFalse(repo0.getWriter().isOpen());
  }

  /**
   * Test that we get the expected exception when a user bucket is missing
   */
  @Test(expected = BucketNotFoundException.class)
  public void getMissingBucketByKey() throws BucketNotFoundException {
    repoManager.getRepository(userRegion, 0, null);
  }

  @Test
  public void createMissingBucket() throws BucketNotFoundException {
    final BucketId bucketId = BucketId.valueOf(0);
    setUpMockBucket(0);

    when(fileDataStore.getLocalBucketById(eq(bucketId))).thenReturn(null);

    when(fileAndChunkRegion.getOrCreateNodeForBucketWrite(eq(bucketId), any()))
        .then(invocation -> {
          when(fileDataStore.getLocalBucketById(eq(bucketId)))
              .thenReturn(fileAndChunkBuckets.get(bucketId));
          return null;
        });

    assertNotNull(repoManager.getRepository(userRegion, 0, null));
  }

  @Test
  public void getByRegion() throws BucketNotFoundException {
    final BucketId bucket0 = BucketId.valueOf(0);
    final BucketId bucket1 = BucketId.valueOf(1);

    setUpMockBucket(0);
    setUpMockBucket(1);

    when(indexForPR.isIndexAvailable(bucket0)).thenReturn(true);
    when(indexForPR.isIndexAvailable(bucket1)).thenReturn(true);

    InternalRegionFunctionContext ctx = Mockito.mock(InternalRegionFunctionContext.class);
    when(ctx.getLocalBuckets((any()))).thenReturn(buckets);
    Collection<IndexRepository> repos = repoManager.getRepositories(ctx);
    assertEquals(2, repos.size());

    Iterator<IndexRepository> itr = repos.iterator();
    IndexRepositoryImpl repo0 = (IndexRepositoryImpl) itr.next();
    IndexRepositoryImpl repo1 = (IndexRepositoryImpl) itr.next();

    assertNotNull(repo0);
    assertNotNull(repo1);
    assertNotEquals(repo0, repo1);

    checkRepository(repo0, bucket0);
    checkRepository(repo1, bucket1);
  }

  /**
   * Test that we get the expected exception when a user bucket is missing
   */
  @Test(expected = BucketNotFoundException.class)
  public void getMissingBucketByRegion() throws BucketNotFoundException {
    setUpMockBucket(0);
    when(indexForPR.isIndexAvailable(BucketId.valueOf(0))).thenReturn(true);

    InternalRegionFunctionContext ctx = Mockito.mock(InternalRegionFunctionContext.class);
    when(ctx.getLocalBuckets((any()))).thenReturn(buckets);
    repoManager.getRepositories(ctx);
  }

  /**
   * Test that we get the expected exception when a user bucket is not indexed yet
   */
  @Test(expected = LuceneIndexCreationInProgressException.class)
  public void luceneIndexCreationInProgressExceptionExpectedIfIndexIsNotYetIndexed()
      throws BucketNotFoundException {
    setUpMockBucket(0);

    InternalRegionFunctionContext ctx = Mockito.mock(InternalRegionFunctionContext.class);
    when(ctx.getLocalBuckets((any()))).thenReturn(buckets);
    repoManager.getRepositories(ctx);
  }

  @Test
  public void queryOnlyWhenIndexIsAvailable() throws Exception {
    final BucketId bucket0 = BucketId.valueOf(0);
    final BucketId bucket1 = BucketId.valueOf(1);

    setUpMockBucket(0);
    setUpMockBucket(1);

    when(indexForPR.isIndexAvailable(bucket0)).thenReturn(true);
    when(indexForPR.isIndexAvailable(bucket1)).thenReturn(true);

    InternalRegionFunctionContext ctx = Mockito.mock(InternalRegionFunctionContext.class);
    when(ctx.getLocalBuckets((any()))).thenReturn(buckets);

    await().until(() -> {
      final Collection<IndexRepository> repositories = new HashSet<>();
      try {
        repositories.addAll(repoManager.getRepositories(ctx));
      } catch (BucketNotFoundException | LuceneIndexCreationInProgressException ignored) {
      }
      return repositories.size() == 2;
    });

    Iterator<IndexRepository> itr = repoManager.getRepositories(ctx).iterator();
    IndexRepositoryImpl repo0 = (IndexRepositoryImpl) itr.next();
    IndexRepositoryImpl repo1 = (IndexRepositoryImpl) itr.next();

    assertNotNull(repo0);
    assertNotNull(repo1);
    assertNotEquals(repo0, repo1);

    checkRepository(repo0, bucket0, bucket1);
    checkRepository(repo1, bucket0, bucket1);
  }

  protected void checkRepository(IndexRepositoryImpl repo0, BucketId... bucketIds) {
    IndexWriter writer0 = repo0.getWriter();
    RegionDirectory dir0 = (RegionDirectory) writer0.getDirectory();
    boolean result = false;
    for (BucketId bucketId : bucketIds) {
      BucketTargetingMap<?, ?> bucketTargetingMap =
          new BucketTargetingMap<>(fileAndChunkBuckets.get(bucketId), bucketId);
      result |= bucketTargetingMap.equals(dir0.getFileSystem().getFileAndChunkRegion());
    }

    assertTrue(result);
    assertEquals(serializer, repo0.getSerializer());
  }

  protected void setUpMockBucket(final int key) {
    final BucketId bucketId = BucketId.valueOf(key);
    final BucketRegion mockBucket = Mockito.mock(BucketRegion.class);
    final BucketRegion fileAndChunkBucket = Mockito.mock(BucketRegion.class);
    // Allowing the fileAndChunkBucket to behave like a map so that the IndexWriter operations don't
    // fail
    Fakes.addMapBehavior(fileAndChunkBucket);
    when(fileAndChunkBucket.getFullPath()).thenReturn("File" + bucketId);
    when(mockBucket.getId()).thenReturn(bucketId);
    when(userRegion.getBucketRegion(eq(key), eq(null))).thenReturn(mockBucket);
    when(userDataStore.getLocalBucketById(eq(bucketId))).thenReturn(mockBucket);
    when(userRegion.getBucketRegion(eq(key + 113), eq(null))).thenReturn(mockBucket);
    when(fileDataStore.getLocalBucketById(eq(bucketId))).thenReturn(fileAndChunkBucket);

    fileAndChunkBuckets.put(bucketId, fileAndChunkBucket);
    dataBuckets.put(bucketId, mockBucket);

    BucketAdvisor mockBucketAdvisor = Mockito.mock(BucketAdvisor.class);
    when(fileAndChunkBucket.getBucketAdvisor()).thenReturn(mockBucketAdvisor);
    when(mockBucketAdvisor.isPrimary()).thenReturn(true);
  }
}
