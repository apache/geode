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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingMap;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionRegionConfig;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegion.RetryTimeKeeper;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.UnitTest")
@PrepareForTest({PartitionedRegionHelper.class})
public class PartitionedRepositoryManagerJUnitTest {

  protected PartitionedRegion userRegion;
  protected PartitionedRegion fileAndChunkRegion;
  protected LuceneSerializer serializer;
  protected PartitionedRegionDataStore userDataStore;
  protected PartitionedRegionDataStore fileDataStore;
  protected PartitionedRegionHelper prHelper;
  protected PartitionRegionConfig prConfig;
  protected LocalRegion prRoot;

  protected Map<Integer, BucketRegion> fileAndChunkBuckets = new HashMap<Integer, BucketRegion>();
  protected Map<Integer, BucketRegion> dataBuckets = new HashMap<Integer, BucketRegion>();
  protected LuceneIndexStats indexStats;
  protected FileSystemStats fileSystemStats;
  protected LuceneIndexImpl indexForPR;
  protected AbstractPartitionedRepositoryManager repoManager;
  protected GemFireCacheImpl cache;

  @Before
  public void setUp() {
    cache = Fakes.cache();
    userRegion = Mockito.mock(PartitionedRegion.class);
    userDataStore = Mockito.mock(PartitionedRegionDataStore.class);
    when(userRegion.getDataStore()).thenReturn(userDataStore);
    when(cache.getRegion("/testRegion")).thenReturn(userRegion);
    serializer = new HeterogeneousLuceneSerializer();
    DLockService lockService = mock(DLockService.class);
    when(lockService.lock(any(), anyLong(), anyLong())).thenReturn(true);
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
    when(indexForPR.getRegionPath()).thenReturn("/testRegion");
    prRoot = Mockito.mock(LocalRegion.class);
    prConfig = Mockito.mock(PartitionRegionConfig.class);
    when(prConfig.isColocationComplete()).thenReturn(true);
    when(prRoot.get("rid")).thenReturn(prConfig);
    PowerMockito.mockStatic(PartitionedRegionHelper.class);
    PowerMockito.when(PartitionedRegionHelper.getPRRoot(cache)).thenReturn(prRoot);
    repoManager = new PartitionedRepositoryManager(indexForPR, serializer);
    repoManager.setUserRegionForRepositoryManager();
    repoManager.allowRepositoryComputation();
  }

  @Test
  public void getByKey() throws BucketNotFoundException, IOException {
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

    checkRepository(repo0, 0);
    checkRepository(repo1, 1);
  }

  /**
   * Test what happens when a bucket is destroyed.
   */
  @Test
  public void destroyBucketShouldCreateNewIndexRepository()
      throws BucketNotFoundException, IOException {
    setUpMockBucket(0);

    IndexRepositoryImpl repo0 =
        (IndexRepositoryImpl) repoManager.getRepository(userRegion, 0, null);

    assertNotNull(repo0);
    checkRepository(repo0, 0);

    BucketRegion fileBucket0 = fileAndChunkBuckets.get(0);
    BucketRegion dataBucket0 = dataBuckets.get(0);

    // Simulate rebalancing of a bucket by marking the old bucket is destroyed
    // and creating a new bucket
    when(dataBucket0.isDestroyed()).thenReturn(true);
    setUpMockBucket(0);

    IndexRepositoryImpl newRepo0 =
        (IndexRepositoryImpl) repoManager.getRepository(userRegion, 0, null);
    assertNotEquals(repo0, newRepo0);
    checkRepository(newRepo0, 0);
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
    setUpMockBucket(0);

    when(fileDataStore.getLocalBucketById(eq(0))).thenReturn(null);

    when(fileAndChunkRegion.getOrCreateNodeForBucketWrite(eq(0), (RetryTimeKeeper) any()))
        .then(new Answer() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            when(fileDataStore.getLocalBucketById(eq(0))).thenReturn(fileAndChunkBuckets.get(0));
            return null;
          }
        });

    assertNotNull(repoManager.getRepository(userRegion, 0, null));
  }

  @Test
  public void getByRegion() throws BucketNotFoundException {
    setUpMockBucket(0);
    setUpMockBucket(1);

    Set<Integer> buckets = new LinkedHashSet<Integer>(Arrays.asList(0, 1));
    InternalRegionFunctionContext ctx = Mockito.mock(InternalRegionFunctionContext.class);
    when(ctx.getLocalBucketSet((any()))).thenReturn(buckets);
    Collection<IndexRepository> repos = repoManager.getRepositories(ctx);
    assertEquals(2, repos.size());

    Iterator<IndexRepository> itr = repos.iterator();
    IndexRepositoryImpl repo0 = (IndexRepositoryImpl) itr.next();
    IndexRepositoryImpl repo1 = (IndexRepositoryImpl) itr.next();

    assertNotNull(repo0);
    assertNotNull(repo1);
    assertNotEquals(repo0, repo1);

    checkRepository(repo0, 0);
    checkRepository(repo1, 1);
  }

  /**
   * Test that we get the expected exception when a user bucket is missing
   */
  @Test(expected = BucketNotFoundException.class)
  public void getMissingBucketByRegion() throws BucketNotFoundException {
    setUpMockBucket(0);

    Set<Integer> buckets = new LinkedHashSet<Integer>(Arrays.asList(0, 1));

    InternalRegionFunctionContext ctx = Mockito.mock(InternalRegionFunctionContext.class);
    when(ctx.getLocalBucketSet((any()))).thenReturn(buckets);
    repoManager.getRepositories(ctx);
  }

  protected void checkRepository(IndexRepositoryImpl repo0, int bucketId) {
    IndexWriter writer0 = repo0.getWriter();
    RegionDirectory dir0 = (RegionDirectory) writer0.getDirectory();
    assertEquals(new BucketTargetingMap(fileAndChunkBuckets.get(bucketId), bucketId),
        dir0.getFileSystem().getFileAndChunkRegion());
    assertEquals(serializer, repo0.getSerializer());
  }

  protected BucketRegion setUpMockBucket(int id) throws BucketNotFoundException {
    BucketRegion mockBucket = Mockito.mock(BucketRegion.class);
    BucketRegion fileAndChunkBucket = Mockito.mock(BucketRegion.class);
    // Allowing the fileAndChunkBucket to behave like a map so that the IndexWriter operations don't
    // fail
    Fakes.addMapBehavior(fileAndChunkBucket);
    when(fileAndChunkBucket.getFullPath()).thenReturn("File" + id);
    when(mockBucket.getId()).thenReturn(id);
    when(userRegion.getBucketRegion(eq(id), eq(null))).thenReturn(mockBucket);
    when(userDataStore.getLocalBucketById(eq(id))).thenReturn(mockBucket);
    when(userRegion.getBucketRegion(eq(id + 113), eq(null))).thenReturn(mockBucket);
    when(userDataStore.getLocalBucketById(eq(id + 113))).thenReturn(mockBucket);
    when(fileDataStore.getLocalBucketById(eq(id))).thenReturn(fileAndChunkBucket);

    fileAndChunkBuckets.put(id, fileAndChunkBucket);
    dataBuckets.put(id, mockBucket);

    BucketAdvisor mockBucketAdvisor = Mockito.mock(BucketAdvisor.class);
    when(fileAndChunkBucket.getBucketAdvisor()).thenReturn(mockBucketAdvisor);
    when(mockBucketAdvisor.isPrimary()).thenReturn(true);
    return mockBucket;
  }
}
