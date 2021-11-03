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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.test.fake.Fakes;

public class RawLuceneRepositoryManagerJUnitTest extends PartitionedRepositoryManagerJUnitTest {

  @Override
  @Before
  public void setUp() {
    cache = Fakes.cache();

    userRegion = Mockito.mock(PartitionedRegion.class);
    userDataStore = Mockito.mock(PartitionedRegionDataStore.class);
    when(userRegion.getDataStore()).thenReturn(userDataStore);
    when(cache.getRegion(SEPARATOR + "testRegion")).thenReturn(userRegion);
    serializer = new HeterogeneousLuceneSerializer();
    createIndexAndRepoManager();
  }

  @Override
  @After
  public void tearDown() {
    ((RawLuceneRepositoryManager) repoManager).close();
  }

  @Override
  protected void createIndexAndRepoManager() {
    LuceneServiceImpl.luceneIndexFactory = new LuceneRawIndexFactory();

    indexStats = Mockito.mock(LuceneIndexStats.class);
    indexForPR = Mockito.mock(LuceneRawIndex.class);
    when(indexForPR.getIndexStats()).thenReturn(indexStats);
    when(indexForPR.getAnalyzer()).thenReturn(new StandardAnalyzer());
    when(indexForPR.getCache()).thenReturn(cache);
    when(indexForPR.getRegionPath()).thenReturn(SEPARATOR + "testRegion");
    when(indexForPR.withPersistence()).thenReturn(true);
    repoManager =
        new RawLuceneRepositoryManager(indexForPR, serializer, Executors.newSingleThreadExecutor());
    repoManager.setUserRegionForRepositoryManager(userRegion);
    repoManager.allowRepositoryComputation();
  }

  @Test
  public void testIndexRepositoryFactoryShouldBeRaw() {
    assertTrue(
        RawLuceneRepositoryManager.indexRepositoryFactory instanceof RawIndexRepositoryFactory);
  }

  @Override
  protected void checkRepository(IndexRepositoryImpl repo0, int... bucketId) {
    IndexWriter writer0 = repo0.getWriter();
    Directory dir0 = writer0.getDirectory();
    assertTrue(dir0 instanceof NIOFSDirectory);
  }

  @Override
  protected BucketRegion setUpMockBucket(int id) throws BucketNotFoundException {
    BucketRegion mockBucket = Mockito.mock(BucketRegion.class);
    when(mockBucket.getId()).thenReturn(id);
    when(userRegion.getBucketRegion(eq(id), eq(null))).thenReturn(mockBucket);
    when(userDataStore.getLocalBucketById(eq(id))).thenReturn(mockBucket);
    when(userRegion.getBucketRegion(eq(id + 113), eq(null))).thenReturn(mockBucket);
    when(userDataStore.getLocalBucketById(eq(id + 113))).thenReturn(mockBucket);
    dataBuckets.put(id, mockBucket);

    repoManager.computeRepository(mockBucket.getId());
    return mockBucket;
  }

  @Override
  @Test
  public void createMissingBucket() throws BucketNotFoundException {
    setUpMockBucket(0);

    assertNotNull(repoManager.getRepository(userRegion, 0, null));
  }


}
