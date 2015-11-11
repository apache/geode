/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.internal.directory.RegionDirectory;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepositoryImpl;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.RetryTimeKeeper;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PartitionedRepositoryManagerJUnitTest {

  
  private PartitionedRegion userRegion;
  private PartitionedRegion fileRegion;
  private PartitionedRegion chunkRegion;
  private LuceneSerializer serializer;
  private PartitionedRegionDataStore userDataStore;
  private PartitionedRegionDataStore fileDataStore;
  private PartitionedRegionDataStore chunkDataStore;
  
  private Map<Integer, BucketRegion> fileBuckets = new HashMap<Integer, BucketRegion>();
  private Map<Integer, BucketRegion> chunkBuckets= new HashMap<Integer, BucketRegion>();

  @Before
  public void setUp() {
    userRegion = Mockito.mock(PartitionedRegion.class);
    userDataStore = Mockito.mock(PartitionedRegionDataStore.class);
    Mockito.when(userRegion.getDataStore()).thenReturn(userDataStore);
    
    fileRegion = Mockito.mock(PartitionedRegion.class);
    fileDataStore = Mockito.mock(PartitionedRegionDataStore.class);
    Mockito.when(fileRegion.getDataStore()).thenReturn(fileDataStore);
    chunkRegion = Mockito.mock(PartitionedRegion.class);
    chunkDataStore = Mockito.mock(PartitionedRegionDataStore.class);
    Mockito.when(chunkRegion.getDataStore()).thenReturn(chunkDataStore);
    serializer = new HeterogenousLuceneSerializer(new String[] {"a", "b"} );  
  }
  
  @Test
  public void getByKey() throws BucketNotFoundException, IOException {
    PartitionedRepositoryManager repoManager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, serializer, new StandardAnalyzer());
    
    setUpMockBucket(0);
    setUpMockBucket(1);
    
    IndexRepositoryImpl repo0 = (IndexRepositoryImpl) repoManager.getRepository(userRegion, 0, null);
    IndexRepositoryImpl repo1 = (IndexRepositoryImpl) repoManager.getRepository(userRegion, 1, null);
    IndexRepositoryImpl repo113 = (IndexRepositoryImpl) repoManager.getRepository(userRegion, 113, null);

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
  public void destroyBucket() throws BucketNotFoundException, IOException {
    PartitionedRepositoryManager repoManager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, serializer, new StandardAnalyzer());
    
    setUpMockBucket(0);
    
    IndexRepositoryImpl repo0 = (IndexRepositoryImpl) repoManager.getRepository(userRegion, 0, null);

    assertNotNull(repo0);
    checkRepository(repo0, 0);
    
    BucketRegion fileBucket0 = fileBuckets.get(0);
    
    //Simulate rebalancing of a bucket by marking the old bucket is destroyed
    //and creating a new bucket
    Mockito.when(fileBucket0.isDestroyed()).thenReturn(true);
    setUpMockBucket(0);
    
    IndexRepositoryImpl newRepo0 = (IndexRepositoryImpl) repoManager.getRepository(userRegion, 0, null);
    assertNotEquals(repo0, newRepo0);
    checkRepository(newRepo0, 0);
  }
  
  /**
   * Test that we get the expected exception when a user bucket is missing
   */
  @Test(expected = BucketNotFoundException.class)
  public void getMissingBucketByKey() throws BucketNotFoundException {
    PartitionedRepositoryManager repoManager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, serializer, new StandardAnalyzer());
    repoManager.getRepository(userRegion, 0, null);
  }
  
  @Test
  public void createMissingBucket() throws BucketNotFoundException {
    PartitionedRepositoryManager repoManager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, serializer, new StandardAnalyzer());
    setUpMockBucket(0);
    
    Mockito.when(fileDataStore.getLocalBucketById(eq(0))).thenReturn(null);
    
    Mockito.when(fileRegion.getOrCreateNodeForBucketWrite(eq(0), (RetryTimeKeeper) any())).then(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Mockito.when(fileDataStore.getLocalBucketById(eq(0))).thenReturn(fileBuckets.get(0));
        return null;
      }
    });
    
    assertNotNull(repoManager.getRepository(userRegion, 0, null));
  }
  
  @Test
  public void getByRegion() throws BucketNotFoundException {

    PartitionedRepositoryManager repoManager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, serializer, new StandardAnalyzer());
    
    setUpMockBucket(0);
    setUpMockBucket(1);

    Set<Integer> buckets = new LinkedHashSet<Integer>(Arrays.asList(0, 1));
    InternalRegionFunctionContext ctx = Mockito.mock(InternalRegionFunctionContext.class);
    Mockito.when(ctx.getLocalBucketSet((any(Region.class)))).thenReturn(buckets);
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
    PartitionedRepositoryManager repoManager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, serializer, new StandardAnalyzer());
    
    setUpMockBucket(0);

    Set<Integer> buckets = new LinkedHashSet<Integer>(Arrays.asList(0, 1));

    InternalRegionFunctionContext ctx = Mockito.mock(InternalRegionFunctionContext.class);
    Mockito.when(ctx.getLocalBucketSet((any(Region.class)))).thenReturn(buckets);
    repoManager.getRepositories(ctx);
  }
  
  private void checkRepository(IndexRepositoryImpl repo0, int bucketId) {
    IndexWriter writer0 = repo0.getWriter();
    RegionDirectory dir0 = (RegionDirectory) writer0.getDirectory();
    assertEquals(fileBuckets.get(bucketId), dir0.getFileSystem().getFileRegion());
    assertEquals(chunkBuckets.get(bucketId), dir0.getFileSystem().getChunkRegion());
    assertEquals(serializer, repo0.getSerializer());
  }
  
  private BucketRegion setUpMockBucket(int id) {
    BucketRegion mockBucket = Mockito.mock(BucketRegion.class);
    BucketRegion fileBucket = Mockito.mock(BucketRegion.class);
    BucketRegion chunkBucket = Mockito.mock(BucketRegion.class);
    Mockito.when(mockBucket.getId()).thenReturn(id);
    Mockito.when(userRegion.getBucketRegion(eq(id), eq(null))).thenReturn(mockBucket);
    Mockito.when(userDataStore.getLocalBucketById(eq(id))).thenReturn(mockBucket);
    Mockito.when(userRegion.getBucketRegion(eq(id + 113), eq(null))).thenReturn(mockBucket);
    Mockito.when(userDataStore.getLocalBucketById(eq(id + 113))).thenReturn(mockBucket);
    Mockito.when(fileDataStore.getLocalBucketById(eq(id))).thenReturn(fileBucket);
    Mockito.when(chunkDataStore.getLocalBucketById(eq(id))).thenReturn(chunkBucket);
    
    fileBuckets.put(id, fileBucket);
    chunkBuckets.put(id, chunkBucket);
    return mockBucket;
  }
}
