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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.FileSystemStats;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class LuceneIndexRecoveryHAIntegrationTest {
  String[] indexedFields = new String[] { "txt" };
  HeterogeneousLuceneSerializer mapper = new HeterogeneousLuceneSerializer(indexedFields);
  Analyzer analyzer = new StandardAnalyzer();

  Cache cache;
  private LuceneIndexStats indexStats;
  private FileSystemStats fileSystemStats;

  @Before
  public void setup() {
    indexedFields = new String[] { "txt" };
    mapper = new HeterogeneousLuceneSerializer(indexedFields);
    analyzer = new StandardAnalyzer();
    LuceneServiceImpl.registerDataSerializables();

    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    indexStats = new LuceneIndexStats(cache.getDistributedSystem(), "INDEX-REGION");
    fileSystemStats = new FileSystemStats(cache.getDistributedSystem(), "INDEX-REGION");
  }

  @After
  public void tearDown() {
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      cache.close();
    }
  }

  /**
   * On rebalance, new repository manager will be created. It will try to read fileRegion and construct index. This test
   * simulates the same.
   */
  @Test
  public void recoverRepoInANewNode() throws BucketNotFoundException, IOException {
    PartitionAttributes<String, String> attrs = new PartitionAttributesFactory().setTotalNumBuckets(1).create();
    RegionFactory<String, String> regionfactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    regionfactory.setPartitionAttributes(attrs);

    PartitionedRegion userRegion = (PartitionedRegion) regionfactory.create("userRegion");
    // put an entry to create the bucket
    userRegion.put("rebalance", "test");

    PartitionedRegion fileRegion = (PartitionedRegion) regionfactory.create("fileRegion");
    PartitionedRegion chunkRegion = (PartitionedRegion) regionfactory.create("chunkRegion");

    RepositoryManager manager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, mapper, analyzer,
      indexStats, fileSystemStats);
    IndexRepository repo = manager.getRepository(userRegion, 0, null);
    assertNotNull(repo);

    repo.create("rebalance", "test");
    repo.commit();

    // close the region to simulate bucket movement. New node will create repo using data persisted by old region
    userRegion.close();

    userRegion = (PartitionedRegion) regionfactory.create("userRegion");
    userRegion.put("rebalance", "test");
    manager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, mapper, analyzer, indexStats, fileSystemStats);
    IndexRepository newRepo = manager.getRepository(userRegion, 0, null);

    Assert.assertNotEquals(newRepo, repo);
  }




  private void verifyIndexFinishFlushing(String indexName, String regionName) {
    LuceneIndex index = LuceneServiceProvider.get(cache).getIndex(indexName, regionName);
    boolean flushed = index.waitUntilFlushed(60000);
    assertTrue(flushed);
  }
}
