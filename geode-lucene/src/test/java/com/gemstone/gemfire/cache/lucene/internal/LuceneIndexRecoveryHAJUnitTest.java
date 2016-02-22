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

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.Type1;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class LuceneIndexRecoveryHAJUnitTest {
  private static final String INDEX = "index";
  private static final String REGION = "indexedRegion";
  String[] indexedFields = new String[] { "txt" };
  HeterogenousLuceneSerializer mapper = new HeterogenousLuceneSerializer(indexedFields);
  Analyzer analyzer = new StandardAnalyzer();

  Cache cache;

  @Before
  public void setup() {
    indexedFields = new String[] { "txt" };
    mapper = new HeterogenousLuceneSerializer(indexedFields);
    analyzer = new StandardAnalyzer();
    LuceneServiceImpl.registerDataSerializables();

    cache = new CacheFactory().set("mcast-port", "0").create();
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

    RepositoryManager manager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, mapper, analyzer);
    IndexRepository repo = manager.getRepository(userRegion, 0, null);
    assertNotNull(repo);

    repo.create("rebalance", "test");
    repo.commit();

    // close the region to simulate bucket movement. New node will create repo using data persisted by old region
    userRegion.close();

    userRegion = (PartitionedRegion) regionfactory.create("userRegion");
    userRegion.put("rebalance", "test");
    manager = new PartitionedRepositoryManager(userRegion, fileRegion, chunkRegion, mapper, analyzer);
    IndexRepository newRepo = manager.getRepository(userRegion, 0, null);

    Assert.assertNotEquals(newRepo, repo);
  }

  @Test
  public void recoverPersistentIndex() throws Exception {
    LuceneService service = LuceneServiceProvider.get(cache);
    service.createIndex(INDEX, REGION, Type1.fields);

    RegionFactory<String, Type1> regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
    Region<String, Type1> userRegion = regionFactory.create(REGION);

    Type1 value = new Type1("hello world", 1, 2L, 3.0, 4.0f);
    userRegion.put("value1", value);
    value = new Type1("test world", 1, 2L, 3.0, 4.0f);
    userRegion.put("value2", value);
    value = new Type1("lucene world", 1, 2L, 3.0, 4.0f);
    userRegion.put("value3", value);

    // TODO flush queue
    TimeUnit.MILLISECONDS.sleep(500);

    LuceneQuery<Integer, Type1> query = service.createLuceneQueryFactory().create(INDEX, REGION, "s:world");
    LuceneQueryResults<Integer, Type1> results = query.search();
    Assert.assertEquals(3, results.size());

    // close the cache and all the regions
    cache.close();

    cache = new CacheFactory().set("mcast-port", "0").create();
    service = LuceneServiceProvider.get(cache);
    service.createIndex(INDEX, REGION, Type1.fields);
    regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
    userRegion = regionFactory.create(REGION);

    query = service.createLuceneQueryFactory().create(INDEX, REGION, "s:world");
    results = query.search();
    Assert.assertEquals(3, results.size());

    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX, REGION);
    PartitionedRegion chunkRegion = (PartitionedRegion) cache.getRegion(aeqId + ".chunks");
    assertNotNull(chunkRegion);
    chunkRegion.destroyRegion();
    PartitionedRegion fileRegion = (PartitionedRegion) cache.getRegion(aeqId + ".files");
    assertNotNull(fileRegion);
    fileRegion.destroyRegion();
    userRegion.destroyRegion();
  }

  @Test
  public void overflowRegionIndex() throws Exception {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX, REGION);

    LuceneService service = LuceneServiceProvider.get(cache);
    service.createIndex(INDEX, REGION, Type1.fields);

    RegionFactory<String, Type1> regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    EvictionAttributesImpl evicAttr = new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
    evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
    regionFactory.setEvictionAttributes(evicAttr);

    PartitionedRegion userRegion = (PartitionedRegion) regionFactory.create(REGION);
    Assert.assertEquals(0, userRegion.getDiskRegionStats().getNumOverflowOnDisk());

    Type1 value = new Type1("hello world", 1, 2L, 3.0, 4.0f);
    userRegion.put("value1", value);
    value = new Type1("test world", 1, 2L, 3.0, 4.0f);
    userRegion.put("value2", value);
    value = new Type1("lucene world", 1, 2L, 3.0, 4.0f);
    userRegion.put("value3", value);

    // TODO flush queue
    TimeUnit.MILLISECONDS.sleep(500);

    PartitionedRegion fileRegion = (PartitionedRegion) cache.getRegion(aeqId + ".files");
    assertNotNull(fileRegion);
    PartitionedRegion chunkRegion = (PartitionedRegion) cache.getRegion(aeqId + ".chunks");
    assertNotNull(chunkRegion);
    Assert.assertTrue(0 < userRegion.getDiskRegionStats().getNumOverflowOnDisk());

    LuceneQuery<Integer, Type1> query = service.createLuceneQueryFactory().create(INDEX, REGION, "s:world");
    LuceneQueryResults<Integer, Type1> results = query.search();
    Assert.assertEquals(3, results.size());
  }
}
