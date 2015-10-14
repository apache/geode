package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class LuceneRebalanceJUnitTest {
  String[] indexedFields = new String[] { "txt" };
  HeterogenousLuceneSerializer mapper = new HeterogenousLuceneSerializer(indexedFields);
  Analyzer analyzer = new StandardAnalyzer();

  @Before
  public void setup() {
    indexedFields = new String[] { "txt" };
    mapper = new HeterogenousLuceneSerializer(indexedFields);
    analyzer = new StandardAnalyzer();
    LuceneServiceImpl.registerDataSerializables();
  }
  
  @After
  public void tearDown() {
    Cache cache = GemFireCacheImpl.getInstance();
    if(cache != null) {
      cache.close();
    }
  }

  /**
   * Test what happens when a bucket is destroyed.
   */
  @Test
  public void recoverRepoInANewNode() throws BucketNotFoundException, IOException {
    Cache cache = new CacheFactory().set("mcast-port", "0").create();
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
}
