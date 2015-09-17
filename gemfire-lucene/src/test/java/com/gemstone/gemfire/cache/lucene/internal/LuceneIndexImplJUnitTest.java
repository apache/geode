package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.directory.RegionDirectory;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.Type2;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test of the {@link IndexRepository} and everything below
 * it. This tests that we can save gemfire objects or PDXInstance
 * objects into a lucene index and search for those objects later.
 */
@Category(IntegrationTest.class)
public class LuceneIndexImplJUnitTest {

  private LuceneIndexImpl repo;
  private HeterogenousLuceneSerializer mapper;
  private StandardAnalyzer analyzer = new StandardAnalyzer();
  private IndexWriter writer;

  Cache cache = null;
  LuceneServiceImpl service = null;
  
  @Before
  public void setUp() throws IOException {
    if (cache == null) {
      getCache();
    }
    if (service == null) {
      service = (LuceneServiceImpl)LuceneServiceProvider.get(cache);
    }
    
  }
  
  private void getCache() {
    try {
       cache = CacheFactory.getAnyInstance();
    } catch (Exception e) {
      //ignore
    }
    if (null == cache) {
      cache = new CacheFactory().set("mcast-port", "0").set("log-level", "error").create();
      cache.getLogger().info("Created cache in test");
    }
  }
  
  private LocalRegion createPR(String regionName, boolean isSubRegion) {
    if (isSubRegion) {
      LocalRegion root = (LocalRegion)cache.createRegionFactory(RegionShortcut.REPLICATE).create("root");
      LocalRegion region = (LocalRegion)cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).
          createSubregion(root, regionName);
      return region;
    } else {
      LocalRegion root = (LocalRegion)cache.createRegionFactory(RegionShortcut.REPLICATE).create("root");
      LocalRegion region = (LocalRegion)cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).
          create(regionName);
      return region;
    }
  }

  private LocalRegion createRR(String regionName, boolean isSubRegion) {
    if (isSubRegion) {
      LocalRegion root = (LocalRegion)cache.createRegionFactory(RegionShortcut.REPLICATE).create("root");
      LocalRegion region = (LocalRegion)cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).
          createSubregion(root, regionName);
      return region;
    } else {
      LocalRegion root = (LocalRegion)cache.createRegionFactory(RegionShortcut.REPLICATE).create("root");
      LocalRegion region = (LocalRegion)cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).
          create(regionName);
      return region;
    }
  }

  @Test
  public void testCreateIndexForPR() throws IOException, ParseException {
    createPR("PR1", false);
    LuceneIndexImpl index1 = (LuceneIndexImpl)service.createIndex("index1", "PR1", "field1", "field2", "field3");
    assertTrue(index1 instanceof LuceneIndexForPartitionedRegion);
    LuceneIndexForPartitionedRegion index1PR = (LuceneIndexForPartitionedRegion)index1;
    assertEquals("index1", index1.getName());
    assertEquals("PR1", index1.getRegionPath());
    String[] fields1 = index1.getFieldNames();
    String[] pdxfields1 = index1.getPDXFieldNames();
    assertEquals(3, fields1.length);
    assertEquals(3, pdxfields1.length);
   
    final String fileRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "PR1")+".files";
    final String chunkRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "PR1")+".chunks";
    PartitionedRegion filePR = (PartitionedRegion)cache.getRegion(fileRegionName);
    PartitionedRegion chunkPR = (PartitionedRegion)cache.getRegion(chunkRegionName);
    assertTrue(filePR != null);
    assertTrue(chunkPR != null);
  }

  @Test
  public void testCreateIndexForRR() throws IOException, ParseException {
//    service.createIndex("index1", "RR1", "field1", "field2", "field3");
  
    
  }
  
}
