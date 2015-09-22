package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunction;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneServiceImplJUnitTest {
  Cache cache;
  private LuceneIndexImpl repo;
  private HeterogenousLuceneSerializer mapper;
  private StandardAnalyzer analyzer = new StandardAnalyzer();
  private IndexWriter writer;
  LuceneServiceImpl service = null;
  private static final Logger logger = LogService.getLogger();
  
  // lucene service will register query execution function on initialization
  @Test
  public void shouldRegisterQueryFunction() {
    Function function = FunctionService.getFunction(LuceneFunction.ID);
    assertNull(function);

    cache = createBasicCache();
    new LuceneServiceImpl(cache);

    function = FunctionService.getFunction(LuceneFunction.ID);
    assertNotNull(function);
  }
  
  private GemFireCacheImpl createBasicCache() {
    return (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").create();
  }

  @After
  public void destroyCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
  }
  
  private void getCache() {
    try {
       cache = CacheFactory.getAnyInstance();
    } catch (Exception e) {
      //ignore
    }
    if (null == cache) {
      cache = createBasicCache();
    }
  }
  
  private void getService() {
    if (cache == null) {
      getCache();
    }
    if (service == null) {
      service = (LuceneServiceImpl)LuceneServiceProvider.get(cache);
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
    getService();
    createPR("PR1", false);
    LuceneIndexImpl index1 = (LuceneIndexImpl)service.createIndex("index1", "PR1", "field1", "field2", "field3");
    assertTrue(index1 instanceof LuceneIndexForPartitionedRegion);
    LuceneIndexForPartitionedRegion index1PR = (LuceneIndexForPartitionedRegion)index1;
    assertEquals("index1", index1.getName());
    assertEquals("PR1", index1.getRegionPath());
    String[] fields1 = index1.getFieldNames();
    assertEquals(3, fields1.length);
   
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
