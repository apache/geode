/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package com.gemstone.gemfire.cache30;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.DiskStoreAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.test.dunit.Assert;

/**
 * @author dsmith
 *
 */
public class CacheXml80DUnitTest extends CacheXml70DUnitTest {
  private static final long serialVersionUID = 225193925777688541L;

  public CacheXml80DUnitTest(String name) {
    super(name);
  }

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_8_0;
  }

  @SuppressWarnings("rawtypes")
  public void testCompressor() {
    final String regionName = "testCompressor";
    
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setCompressor(SnappyCompressor.getDefaultInstance());
    /* Region regionBefore = */ cache.createRegion(regionName, attrs);
    
    testXml(cache);
    
    final Cache c = getCache();
    assertNotNull(c);

    final Region regionAfter = c.getRegion(regionName);
    assertNotNull(regionAfter);
    assertTrue(SnappyCompressor.getDefaultInstance().equals(regionAfter.getAttributes().getCompressor()));
    regionAfter.localDestroyRegion();
  }
  
  /*
   * Tests xml creation for indexes
   * First creates 3 indexes and makes sure the cache creates all 3
   * Creates a 4th through the api and writes out the xml
   * Restarts the cache with the new xml
   * Makes sure the new cache has the 4 indexes
   */
  public void testIndexXmlCreation() throws Exception {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    cache.createRegion("replicated", attrs);
   
    cache.getQueryService().createIndex("crIndex", "CR_ID", "/replicated");
    cache.getQueryService().createHashIndex("hashIndex", "HASH_ID", "/replicated");
    cache.getQueryService().createKeyIndex("primaryKeyIndex", "ID", "/replicated");
    
    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);
    QueryService qs = c.getQueryService();
    Collection<Index> indexes = qs.getIndexes();
    assertEquals(3, indexes.size());
    c.getQueryService().createIndex("crIndex2", "r.CR_ID_2", "/replicated r");
    c.getQueryService().createIndex("rIndex", "r.R_ID", "/replicated r, r.positions.values rv");

    File dir = new File("XML_" + this.getGemFireVersion());
    dir.mkdirs();
    File file = new File(dir, "actual-" + this.getUniqueName() + ".xml");
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(c, pw, this.getUseSchema(), this.getGemFireVersion());
      pw.close();
    }
    catch (IOException ex) {
      Assert.fail("IOException during cache.xml generation to " + file, ex);
    }
    
    // Get index info before closing cache.
    indexes = qs.getIndexes();
    
    c.close();
    GemFireCacheImpl.testCacheXml = file;
    assert(c.isClosed());
    
    c = getCache();
    qs = c.getQueryService();
    Collection<Index> newIndexes = qs.getIndexes();
    assertEquals(5, newIndexes.size());
    
    Region r = c.getRegion("/replicated");
    for (int i = 0; i < 5;i++) {
      r.put(i, new TestObject(i));
    }

    // Validate to see, newly created indexes match the initial configuration
    for (Index index : indexes) {
      Index newIndex = qs.getIndex(r, index.getName());
      assertEquals ("Index from clause is not same for index " + index.getName(), newIndex.getFromClause(), index.getFromClause());
      assertEquals ("Index expression is not same for index " + index.getName(), newIndex.getIndexedExpression(), index.getIndexedExpression());
    }

    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    SelectResults results = (SelectResults) qs.newQuery("select * from /replicated r where r.ID = 1").execute();
    assertEquals(1, results.size());
    assertTrue(checkIndexUsed(observer, "primaryKeyIndex"));
    observer.reset();
    
    results = (SelectResults) qs.newQuery("select * from /replicated r where r.CR_ID = 1").execute();
    assertEquals(2, results.size());
    assertTrue(checkIndexUsed(observer, "crIndex"));
    observer.reset();
    
    results = (SelectResults) qs.newQuery("select * from /replicated r where r.CR_ID_2 = 1").execute();
    assertEquals(2, results.size());
    assertTrue(checkIndexUsed(observer, "crIndex2"));
    observer.reset();
    
    results = (SelectResults) qs.newQuery("select * from /replicated r, r.positions.values rv where r.R_ID > 1").execute();
    assertEquals(3, results.size());
    assertTrue(checkIndexUsed(observer, "rIndex"));
    observer.reset();
    
    results = (SelectResults) qs.newQuery("select * from /replicated r where r.HASH_ID = 1").execute();
    assertEquals(1, results.size());
    assertTrue(checkIndexUsed(observer, "hashIndex"));
    observer.reset();
  }
  
  public void testCacheServerDisableTcpNoDelay()
      throws CacheException
  {
    CacheCreation cache = new CacheCreation();

    CacheServer cs = cache.addCacheServer();
    cs.setTcpNoDelay(false);
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
  }

  
  public void testCacheServerEnableTcpNoDelay()
      throws CacheException
  {
    CacheCreation cache = new CacheCreation();

    CacheServer cs = cache.addCacheServer();
    cs.setTcpNoDelay(true);
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
  }

  public void testDiskUsage() {
    CacheCreation cache = new CacheCreation();
    
    
    DiskStoreAttributesCreation disk = new DiskStoreAttributesCreation();
    disk.setDiskUsageWarningPercentage(97);
    disk.setDiskUsageCriticalPercentage(98);
    disk.setName("mydisk");
    cache.addDiskStore(disk);
    
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    attrs.setDiskStoreName("mydisk");
    
    cache.createVMRegion("whatever", attrs);
    testXml(cache);
  }
  
  private boolean checkIndexUsed(QueryObserverImpl observer, String indexName) {
    return observer.isIndexesUsed && observer.indexName.equals(indexName);
  }
  
  private class QueryObserverImpl extends QueryObserverAdapter {

    boolean isIndexesUsed = false;

    ArrayList<String> indexesUsed = new ArrayList<String>();

    String indexName;

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexName = index.getName();
      indexesUsed.add(index.getName());
     
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }

    public int numIndexesUsed() {
      return indexesUsed.size();
    }
    
    public void reset() {
      indexName = null;
      isIndexesUsed = false;
      indexesUsed.clear();
    }
  }
  
  private class TestObject {
    public int CR_ID;
    public int CR_ID_2;
    public int R_ID;
    public int HASH_ID;
    public int ID;
    public Map positions;
    
    public TestObject(int ID) {
     this.ID = ID;
     CR_ID = ID % 2;
     CR_ID_2 = ID % 2;
     R_ID = ID;
     HASH_ID = ID;
     positions = new HashMap();
     positions.put(ID, "TEST_STRING");
    }
    public int getCR_ID() {
      return CR_ID;
    }
    
    public int getCR_ID_2() {
      return CR_ID_2;
    }
    
    public int getR_ID() {
      return R_ID;
    }
    
    public int ID() {
      return ID;
    }
    
    public int getHASH_ID() {
      return HASH_ID;
    }
  }
}
