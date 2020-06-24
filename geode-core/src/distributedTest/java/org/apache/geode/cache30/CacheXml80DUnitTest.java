/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache30;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.DiskStoreAttributesCreation;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;


@SuppressWarnings("serial")
public class CacheXml80DUnitTest extends CacheXml70DUnitTest {

  @Override
  protected String getGemFireVersion() {
    return CacheXml.VERSION_8_0;
  }

  @Test
  public void testCompressor() throws Exception {
    final String regionName = "testCompressor";

    final CacheCreation cache = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setCompressor(SnappyCompressor.getDefaultInstance());
    /* Region regionBefore = */ cache.createRegion(regionName, attrs);

    testXml(cache);

    final Cache c = getCache();
    assertNotNull(c);

    final Region regionAfter = c.getRegion(regionName);
    assertNotNull(regionAfter);
    assertTrue(
        SnappyCompressor.getDefaultInstance().equals(regionAfter.getAttributes().getCompressor()));
    regionAfter.localDestroyRegion();
  }

  /**
   * Tests xml creation for indexes First creates 3 indexes and makes sure the cache creates all 3
   * Creates a 4th through the api and writes out the xml Restarts the cache with the new xml Makes
   * sure the new cache has the 4 indexes
   */
  @Test
  public void testIndexXmlCreation() throws Exception {
    CacheCreation cache = new CacheCreation(new ServiceLoaderModuleService(LogService.getLogger()));
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    cache.createRegion("replicated", attrs);

    cache.getQueryService().createIndex("crIndex", "CR_ID", SEPARATOR + "replicated");
    cache.getQueryService().createHashIndex("hashIndex", "HASH_ID", SEPARATOR + "replicated");
    cache.getQueryService().createKeyIndex("primaryKeyIndex", "ID", SEPARATOR + "replicated");

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);
    QueryService qs = c.getQueryService();
    Collection<Index> indexes = qs.getIndexes();
    assertEquals(3, indexes.size());
    c.getQueryService().createIndex("crIndex2", "r.CR_ID_2", SEPARATOR + "replicated r");
    c.getQueryService().createIndex("rIndex", "r.R_ID",
        SEPARATOR + "replicated r, r.positions.values rv");

    File dir = new File(this.temporaryFolder.getRoot(), "XML_" + this.getGemFireVersion());
    dir.mkdirs();
    File file = new File(dir, "actual-" + getUniqueName() + ".xml");

    PrintWriter pw = new PrintWriter(new FileWriter(file), true);
    CacheXmlGenerator.generate(c, pw, getUseSchema(), getGemFireVersion());
    pw.close();

    // Get index info before closing cache.
    indexes = qs.getIndexes();

    c.close();
    GemFireCacheImpl.testCacheXml = file;
    assertTrue(c.isClosed());

    c = getCache();
    qs = c.getQueryService();
    Collection<Index> newIndexes = qs.getIndexes();
    assertEquals(5, newIndexes.size());

    Region r = c.getRegion(SEPARATOR + "replicated");
    for (int i = 0; i < 5; i++) {
      r.put(i, new TestObject(i));
    }

    // Validate to see, newly created indexes match the initial configuration
    for (Index index : indexes) {
      Index newIndex = qs.getIndex(r, index.getName());
      assertEquals("Index from clause is not same for index " + index.getName(),
          newIndex.getFromClause(), index.getFromClause());
      assertEquals("Index expression is not same for index " + index.getName(),
          newIndex.getIndexedExpression(), index.getIndexedExpression());
    }

    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    SelectResults results =
        (SelectResults) qs.newQuery("select * from " + SEPARATOR + "replicated r where r.ID = 1")
            .execute();
    assertEquals(1, results.size());
    assertTrue(checkIndexUsed(observer, "primaryKeyIndex"));
    observer.reset();

    results =
        (SelectResults) qs.newQuery("select * from " + SEPARATOR + "replicated r where r.CR_ID = 1")
            .execute();
    assertEquals(2, results.size());
    assertTrue(checkIndexUsed(observer, "crIndex"));
    observer.reset();

    results =
        (SelectResults) qs
            .newQuery("select * from " + SEPARATOR + "replicated r where r.CR_ID_2 = 1").execute();
    assertEquals(2, results.size());
    assertTrue(checkIndexUsed(observer, "crIndex2"));
    observer.reset();

    results = (SelectResults) qs
        .newQuery(
            "select * from " + SEPARATOR + "replicated r, r.positions.values rv where r.R_ID > 1")
        .execute();
    assertEquals(3, results.size());
    assertTrue(checkIndexUsed(observer, "rIndex"));
    observer.reset();

    results =
        (SelectResults) qs
            .newQuery("select * from " + SEPARATOR + "replicated r where r.HASH_ID = 1").execute();
    assertEquals(1, results.size());
    assertTrue(checkIndexUsed(observer, "hashIndex"));
    observer.reset();
  }

  @Test
  public void testCacheServerDisableTcpNoDelay() throws Exception {
    CacheCreation cache = new CacheCreation(new ServiceLoaderModuleService(LogService.getLogger()));

    CacheServer cs = cache.addCacheServer();
    cs.setPort(0);
    cs.setTcpNoDelay(false);
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
  }

  @Test
  public void testCacheServerEnableTcpNoDelay() throws Exception {
    CacheCreation cache = new CacheCreation(new ServiceLoaderModuleService(LogService.getLogger()));

    CacheServer cs = cache.addCacheServer();
    cs.setPort(0);
    cs.setTcpNoDelay(true);
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
  }

  @Test
  public void testDiskUsage() throws Exception {
    CacheCreation cache = new CacheCreation(new ServiceLoaderModuleService(LogService.getLogger()));

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

  private static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    List<String> indexesUsed = new ArrayList<>();
    String indexName;

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexName = index.getName();
      indexesUsed.add(index.getName());

    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }

    public void reset() {
      indexName = null;
      isIndexesUsed = false;
      indexesUsed.clear();
    }
  }

  private static class TestObject implements Serializable {
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

    public int ID() {
      return ID;
    }
  }
}
