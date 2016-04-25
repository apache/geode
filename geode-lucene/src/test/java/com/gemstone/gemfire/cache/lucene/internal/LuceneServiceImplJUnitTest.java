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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunction;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneServiceImplJUnitTest {

  Cache cache;
  ClientCache clientCache;
  private LuceneIndexImpl repo;
  private HeterogenousLuceneSerializer mapper;
  private StandardAnalyzer analyzer = new StandardAnalyzer();
  private IndexWriter writer;
  LuceneServiceImpl service = null;
  private static final Logger logger = LogService.getLogger();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void luceneServiceProviderGetShouldAcceptClientCacheAsAParameter(){
    clientCache = getClientCache();
    LuceneService luceneService = LuceneServiceProvider.get(clientCache);
    assertNotNull(luceneService);
  }

  // lucene service will register query execution function on initialization
  @Test
  public void shouldRegisterQueryFunction() {
    Function function = FunctionService.getFunction(LuceneFunction.ID);
    assertNull(function);

    cache = getCache();
    new LuceneServiceImpl().init(cache);

    function = FunctionService.getFunction(LuceneFunction.ID);
    assertNotNull(function);
  }

  @After
  public void destroyService() {
    if (null != service) {
      service = null;
    }
  }

  @After
  public void destroyCache() {
    if (null != cache && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
    if (null != clientCache  && !clientCache.isClosed()) {
      clientCache.close();
      clientCache = null;
    }
  }

  private ClientCache getClientCache() {
    if (null == clientCache) {
      clientCache = new ClientCacheFactory().set("mcast-port", "0").create();
    }
    else{
      return clientCache;
    }
    return clientCache;
  }

  private Cache getCache() {
    if (null == cache) {
      cache = new CacheFactory().set("mcast-port", "0").create();
    }
    return cache;
  }

  private LuceneService getService() {
    if (null == cache) {
      getCache();
    }
    if (null == service) {
      service = (LuceneServiceImpl)LuceneServiceProvider.get(cache);
    }
    return service;
  }


  private Region createRegion(String regionName, RegionShortcut shortcut) {
    return cache.createRegionFactory(shortcut).create(regionName);
  }

  private LocalRegion createPR(String regionName, boolean isSubRegion) {
    if (isSubRegion) {
      LocalRegion root = (LocalRegion) cache.createRegionFactory(RegionShortcut.PARTITION).create("root");
      LocalRegion region = (LocalRegion) cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).
        createSubregion(root, regionName);
      return region;
    }
    else {
      LocalRegion region = (LocalRegion) createRegion(regionName, RegionShortcut.PARTITION_PERSISTENT);
      return region;
    }
  }

  private LocalRegion createRR(String regionName, boolean isSubRegion) {
    if (isSubRegion) {

      LocalRegion root = (LocalRegion) cache.createRegionFactory(RegionShortcut.REPLICATE).create("root");
      LocalRegion region = (LocalRegion) cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).
        createSubregion(root, regionName);
      return region;
    }
    else {
      LocalRegion region = (LocalRegion) createRegion(regionName, RegionShortcut.REPLICATE_PERSISTENT);
      return region;
    }
  }

  @Test(expected = IllegalStateException.class)
  public void cannotCreateLuceneIndexAfterRegionHasBeenCreated() throws IOException, ParseException {
    getService();

    LocalRegion userRegion = createPR("PR1", false);
    service.createIndex("index1", "PR1", "field1", "field2", "field3");
  }

  @Test
  public void canCreateLuceneIndexForPRWithAnalyzer() throws IOException, ParseException {
    getService();
    StandardAnalyzer sa = new StandardAnalyzer();
    KeywordAnalyzer ka = new KeywordAnalyzer();
    Map<String, Analyzer> analyzerPerField = new HashMap<String, Analyzer>();
    analyzerPerField.put("field1", ka);
    analyzerPerField.put("field2", sa);
    analyzerPerField.put("field3", sa);
    //  field2 and field3 will use StandardAnalyzer
    PerFieldAnalyzerWrapper analyzer2 = new PerFieldAnalyzerWrapper(sa, analyzerPerField);

    service.createIndex("index1", "PR1", analyzerPerField);
    createPR("PR1", false);
    LuceneIndexImpl index1 = (LuceneIndexImpl) service.getIndex("index1", "PR1");
    assertTrue(index1 instanceof LuceneIndexForPartitionedRegion);
    LuceneIndexForPartitionedRegion index1PR = (LuceneIndexForPartitionedRegion) index1;
    assertEquals("index1", index1.getName());
    assertEquals("/PR1", index1.getRegionPath());
    String[] fields1 = index1.getFieldNames();
    assertEquals(3, fields1.length);
    Analyzer analyzer = index1PR.getAnalyzer();
    assertTrue(analyzer instanceof PerFieldAnalyzerWrapper);
    RepositoryManager RepositoryManager = index1PR.getRepositoryManager();
    assertTrue(RepositoryManager != null);

    final String fileRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "/PR1") + ".files";
    final String chunkRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "/PR1") + ".chunks";
    PartitionedRegion filePR = (PartitionedRegion) cache.getRegion(fileRegionName);
    PartitionedRegion chunkPR = (PartitionedRegion) cache.getRegion(chunkRegionName);
    assertTrue(filePR != null);
    assertTrue(chunkPR != null);
  }

  @Test
  public void cannotCreateLuceneIndexForReplicateRegion() throws IOException, ParseException {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Lucene indexes on replicated regions are not supported");
    getService();
    service.createIndex("index1", "RR1", "field1", "field2", "field3");
    createRR("RR1", false);
  }

  @Test
  public void canCreateIndexForAllNonProxyPartitionRegionTypes() {
    for (RegionShortcut shortcut : RegionShortcut.values()) {
      String sname = shortcut.name().toLowerCase();
      if (sname.contains("partition") && !sname.contains("proxy")) {
        canCreateLuceneIndexForPRType(shortcut);
        //Destroying cache and service for now because aeq's are not completely being cleaned up correctly after
        // being destroyed.  Instead we should close the aeq and clean up any regions associated with this lucene
        //index but only after aeq destroy works properly
        destroyCache();
        destroyService();
      }
    }
  }

  public void canCreateLuceneIndexForPRType(RegionShortcut regionShortcut) {
    getService();
    service.createIndex("index1", "PR1", "field1", "field2", "field3");
    Region region = null;
    AsyncEventQueueImpl aeq = null;
    try {
      region = createRegion("PR1", regionShortcut);
      LuceneIndexImpl index1 = (LuceneIndexImpl) service.getIndex("index1", "PR1");
      assertTrue(index1 instanceof LuceneIndexForPartitionedRegion);
      LuceneIndexForPartitionedRegion index1PR = (LuceneIndexForPartitionedRegion) index1;
      assertEquals("index1", index1.getName());
      assertEquals("/PR1", index1.getRegionPath());
      String[] fields1 = index1.getFieldNames();
      assertEquals(3, fields1.length);
      Analyzer analyzer = index1PR.getAnalyzer();
      assertTrue(analyzer instanceof StandardAnalyzer);
      RepositoryManager RepositoryManager = index1PR.getRepositoryManager();
      assertTrue(RepositoryManager != null);

      final String fileRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "/PR1") + ".files";
      final String chunkRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "/PR1") + ".chunks";
      PartitionedRegion filePR = (PartitionedRegion) cache.getRegion(fileRegionName);
      PartitionedRegion chunkPR = (PartitionedRegion) cache.getRegion(chunkRegionName);
      assertTrue(filePR != null);
      assertTrue(chunkPR != null);

      String aeqId = LuceneServiceImpl.getUniqueIndexName(index1.getName(), index1.getRegionPath());
      aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
      assertTrue(aeq != null);

      //Make sure our queue doesn't show up in the list of async event queues
      assertEquals(Collections.emptySet(), cache.getAsyncEventQueues());
    }
    finally {
      String aeqId = LuceneServiceImpl.getUniqueIndexName("index1", "PR1");
      PartitionedRegion chunkRegion = (PartitionedRegion) cache.getRegion(aeqId + ".chunks");
      if (chunkRegion != null) {
        chunkRegion.destroyRegion();
      }
      PartitionedRegion fileRegion = (PartitionedRegion) cache.getRegion(aeqId + ".files");
      if (fileRegion != null) {
        fileRegion.destroyRegion();
      }
      ((GemFireCacheImpl) cache).removeAsyncEventQueue(aeq);
      if (aeq != null) {
        aeq.destroy();
      }
      region.destroyRegion();
    }
  }

}
