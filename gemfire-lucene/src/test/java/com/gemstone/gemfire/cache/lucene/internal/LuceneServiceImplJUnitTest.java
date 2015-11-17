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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
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
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
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
    new LuceneServiceImpl().init(cache);

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
  
  /**Test that we don't allow the user
   * to create the region first.
   */
  @Test(expected = IllegalStateException.class)
  public void createRegionFirst() throws IOException, ParseException {
    getService();
    LocalRegion userRegion = createPR("PR1", false);
    service.createIndex("index1", "PR1", "field1", "field2", "field3");
  }

  @Test
  public void testCreateIndexForPR() throws IOException, ParseException {
    getService();
    service.createIndex("index1", "PR1", "field1", "field2", "field3");
    LocalRegion userRegion = createPR("PR1", false);
    LuceneIndexImpl index1 = (LuceneIndexImpl) service.getIndex("index1", "PR1");
    assertTrue(index1 instanceof LuceneIndexForPartitionedRegion);
    LuceneIndexForPartitionedRegion index1PR = (LuceneIndexForPartitionedRegion)index1;
    assertEquals("index1", index1.getName());
    assertEquals("/PR1", index1.getRegionPath());
    String[] fields1 = index1.getFieldNames();
    assertEquals(3, fields1.length);
    Analyzer analyzer = index1PR.getAnalyzer();
    assertTrue(analyzer instanceof StandardAnalyzer);
    RepositoryManager RepositoryManager = index1PR.getRepositoryManager();
    assertTrue(RepositoryManager != null);
   
    final String fileRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "/PR1")+".files";
    final String chunkRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "/PR1")+".chunks";
    PartitionedRegion filePR = (PartitionedRegion)cache.getRegion(fileRegionName);
    PartitionedRegion chunkPR = (PartitionedRegion)cache.getRegion(chunkRegionName);
    assertTrue(filePR != null);
    assertTrue(chunkPR != null);
    
    String aeqId = LuceneServiceImpl.getUniqueIndexName(index1.getName(), index1.getRegionPath());
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl)cache.getAsyncEventQueue(aeqId);
    assertTrue(aeq != null);
    
    //Make sure our queue doesn't show up in the list of async event queues 
    assertEquals(Collections.emptySet(), cache.getAsyncEventQueues());
  }

  @Test
  public void testCreateIndexForPRWithAnalyzer() throws IOException, ParseException {
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
    LuceneIndexImpl index1 = (LuceneIndexImpl)service.getIndex("index1", "PR1");
    assertTrue(index1 instanceof LuceneIndexForPartitionedRegion);
    LuceneIndexForPartitionedRegion index1PR = (LuceneIndexForPartitionedRegion)index1;
    assertEquals("index1", index1.getName());
    assertEquals("/PR1", index1.getRegionPath());
    String[] fields1 = index1.getFieldNames();
    assertEquals(3, fields1.length);
    Analyzer analyzer = index1PR.getAnalyzer();
    assertTrue(analyzer instanceof PerFieldAnalyzerWrapper);
    RepositoryManager RepositoryManager = index1PR.getRepositoryManager();
    assertTrue(RepositoryManager != null);
   
    final String fileRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "/PR1")+".files";
    final String chunkRegionName = LuceneServiceImpl.getUniqueIndexName("index1", "/PR1")+".chunks";
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
