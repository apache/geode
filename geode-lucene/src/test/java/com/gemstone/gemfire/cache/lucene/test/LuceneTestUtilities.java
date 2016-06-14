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
package com.gemstone.gemfire.cache.lucene.test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexForPartitionedRegion;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;

public class LuceneTestUtilities {
  public static final String INDEX_NAME = "index";
  public static final String REGION_NAME = "region";
  public static final String DEFAULT_FIELD = "text";

  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS = "Cannot create Lucene index index on region /region with fields [field1, field2] because another member defines the same index with fields [field1].";
  public static final String CANNOT_CREATE_LUCENE_INDEX_NO_ANALYZER_FIELD2 = "Cannot create Lucene index index on region /region with no analyzer on field field2 because another member defines the same index with analyzer org.apache.lucene.analysis.core.KeywordAnalyzer on that field.";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZER_SIZES_1 = "Cannot create Lucene index index on region /region with field analyzers {field2=class org.apache.lucene.analysis.core.KeywordAnalyzer, field1=class org.apache.lucene.analysis.core.KeywordAnalyzer} because another member defines the same index with field analyzers {field1=class org.apache.lucene.analysis.core.KeywordAnalyzer}.";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZER_SIZES_2 = "Cannot create Lucene index index on region /region with field analyzers {field1=class org.apache.lucene.analysis.core.KeywordAnalyzer, field2=class org.apache.lucene.analysis.core.KeywordAnalyzer} because another member defines the same index with field analyzers {field1=class org.apache.lucene.analysis.core.KeywordAnalyzer}.";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS = "Cannot create Lucene index index on region /region with analyzer org.apache.lucene.analysis.core.KeywordAnalyzer on field field1 because another member defines the same index with analyzer org.apache.lucene.analysis.standard.StandardAnalyzer on that field.";
  public static final String CANNOT_CREATE_LUCENE_INDEX_NO_ANALYZER_FIELD1 = "Cannot create Lucene index index on region /region with no analyzer on field field1 because another member defines the same index with analyzer org.apache.lucene.analysis.core.KeywordAnalyzer on that field.";
  public static final String CANNOT_CREATE_LUCENE_INDEX_NO_ANALYZER_EXISTING_MEMBER = "Cannot create Lucene index index on region /region with analyzer org.apache.lucene.analysis.core.KeywordAnalyzer on field field1 because another member defines the same index with no analyzer on that field.";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_NAMES = "Cannot create Region /region with [index2#_region] async event ids because another cache has the same region defined with [index1#_region] async event ids";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_INDEXES_1 = "Cannot create Region /region with [] async event ids because another cache has the same region defined with [index#_region] async event ids";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_INDEXES_2 = "Cannot create Region /region with [index#_region, index2#_region] async event ids because another cache has the same region defined with [index#_region] async event ids";

  public static void verifyInternalRegions(LuceneService luceneService, Cache cache, Consumer<LocalRegion> verify) {
    // Get index
    LuceneIndexForPartitionedRegion index = (LuceneIndexForPartitionedRegion) luceneService.getIndex(INDEX_NAME, REGION_NAME);

    // Verify the meta regions exist and are internal
    LocalRegion chunkRegion = (LocalRegion) cache.getRegion(index.createChunkRegionName());
    LocalRegion fileRegion = (LocalRegion) cache.getRegion(index.createFileRegionName());
    verify.accept(chunkRegion);
    verify.accept(fileRegion);
  }

  public static AsyncEventQueue getIndexQueue(Cache cache) {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
    return cache.getAsyncEventQueue(aeqId);
  }

  public static void createIndex(Cache cache, String... fieldNames) {
    LuceneServiceProvider.get(cache).createIndex(INDEX_NAME, REGION_NAME, fieldNames);
  }

  public static void verifyIndexFinishFlushing(Cache cache, String indexName, String regionName) {
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    LuceneIndex index = luceneService.getIndex(indexName, regionName);
    boolean flushed = index.waitUntilFlushed(60000);
    assertTrue(flushed);
  }

  /**
   * Verify that a query returns the expected list of keys. Ordering is ignored.
   */
  public static <K> void  verifyQueryKeys(LuceneQuery<K,Object> query,K ... expectedKeys) {
    Set<K> expectedKeySet = new HashSet<>(Arrays.asList(expectedKeys));
    Set<K> actualKeySet = new HashSet<>();
    final LuceneQueryResults<K, Object> results = query.search();
    while(results.hasNextPage()) {
      results.getNextPage().stream()
        .forEach(struct -> actualKeySet.add(struct.getKey()));
    }
    assertEquals(expectedKeySet, actualKeySet);
  }

  /**
   * Verify that a query returns the expected map of key-value. Ordering is ignored.
   */
  public static <K> void verifyQueryKeyAndValues(LuceneQuery<K,Object> query, HashMap expectedResults) {
    HashMap actualResults = new HashMap<>();
    final LuceneQueryResults<K, Object> results = query.search();
    while(results.hasNextPage()) {
      results.getNextPage().stream()
        .forEach(struct -> {
          Object value = struct.getValue();
          if (value instanceof PdxInstance) {
            PdxInstance pdx = (PdxInstance)value;
            String jsonString = JSONFormatter.toJSON(pdx);
            actualResults.put(struct.getKey(), pdx);
          } else {
            actualResults.put(struct.getKey(), value);
          }
        });
    }
    assertEquals(expectedResults, actualResults);
  }
  
  public static void pauseSender(final Cache cache) {
    final AsyncEventQueueImpl queue = (AsyncEventQueueImpl) getIndexQueue(cache);
    queue.getSender().pause();
  }

  public static void resumeSender(final Cache cache) {
    final AsyncEventQueueImpl queue = (AsyncEventQueueImpl) getIndexQueue(cache);
    queue.getSender().resume();
  }
}
