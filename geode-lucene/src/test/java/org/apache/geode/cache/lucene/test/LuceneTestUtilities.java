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
package org.apache.geode.cache.lucene.test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.PageableLuceneQueryResults;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneIndexForPartitionedRegion;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;

public class LuceneTestUtilities {
  public static final String INDEX_NAME = "index";
  public static final String REGION_NAME = "region";
  public static final String DEFAULT_FIELD = "text";

  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS = "Cannot create Lucene index index on region /region with fields [field1, field2] because another member defines the same index with fields [field1].";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS_2 = "Cannot create Lucene index index on region /region with fields [field1] because another member defines the same index with fields [field1, field2].";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS = "Cannot create Lucene index index on region /region with analyzer StandardAnalyzer on field field2 because another member defines the same index with analyzer KeywordAnalyzer on that field.";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_1 = "Cannot create Lucene index index on region /region with analyzer StandardAnalyzer on field field1 because another member defines the same index with analyzer KeywordAnalyzer on that field.";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_2 = "Cannot create Lucene index index on region /region with analyzer KeywordAnalyzer on field field1 because another member defines the same index with analyzer StandardAnalyzer on that field.";
  public static final String CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_3 = "Cannot create Lucene index index on region /region with analyzer KeywordAnalyzer on field field2 because another member defines the same index with analyzer StandardAnalyzer on that field.";
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
  public static <K> void  verifyQueryKeys(LuceneQuery<K,Object> query,K ... expectedKeys) throws LuceneQueryException {
    Set<K> expectedKeySet = new HashSet<>(Arrays.asList(expectedKeys));
    Set<K> actualKeySet = new HashSet<>(query.findKeys());
    assertEquals(expectedKeySet, actualKeySet);
  }

  /**
   * Verify that a query returns the expected map of key-value. Ordering is ignored.
   */
  public static <K> void verifyQueryKeyAndValues(LuceneQuery<K,Object> query, HashMap expectedResults) throws LuceneQueryException {
    HashMap actualResults = new HashMap<>();
    final PageableLuceneQueryResults<K, Object> results = query.findPages();
    while(results.hasNext()) {
      results.next().stream()
        .forEach(struct -> {
          Object value = struct.getValue();
            actualResults.put(struct.getKey(), value);
        });
    }
    assertEquals(expectedResults, actualResults);
  }
  
  public static void pauseSender(final Cache cache) {
    final AsyncEventQueueImpl queue = (AsyncEventQueueImpl) getIndexQueue(cache);
    queue.getSender().pause();

    AbstractGatewaySender sender = (AbstractGatewaySender) queue.getSender();
    sender.getEventProcessor().waitForDispatcherToPause();
  }

  public static void resumeSender(final Cache cache) {
    final AsyncEventQueueImpl queue = (AsyncEventQueueImpl) getIndexQueue(cache);
    queue.getSender().resume();
  }
}
