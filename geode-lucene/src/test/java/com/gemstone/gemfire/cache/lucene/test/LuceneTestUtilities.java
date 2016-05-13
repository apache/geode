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

import static org.junit.Assert.assertTrue;

import java.util.function.Consumer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexForPartitionedRegion;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;

public class LuceneTestUtilities {
  public static final String INDEX_NAME = "index";
  public static final String REGION_NAME = "region";

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
}
