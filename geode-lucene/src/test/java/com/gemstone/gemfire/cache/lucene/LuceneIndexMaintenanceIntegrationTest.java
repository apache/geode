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
package com.gemstone.gemfire.cache.lucene;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexForPartitionedRegion;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexStats;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.FileSystemStats;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class LuceneIndexMaintenanceIntegrationTest extends LuceneIntegrationTest {

  private static int WAIT_FOR_FLUSH_TIME = 10000;

  @Test
  public void indexIsNotUpdatedIfTransactionHasNotCommittedYet() throws Exception {
    luceneService.createIndex(INDEX_NAME, REGION_NAME, "title", "description");

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    index.waitUntilFlushed(WAIT_FOR_FLUSH_TIME);
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "description:\"hello world\"", DEFAULT_FIELD);
    LuceneQueryResults<Integer, TestObject> results = query.search();
    assertEquals(3, results.size());

    //begin transaction
    cache.getCacheTransactionManager().begin();
    region.put("object-1", new TestObject("title 1", "updated"));
    index.waitUntilFlushed(WAIT_FOR_FLUSH_TIME);
    assertEquals(3, query.search().size());
  }

  @Test
  public void indexIsUpdatedAfterTransactionHasCommitted() throws Exception {
    luceneService.createIndex(INDEX_NAME, REGION_NAME, "title", "description");

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    index.waitUntilFlushed(WAIT_FOR_FLUSH_TIME);
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "description:\"hello world\"", DEFAULT_FIELD);
    LuceneQueryResults<Integer, TestObject> results = query.search();
    assertEquals(3, results.size());

    cache.getCacheTransactionManager().begin();
    region.put("object-1", new TestObject("title 1", "updated"));
    cache.getCacheTransactionManager().commit();
    index.waitUntilFlushed(WAIT_FOR_FLUSH_TIME);

    assertEquals(2, query.search().size());
  }

  @Test
  public void indexIsNotUpdatedAfterTransactionRollback() throws Exception {
    luceneService.createIndex(INDEX_NAME, REGION_NAME, "title", "description");

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    index.waitUntilFlushed(WAIT_FOR_FLUSH_TIME);
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "description:\"hello world\"", DEFAULT_FIELD);
    LuceneQueryResults<Integer, TestObject> results = query.search();
    assertEquals(3, results.size());

    cache.getCacheTransactionManager().begin();
    region.put("object-1", new TestObject("title 1", "updated"));
    cache.getCacheTransactionManager().rollback();
    index.waitUntilFlushed(WAIT_FOR_FLUSH_TIME);

    assertEquals(3, query.search().size());
  }

  @Test
  public void statsAreUpdatedAfterACommit() throws Exception {
    luceneService.createIndex(INDEX_NAME, REGION_NAME, "title", "description");

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndexForPartitionedRegion index = (LuceneIndexForPartitionedRegion) luceneService.getIndex(INDEX_NAME, REGION_NAME);
    index.waitUntilFlushed(WAIT_FOR_FLUSH_TIME);

    FileSystemStats fileSystemStats = index.getFileSystemStats();
    LuceneIndexStats indexStats = index.getIndexStats();
    await(() -> assertEquals(4, indexStats.getDocuments()));
    await(() -> assertTrue(fileSystemStats.getFiles() > 0));
    await(() -> assertTrue(fileSystemStats.getChunks() > 0));
    await(() -> assertTrue(fileSystemStats.getBytes() > 0));
  }

  private void await(Runnable runnable) {
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(runnable);
  }

  private static class TestObject implements Serializable {

    String title;
    String description;

    public TestObject(String title, String description) {
      this.title = title;
      this.description = description;
    }
  }
}
