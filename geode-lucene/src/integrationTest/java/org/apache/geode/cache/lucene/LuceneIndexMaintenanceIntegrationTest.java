/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.DEFAULT_FIELD;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.Document;
import org.awaitility.core.ThrowingRunnable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.internal.LuceneIndexForPartitionedRegion;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneIndexMaintenanceIntegrationTest extends LuceneIntegrationTest {

  private static int WAIT_FOR_FLUSH_TIME = 10000;


  @Test
  public void indexIsUpdatedAfterTransactionHasCommitted() throws Exception {
    luceneService.createIndexFactory().setFields("title", "description").create(INDEX_NAME,
        REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "description:\"hello world\"", DEFAULT_FIELD);
    PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
    assertEquals(3, results.size());

    cache.getCacheTransactionManager().begin();
    region.put("object-1", new TestObject("title 1", "updated"));
    cache.getCacheTransactionManager().commit();
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);

    assertEquals(2, query.findPages().size());
  }

  @Test
  public void indexIsNotUpdatedAfterTransactionRollback() throws Exception {
    luceneService.createIndexFactory().setFields("title", "description").create(INDEX_NAME,
        REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "description:\"hello world\"", DEFAULT_FIELD);
    PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
    assertEquals(3, results.size());

    cache.getCacheTransactionManager().begin();
    region.put("object-1", new TestObject("title 1", "updated"));
    cache.getCacheTransactionManager().rollback();
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);

    assertEquals(3, query.findPages().size());
  }

  @Test
  public void useSerializerToIndex() throws Exception {
    luceneService.createIndexFactory().setFields("title", "description")
        .setLuceneSerializer(new HeterogeneousLuceneSerializer()).create(INDEX_NAME, REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "description:\"hello world\"", DEFAULT_FIELD);
    PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
    assertEquals(3, results.size());
  }

  @Test
  public void serializerExceptionShouldNotImpactOtherEvents() throws Exception {
    luceneService.createIndexFactory().setFields("title", "description")
        .setLuceneSerializer(new TestCatchingExceptionInSerializer("title 3"))
        .create(INDEX_NAME, REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "description:\"hello world\"", DEFAULT_FIELD);
    PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
    assertEquals(2, results.size());
    LuceneIndexForPartitionedRegion indexForPR = (LuceneIndexForPartitionedRegion) index;
    LuceneIndexStats indexStats = indexForPR.getIndexStats();
    assertEquals(true, 2 >= indexStats.getFailedEntries());
    assertEquals(true, 8 >= indexStats.getUpdates());
  }

  @Test
  public void pdxInstanceShouldNotBeDeserialized() throws Exception {
    luceneService.createIndexFactory().setFields("status", "description")
        .setLuceneSerializer(new TestPdxInstanceSerializer()).create(INDEX_NAME, REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    for (int i = 0; i < 10; i++) {
      PortfolioPdx p = new PortfolioPdx(i);
      region.put(i, p);
    }

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "status:active", "status");
    PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
    assertEquals(5, results.size());
    LuceneIndexForPartitionedRegion indexForPR = (LuceneIndexForPartitionedRegion) index;
    LuceneIndexStats indexStats = indexForPR.getIndexStats();
    assertThat("indexStats.getUpdates()", indexStats.getUpdates(), greaterThanOrEqualTo(10));
  }

  @Test
  public void statsAreUpdatedAfterACommit() throws Exception {
    luceneService.createIndexFactory().setFields("title", "description").create(INDEX_NAME,
        REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndexForPartitionedRegion index =
        (LuceneIndexForPartitionedRegion) luceneService.getIndex(INDEX_NAME, REGION_NAME);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);

    FileSystemStats fileSystemStats = index.getFileSystemStats();
    LuceneIndexStats indexStats = index.getIndexStats();
    await(() -> assertEquals(4, indexStats.getDocuments()));
    await(() -> assertTrue(fileSystemStats.getBytes() > 0));
  }

  @Test
  public void indexShouldBeUpdatedWithRegionExpirationDestroyOperation() throws Exception {
    luceneService.createIndexFactory().setFields("title", "description").create(INDEX_NAME,
        REGION_NAME);

    // Configure PR with expiration operation set to destroy
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
        .setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY))
        .create(REGION_NAME);
    populateRegion(region);
    // Wait for expiration to destroy region entries. The region should be
    // left with zero entries.
    GeodeAwaitility.await().untilAsserted(() -> {
      assertEquals(0, region.size());
    });

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    // Wait for events to be flushed from AEQ.
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
    // Execute query to fetch all the values for "description" field.
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "description:\"hello world\"", DEFAULT_FIELD);
    PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
    // The query should return 0 results.
    assertEquals(0, results.size());
  }

  @Test
  public void nullValuesShouldNotCauseAnException() throws Exception {
    luceneService.createIndexFactory().setFields("title", "description").create(INDEX_NAME,
        REGION_NAME);

    // Configure PR with expiration operation set to destroy
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
    region.create(0, null);
    region.put(113, new TestObject("hello world", "hello world"));
    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    // Wait for events to be flushed from AEQ.
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
    // Execute query to fetch all the values for "description" field.
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "description:\"hello world\"", DEFAULT_FIELD);
    PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
    assertEquals(1, results.size());
  }

  @Test
  public void entriesFlushedToIndexAfterWaitForFlushCalled() throws InterruptedException {
    luceneService.createIndexFactory().setFields("title", "description").create(INDEX_NAME,
        REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    LuceneTestUtilities.pauseSender(cache);

    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));

    LuceneIndexImpl index = (LuceneIndexImpl) luceneService.getIndex(INDEX_NAME, REGION_NAME);
    assertFalse(
        luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 500, TimeUnit.MILLISECONDS));
    LuceneTestUtilities.resumeSender(cache);
    assertTrue(luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS));
    assertEquals(true, 8 >= index.getIndexStats().getCommits());
  }

  @Test
  public void entriesKeptInSerializedFormInDataRegion() throws InterruptedException {
    // Create index and region
    luceneService.createIndexFactory().setFields("title", "description").create(INDEX_NAME,
        REGION_NAME);
    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);

    // Pause sender
    LuceneTestUtilities.pauseSender(cache);

    // Do puts
    populateRegion(region);

    // Verify values are in serialized form
    verifySerializedValues(region);

    // Resume sender and wait for flushed
    LuceneTestUtilities.resumeSender(cache);
    assertTrue(luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS));

    // Verify values are still in serialized form
    verifySerializedValues(region);
  }

  @Test
  public void cacheLoadUpdatesIndex() throws InterruptedException, LuceneQueryException {
    luceneService.createIndexFactory().setFields("title", "description").create(INDEX_NAME,
        REGION_NAME);

    Region region = this.cache.<String, TestObject>createRegionFactory(RegionShortcut.PARTITION)
        .setCacheLoader(new TestCacheLoader()).create(REGION_NAME);

    region.get("object-1");

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    assertTrue(luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS));

    LuceneQuery<Object, Object> query = luceneService.createLuceneQueryFactory().create(INDEX_NAME,
        REGION_NAME, "hello", "description");
    assertEquals(Collections.singletonList("object-1"), query.findKeys());
  }

  private void populateRegion(Region region) {
    region.put("object-1", new TestObject("title 1", "hello world"));
    region.put("object-2", new TestObject("title 2", "this will not match"));
    region.put("object-3", new TestObject("title 3", "hello world"));
    region.put("object-4", new TestObject("hello world", "hello world"));
  }

  private void verifySerializedValues(Region region) {
    Set entries = region.entrySet();
    assertFalse(entries.isEmpty());
    for (Iterator i = entries.iterator(); i.hasNext();) {
      EntrySnapshot entry = (EntrySnapshot) i.next();
      RegionEntry re = entry.getRegionEntry();
      Object reValue = re.getValue(null);
      assertTrue(reValue instanceof CachedDeserializable);
      Object cdValue = ((CachedDeserializable) reValue).getValue();
      assertTrue(cdValue instanceof byte[]);
    }
  }

  private void await(ThrowingRunnable runnable) {
    GeodeAwaitility.await().untilAsserted(runnable);
  }

  private static final class TestCacheLoader implements CacheLoader<String, TestObject> {
    @Override
    public void close() {}

    @Override
    public TestObject load(LoaderHelper<String, TestObject> helper) throws CacheLoaderException {
      return new TestObject("title 1", "hello world");
    }
  }

  private static class TestObject implements Serializable {

    String title;
    String description;

    public TestObject(String title, String description) {
      this.title = title;
      this.description = description;
    }
  }

  private static class TestCatchingExceptionInSerializer extends HeterogeneousLuceneSerializer {

    String match;

    TestCatchingExceptionInSerializer(String match) {
      this.match = match;
    }

    @Override
    public Collection<Document> toDocuments(LuceneIndex index, Object value) {
      TestObject testObject = (TestObject) value;
      if (testObject.title.equals(match)) {
        throw new RuntimeException("Expected exception in Serializer:" + value);
      } else {
        return super.toDocuments(index, value);
      }
    }
  }

  private static class TestPdxInstanceSerializer extends HeterogeneousLuceneSerializer {

    @Override
    public Collection<Document> toDocuments(LuceneIndex index, Object value) {
      assertTrue(value instanceof PdxInstance);
      return super.toDocuments(index, value);
    }
  }
}
