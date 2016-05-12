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

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexForPartitionedRegion;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.jayway.awaitility.Awaitility;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class LuceneIndexCreationIntegrationTest extends LuceneIntegrationTest {
  public static final String INDEX_NAME = "index";
  public static final String REGION_NAME = "region";

  @Test
  public void shouldCreateIndexWriterWithAnalyzersWhenSettingPerFieldAnalyzers()
    throws BucketNotFoundException, InterruptedException
  {
    Map<String, Analyzer> analyzers = new HashMap<>();

    final RecordingAnalyzer field1Analyzer = new RecordingAnalyzer();
    final RecordingAnalyzer field2Analyzer = new RecordingAnalyzer();
    analyzers.put("field1", field1Analyzer);
    analyzers.put("field2", field2Analyzer);
    luceneService.createIndex(INDEX_NAME, REGION_NAME, analyzers);
    Region region = createRegion();
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    region.put("key1", new TestObject());

    assertEquals(analyzers, index.getFieldAnalyzers());
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
      assertEquals(Arrays.asList("field1"), field1Analyzer.analyzedfields);
      assertEquals(Arrays.asList("field2"), field2Analyzer.analyzedfields);
    });
  }

  @Test
  public void shouldUseRedundancyForInternalRegionsWhenUserRegionHasRedundancy() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).create(REGION_NAME);
    verifyInternalRegions(region -> {
      assertEquals(1, region.getAttributes().getPartitionAttributes().getRedundantCopies());
    });
  }

  @Test
  public void shouldNotUseEvictionForInternalRegionsWhenUserRegionHasEviction() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION)
      .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1))
      .create(REGION_NAME);

    verifyInternalRegions(region -> {
      assertEquals(true, region.getAttributes().getEvictionAttributes().getAction().isNone());
    });
  }

  @Test
  public void shouldNotUseIdleTimeoutForInternalRegionsWhenUserRegionHasIdleTimeout() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION)
      .setEntryIdleTimeout(new ExpirationAttributes(5))
      .create(REGION_NAME);

    verifyInternalRegions(region -> {
      assertEquals(0, region.getAttributes().getEntryIdleTimeout().getTimeout());
    });
  }

  @Test
  public void shouldNotUseTTLForInternalRegionsWhenUserRegionHasTTL() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION)
      .setEntryTimeToLive(new ExpirationAttributes(5))
      .create(REGION_NAME);

    verifyInternalRegions(region -> {
      assertEquals(0, region.getAttributes().getEntryTimeToLive().getTimeout());
    });
  }

  @Test
  public void shouldNotUseOffHeapForInternalRegionsWhenUserRegionHasOffHeap() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION)
      .setOffHeap(true)
      .create(REGION_NAME);

    verifyInternalRegions(region -> {
      assertEquals(false, region.getOffHeap());
    });
  }

  @Test
  public void shouldNotUseOverflowForInternalRegionsWhenUserRegionHasOverflow() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION_OVERFLOW).create(REGION_NAME);
    verifyInternalRegions(region -> {
      assertTrue(region.getAttributes().getEvictionAttributes().getAction().isNone());
    });
  }

  @Test
  public void shouldUseDiskSynchronousWhenUserRegionHasDiskSynchronous() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .setDiskSynchronous(true)
      .create(REGION_NAME);
    verifyInternalRegions(region -> {
      assertTrue(region.getDataPolicy().withPersistence());
      assertTrue(region.isDiskSynchronous());
    });
    AsyncEventQueue queue = getIndexQueue();
    assertEquals(true, queue.isDiskSynchronous());
    assertEquals(true, queue.isPersistent());
  }

  @Test
  public void shouldUseDiskSyncFalseOnQueueWhenUserRegionHasDiskSynchronousFalse() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .setDiskSynchronous(false)
      .create(REGION_NAME);
    verifyInternalRegions(region -> {
      assertTrue(region.getDataPolicy().withPersistence());
      assertTrue(region.isDiskSynchronous());
    });
    AsyncEventQueue queue = getIndexQueue();
    assertEquals(false, queue.isDiskSynchronous());
    assertEquals(true, queue.isPersistent());
  }

  @Test
  public void shouldRecoverPersistentIndexWhenDataStillInQueue() throws ParseException, InterruptedException {
    createIndex("field1", "field2");
    Region dataRegion = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .create(REGION_NAME);
    //Pause the sender so that the entry stays in the queue
    final AsyncEventQueueImpl queue = (AsyncEventQueueImpl) getIndexQueue();
    queue.getSender().pause();

    dataRegion.put("A", new TestObject());
    cache.close();
    createCache();
    createIndex("field1", "field2");
    dataRegion = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .create(REGION_NAME);
    LuceneQuery<Object, Object> query = luceneService.createLuceneQueryFactory()
      .create(INDEX_NAME, REGION_NAME,
        "field1:world");
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
      assertEquals(1, query.search().size());
    });
  }

  @Test
  public void shouldRecoverPersistentIndexWhenDataIsWrittenToIndex() throws ParseException, InterruptedException {
    createIndex("field1", "field2");
    Region dataRegion = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .create(REGION_NAME);
    dataRegion.put("A", new TestObject());
    final AsyncEventQueueImpl queue = (AsyncEventQueueImpl) getIndexQueue();

    //Wait until the queue has drained
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> assertEquals(0, queue.size()));
    cache.close();
    createCache();
    createIndex("text");
    dataRegion = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .create(REGION_NAME);
    LuceneQuery<Object, Object> query = luceneService.createLuceneQueryFactory()
      .create(INDEX_NAME, REGION_NAME,
      "field1:world");
    assertEquals(1, query.search().size());
  }

  @Test
  public void shouldCreateInternalRegionsForIndex() {
    createIndex("field1", "field2");

    // Create partitioned region
    createRegion();

    verifyInternalRegions(region -> {
      region.isInternalRegion();
      assertNotNull(region.getAttributes().getPartitionAttributes().getColocatedWith());
      cache.rootRegions().contains(region);
    });
  }

  @Test
  public void shouldUseFixedPartitionsForInternalRegions() {
    createIndex("text");

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory<>();
    final FixedPartitionAttributes fixedAttributes = FixedPartitionAttributes.createFixedPartition("A", true, 1);
    partitionAttributesFactory.addFixedPartitionAttributes(fixedAttributes);
    cache.createRegionFactory(RegionShortcut.PARTITION)
      .setPartitionAttributes(partitionAttributesFactory.create())
      .create(REGION_NAME);

    verifyInternalRegions(region -> {
      //Fixed partitioned regions don't allow you to specify the partitions on the colocated region
      assertNull(region.getAttributes().getPartitionAttributes().getFixedPartitionAttributes());
      assertTrue(((PartitionedRegion) region).isFixedPartitionedRegion());
    });
  }


  private void verifyInternalRegions(Consumer<LocalRegion> verify) {
    // Get index
    LuceneIndexForPartitionedRegion index = (LuceneIndexForPartitionedRegion) luceneService.getIndex(INDEX_NAME, REGION_NAME);

    // Verify the meta regions exist and are internal
    LocalRegion chunkRegion = (LocalRegion) cache.getRegion(index.createChunkRegionName());
    LocalRegion fileRegion = (LocalRegion) cache.getRegion(index.createFileRegionName());
    verify.accept(chunkRegion);
    verify.accept(fileRegion);
  }

  private AsyncEventQueue getIndexQueue() {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
    return cache.getAsyncEventQueue(aeqId);
  }

  private Region createRegion() {
    return this.cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

  private void createIndex(String ... fieldNames) {
    LuceneServiceProvider.get(this.cache).createIndex(INDEX_NAME, REGION_NAME, fieldNames);
  }

  private static class TestObject implements Serializable {

    String field1 = "hello world";
    String field2 = "this is a field";
  }

  private static class RecordingAnalyzer extends Analyzer {

    private List<String> analyzedfields = new ArrayList<String>();

    @Override protected TokenStreamComponents createComponents(final String fieldName) {
      analyzedfields.add(fieldName);
      return new TokenStreamComponents(new KeywordTokenizer());
    }
  }
}
