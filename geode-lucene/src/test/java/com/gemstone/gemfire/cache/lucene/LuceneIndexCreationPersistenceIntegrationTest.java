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

import static com.gemstone.gemfire.cache.RegionShortcut.*;
import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static junitparams.JUnitParamsRunner.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.jayway.awaitility.Awaitility;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities;
import com.gemstone.gemfire.cache.lucene.test.TestObject;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.test.junit.rules.DiskDirRule;

/**
 * Tests of lucene index creation that use persistence
 */
@Category(IntegrationTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneIndexCreationPersistenceIntegrationTest extends LuceneIntegrationTest {

  @Rule
  public DiskDirRule diskDirRule = new DiskDirRule();

  @Override
  public void createCache() {
    super.createCache();
    cache.createDiskStoreFactory()
      .setDiskDirs(new File[] {diskDirRule.get()})
      .setMaxOplogSize(1)
      .create(GemFireCacheImpl.getDefaultDiskStoreName());
  }

  @Test
  public void shouldNotUseOverflowForInternalRegionsWhenUserRegionHasOverflow() {
    createIndex(cache, "text");
    cache.createRegionFactory(RegionShortcut.PARTITION_OVERFLOW).create(REGION_NAME);
    verifyInternalRegions(region -> {
      assertTrue(region.getAttributes().getEvictionAttributes().getAction().isNone());
    });
  }

  @Test
  @Parameters({"true", "false"})
  public void shouldUseDiskSynchronousWhenUserRegionHasDiskSynchronous(boolean synchronous) {
    createIndex(cache, "text");
    cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .setDiskSynchronous(synchronous)
      .create(REGION_NAME);
    verifyInternalRegions(region -> {
      assertTrue(region.getDataPolicy().withPersistence());
      //Underlying region should always be synchronous
      assertTrue(region.isDiskSynchronous());
    });
    AsyncEventQueue queue = getIndexQueue(cache);
    assertEquals(synchronous, queue.isDiskSynchronous());
    assertEquals(true, queue.isPersistent());
  }

  @Test
  public void shouldRecoverPersistentIndexWhenDataStillInQueue() throws Exception {
    createIndex(cache, "field1", "field2");
    Region dataRegion = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .create(REGION_NAME);
    //Pause the sender so that the entry stays in the queue
    pauseSender(cache);

    dataRegion.put("A", new TestObject());
    cache.close();
    createCache();
    createIndex(cache, "field1", "field2");
    dataRegion = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .create(REGION_NAME);
    verifyIndexFinishFlushing(cache, INDEX_NAME, REGION_NAME);
    LuceneQuery<Object, Object> query = luceneService.createLuceneQueryFactory()
      .create(INDEX_NAME, REGION_NAME,
        "field1:world", DEFAULT_FIELD);
    assertEquals(1, query.findPages().size());
  }

  @Test
  public void shouldRecoverPersistentIndexWhenDataIsWrittenToIndex() throws Exception {
    createIndex(cache, "field1", "field2");
    Region dataRegion = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .create(REGION_NAME);
    dataRegion.put("A", new TestObject());
    verifyIndexFinishFlushing(cache, INDEX_NAME, REGION_NAME);
    cache.close();
    createCache();
    createIndex(cache, "field1", "field2");
    dataRegion = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
      .create(REGION_NAME);
    LuceneQuery<Object, Object> query = luceneService.createLuceneQueryFactory()
      .create(INDEX_NAME, REGION_NAME,
      "field1:world", DEFAULT_FIELD);
    assertEquals(1, query.findPages().size());
  }

  @Test
  @Parameters(method = "getRegionShortcuts")
  public void shouldHandleMultipleIndexes(RegionShortcut shortcut) throws Exception {
    LuceneServiceProvider.get(this.cache).createIndex(INDEX_NAME+"_1", REGION_NAME, "field1");
    LuceneServiceProvider.get(this.cache).createIndex(INDEX_NAME+"_2", REGION_NAME, "field2");
    Region region = cache.createRegionFactory(shortcut).create(REGION_NAME);
    region.put("key1", new TestObject());
    verifyQueryResultSize(INDEX_NAME+"_1", REGION_NAME, "field1:world", DEFAULT_FIELD, 1);
    verifyQueryResultSize(INDEX_NAME+"_2", REGION_NAME, "field2:field", DEFAULT_FIELD, 1);
  }

  @Test
  @Parameters(method = "getRegionShortcuts")
  public void shouldCreateInternalRegionsForIndex(RegionShortcut shortcut) {
    luceneService.createIndex(INDEX_NAME, REGION_NAME, "field1", "field2");

    // Create partitioned region
    createRegion(REGION_NAME, shortcut);

    verifyInternalRegions(region -> {
      region.isInternalRegion();
      assertTrue(region.isInternalRegion());

      assertNotNull(region.getAttributes().getPartitionAttributes().getColocatedWith());
      cache.rootRegions().contains(region);
      assertFalse(cache.rootRegions().contains(region));
    });
  }

  private void verifyQueryResultSize(String indexName, String regionName, String queryString, String defaultField, int size) throws Exception {
    LuceneQuery query = luceneService.createLuceneQueryFactory().create(indexName, regionName, queryString, defaultField);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
      try {
        assertEquals(size, query.findPages().size());
      } catch(LuceneQueryException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void verifyInternalRegions(Consumer<LocalRegion> verify) {
    LuceneTestUtilities.verifyInternalRegions(luceneService, cache, verify);
  }


  private static final Object[] getRegionShortcuts() {
    return $(
      new Object[] { PARTITION },
      new Object[] { PARTITION_REDUNDANT },
      new Object[] { PARTITION_PERSISTENT },
      new Object[] { PARTITION_REDUNDANT_PERSISTENT },
      new Object[] { PARTITION_OVERFLOW },
      new Object[] { PARTITION_REDUNDANT_OVERFLOW },
      new Object[] { PARTITION_PERSISTENT_OVERFLOW },
      new Object[] { PARTITION_REDUNDANT_PERSISTENT_OVERFLOW }
    );
  }

}
