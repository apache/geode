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
import static org.junit.Assert.assertEquals;

import java.util.function.Consumer;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.offheap.MemoryAllocatorImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests of lucene index creation that use off heap memory
 */
@Category(IntegrationTest.class)
public class LuceneIndexCreationOffHeapIntegrationTest extends LuceneIntegrationTest {

  @Override
  public void closeCache() {
    super.closeCache();
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

  @Override
  protected CacheFactory getCacheFactory() {
    CacheFactory factory = super.getCacheFactory();
    factory.set("off-heap-memory-size", "100m");
    return factory;
  }

  @Test
  public void shouldNotUseOffHeapForInternalRegionsWhenUserRegionHasOffHeap() {
    createIndex(cache, "text");
    cache.createRegionFactory(RegionShortcut.PARTITION)
      .setOffHeap(true)
      .create(REGION_NAME);

    verifyInternalRegions(region -> {
      assertEquals(false, region.getOffHeap());
    });
  }

  private void verifyInternalRegions(Consumer<LocalRegion> verify) {
    LuceneTestUtilities.verifyInternalRegions(luceneService, cache, verify);
  }
}
