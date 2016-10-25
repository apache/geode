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
package org.apache.geode.cache.lucene;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.function.Consumer;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.createIndex;
import static org.junit.Assert.assertEquals;

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
    factory.set(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, "100m");
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
