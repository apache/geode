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

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class LuceneIndexCreationIntegrationTest extends LuceneIntegrationTest {

  @Test
  public void verifyLuceneRegionInternal() {
    // Create cache
    createCache();

    // Create index
    String indexName = "index";
    String regionName = this.name.getMethodName();
    createIndex(indexName, regionName, "text");

    // Create partitioned region
    createRegion(regionName);

    // Get index
    LuceneIndexForPartitionedRegion index = (LuceneIndexForPartitionedRegion) LuceneServiceProvider.get(this.cache).getIndex(indexName, regionName);
    assertNotNull(index);

    // Verify the meta regions exist and are internal
    LocalRegion chunkRegion = (LocalRegion) this.cache.getRegion(index.createChunkRegionName());
    assertNotNull(chunkRegion);
    assertTrue(chunkRegion.isInternalRegion());
    LocalRegion fileRegion = (LocalRegion) this.cache.getRegion(index.createFileRegionName());
    assertNotNull(fileRegion);
    assertTrue(fileRegion.isInternalRegion());

    // Verify the meta regions are not contained in the root regions
    for (Region region : cache.rootRegions()) {
      assertNotEquals(chunkRegion.getFullPath(), region.getFullPath());
      assertNotEquals(fileRegion.getFullPath(), region.getFullPath());
    }
  }

  private Region createRegion(String regionName) {
    return this.cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
  }

  private void createIndex(String indexName, String regionName, String fieldName) {
    LuceneServiceProvider.get(this.cache).createIndex(indexName, regionName, fieldName);
  }
}
