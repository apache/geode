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

import java.io.File;

import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.Type1;
import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.test.junit.rules.DiskDirRule;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;

/**
 * Tests of lucene index creation that use persistence
 */
@Category(IntegrationTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneQueriesPersistenceIntegrationTest extends LuceneIntegrationTest {

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
  public void shouldReturnCorrectResultsWithEntriesOverflowedToDisk() throws Exception {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);

    LuceneService service = LuceneServiceProvider.get(cache);
    service.createIndex(INDEX_NAME, REGION_NAME, Type1.fields);

    RegionFactory<String, Type1> regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    EvictionAttributesImpl evicAttr = new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
    evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
    regionFactory.setEvictionAttributes(evicAttr);

    PartitionedRegion userRegion = (PartitionedRegion) regionFactory.create(REGION_NAME);
    final LuceneIndex index = service.getIndex(INDEX_NAME, REGION_NAME);

    Assert.assertEquals(0, userRegion.getDiskRegionStats().getNumOverflowOnDisk());

    Type1 value = new Type1("hello world", 1, 2L, 3.0, 4.0f);
    userRegion.put("value1", value);
    value = new Type1("test world", 1, 2L, 3.0, 4.0f);
    userRegion.put("value2", value);
    value = new Type1("lucene world", 1, 2L, 3.0, 4.0f);
    userRegion.put("value3", value);

    index.waitUntilFlushed(60000);

    PartitionedRegion fileRegion = (PartitionedRegion) cache.getRegion(aeqId + ".files");
    assertNotNull(fileRegion);
    PartitionedRegion chunkRegion = (PartitionedRegion) cache.getRegion(aeqId + ".chunks");
    assertNotNull(chunkRegion);
    Assert.assertTrue(0 < userRegion.getDiskRegionStats().getNumOverflowOnDisk());

    LuceneQuery<Integer, Type1> query = service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "s:world", DEFAULT_FIELD);
    PageableLuceneQueryResults<Integer, Type1> results = query.findPages();
    Assert.assertEquals(3, results.size());
  }


}
