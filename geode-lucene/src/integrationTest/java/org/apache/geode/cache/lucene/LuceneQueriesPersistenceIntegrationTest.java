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
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.Type1;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * Tests of lucene index creation that use persistence
 */
@Category({LuceneTest.class})
@RunWith(GeodeParamsRunner.class)
public class LuceneQueriesPersistenceIntegrationTest extends LuceneIntegrationTest {

  @Rule
  public TemporaryFolder tempFolderRule = new TemporaryFolder();

  @Override
  public void createCache() {
    super.createCache();
    cache.createDiskStoreFactory().setDiskDirs(new File[] {tempFolderRule.getRoot()})
        .setMaxOplogSize(1).create(GemFireCacheImpl.getDefaultDiskStoreName());
  }


  @Test
  public void shouldReturnCorrectResultsWithEntriesOverflowedToDisk() throws Exception {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);

    LuceneService service = LuceneServiceProvider.get(cache);
    service.createIndexFactory().setFields(Type1.fields).create(INDEX_NAME, REGION_NAME);

    RegionFactory<String, Type1> regionFactory =
        cache.createRegionFactory(RegionShortcut.PARTITION);
    EvictionAttributesImpl evicAttr =
        new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
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

    service.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

    PartitionedRegion fileRegion = (PartitionedRegion) cache.getRegion(aeqId + ".files");
    assertNotNull(fileRegion);
    Assert.assertTrue(0 < userRegion.getDiskRegionStats().getNumOverflowOnDisk());

    LuceneQuery<Integer, Type1> query = service.createLuceneQueryFactory().create(INDEX_NAME,
        REGION_NAME, "s:world", DEFAULT_FIELD);
    PageableLuceneQueryResults<Integer, Type1> results = query.findPages();
    Assert.assertEquals(3, results.size());
  }


}
