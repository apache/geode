/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * This class contains tests of lucene queries that can fit
 */
@Category({LuceneTest.class})
public class LuceneQueriesWithRegionCreatedBeforeReindexIntegrationTest
    extends LuceneQueriesIntegrationTest {

  @Before
  public void setLuceneReindexFlag() {
    LuceneServiceImpl.LUCENE_REINDEX = true;
  }

  @After
  public void clearLuceneReindexFlag() {
    LuceneServiceImpl.LUCENE_REINDEX = false;
  }

  @Override
  protected Region createRegionAndIndex(Map<String, Analyzer> fields) {
    region = cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
    ((LuceneIndexFactoryImpl) luceneService.createIndexFactory().setFields(fields))
        .create(INDEX_NAME, REGION_NAME, LuceneServiceImpl.LUCENE_REINDEX);
    return region;
  }

  @Override
  protected Region createRegionAndIndex(RegionShortcut regionShortcut, String... fields) {
    region = cache.createRegionFactory(regionShortcut).create(REGION_NAME);
    ((LuceneIndexFactoryImpl) luceneService.createIndexFactory().setFields(fields))
        .create(INDEX_NAME, REGION_NAME, LuceneServiceImpl.LUCENE_REINDEX);
    return region;
  }

}
