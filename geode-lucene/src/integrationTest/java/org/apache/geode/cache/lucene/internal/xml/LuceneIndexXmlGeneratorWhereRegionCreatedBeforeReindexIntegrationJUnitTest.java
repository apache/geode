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
package org.apache.geode.cache.lucene.internal.xml;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.test.LuceneTestSerializer;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneIndexXmlGeneratorWhereRegionCreatedBeforeReindexIntegrationJUnitTest
    extends LuceneIndexXmlGeneratorIntegrationJUnitTest {

  @After
  public void clearLuceneReindexFeatureFlag() {
    LuceneServiceImpl.LUCENE_REINDEX = false;
  }

  @Before
  public void setLuceneReindexFeatureFlag() {
    LuceneServiceImpl.LUCENE_REINDEX = true;
  }

  @Override
  protected void createDataRegionAndLuceneIndex(LuceneService service) {
    super.cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
    ((LuceneIndexFactoryImpl) service.createIndexFactory().setFields("a", "b", "c")).create("index",
        "region", true);

  }

  @Override
  protected void createDataRegionAndLuceneIndexWithSerializer(LuceneTestSerializer luceneSerializer,
      LuceneService service) {
    cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
    ((LuceneIndexFactoryImpl) service.createIndexFactory().setLuceneSerializer(luceneSerializer)
        .setFields("a", "b", "c")).create("index", "region", true);
  }

}
