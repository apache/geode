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

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;

import junitparams.JUnitParamsRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneQueriesWithReindexFlagEnabledClientDUnitTest
    extends LuceneQueriesClientDUnitTest {

  private static final long serialVersionUID = 1L;

  @Before
  public void setLuceneReindexFlag() {
    dataStore1.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    dataStore2.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
  }

  @After
  public void clearLuceneReindexFlag() {
    dataStore1.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
    dataStore2.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
  }
}
