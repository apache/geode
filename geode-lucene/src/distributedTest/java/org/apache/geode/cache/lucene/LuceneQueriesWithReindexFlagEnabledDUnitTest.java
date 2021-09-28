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

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * This test class is intended to contain basic integration tests of the lucene query class that
 * should be executed against a number of different regions types and topologies.
 *
 */
@Category({LuceneTest.class})
@RunWith(GeodeParamsRunner.class)
public class LuceneQueriesWithReindexFlagEnabledDUnitTest extends LuceneQueriesDUnitTest {

  private static final long serialVersionUID = 1L;

  @Before
  public void setLuceneReindexFlag() {
    dataStore1.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    dataStore2.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    accessor.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
  }

  @After
  public void clearLuceneReindexFlag() {
    dataStore1.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
    dataStore2.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
    accessor.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
  }

}
