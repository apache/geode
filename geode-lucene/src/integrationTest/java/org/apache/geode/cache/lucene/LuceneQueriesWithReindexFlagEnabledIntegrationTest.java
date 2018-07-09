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

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * This class contains tests of lucene queries that can fit
 */
@Category({LuceneTest.class})
public class LuceneQueriesWithReindexFlagEnabledIntegrationTest
    extends LuceneQueriesIntegrationTest {

  @Before
  public void setLuceneReindexFlag() {
    LuceneServiceImpl.LUCENE_REINDEX = true;
  }

  @After
  public void clearLuceneReindexFlag() {
    LuceneServiceImpl.LUCENE_REINDEX = false;
  }

}
