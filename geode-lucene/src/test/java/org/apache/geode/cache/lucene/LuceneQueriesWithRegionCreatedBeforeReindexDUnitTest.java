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

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;

import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.cache.lucene.internal.LuceneQueryImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This test class is intended to contain basic integration tests of the lucene query class that
 * should be executed against a number of different regions types and topologies.
 *
 */
@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneQueriesWithRegionCreatedBeforeReindexDUnitTest extends LuceneQueriesDUnitTest {

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

  @Override
  protected void createRegionAndIndexForAllDataStores(RegionTestableType regionTestType,
      SerializableRunnableIF createIndex) throws Exception {

    // Create dataRegion prior to index
    dataStore1.invoke(() -> initDataStore(regionTestType));
    dataStore2.invoke(() -> initDataStore(regionTestType));
    accessor.invoke(() -> initAccessor(regionTestType));

    // re-index stored data
    AsyncInvocation ai1 = dataStore1.invokeAsync(createIndex);
    AsyncInvocation ai2 = dataStore2.invokeAsync(createIndex);
    AsyncInvocation ai3 = accessor.invokeAsync(createIndex);

    ai1.join();
    ai2.join();
    ai3.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();
  }

}
