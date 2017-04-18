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
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneQueriesClientDUnitTest extends LuceneQueriesDUnitTest {

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    SerializableCallableIF<Integer> launchServer = () -> {
      final Cache cache = getCache();
      final CacheServer server = cache.addCacheServer();
      server.setPort(0);
      server.start();
      return server.getPort();
    };
    final int port1 = dataStore1.invoke(launchServer);
    final int port2 = dataStore2.invoke(launchServer);

    accessor.invoke(() -> {
      ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.addPoolServer("localhost", port1);
      clientCacheFactory.addPoolServer("localhost", port2);
      ClientCache clientCache = getClientCache(clientCacheFactory);
      clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(REGION_NAME);
    });
  }

  protected void initAccessor(SerializableRunnableIF createIndex, RegionTestableType regionTestType)
      throws Exception {}

  protected RegionTestableType[] getListOfRegionTestTypes() {
    return new RegionTestableType[] {RegionTestableType.PARTITION_WITH_CLIENT};
  }

  // Due to singlehop transactions differences, the exception actually isn't thrown
  // So the parent test behaves differently if singlehop is enabled or not for a client
  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void transactionWithLuceneQueriesShouldThrowException(RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().addField("text").create(INDEX_NAME, REGION_NAME);
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));

    putDataInRegion(accessor);
    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    accessor.invoke(() -> {
      Cache cache = getCache();
      try {
        LuceneService service = LuceneServiceProvider.get(cache);
        LuceneQuery<Integer, TestObject> query;
        query = service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "text:world",
            DEFAULT_FIELD);
        cache.getCacheTransactionManager().begin();
        PageableLuceneQueryResults<Integer, TestObject> results = query.findPages();
      } finally {
        cache.getCacheTransactionManager().rollback();
      }
    });

  }



}
