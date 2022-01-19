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
package org.apache.geode.cache.query;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class LocalQueryServiceJUnitTest {

  private IgnoredException ignoredException;
  private ClientCache clientCache;
  private int numEntries;

  @Before
  public void before() throws Exception {
    ignoredException = addIgnoredException(IOException.class.getName());

    clientCache = new ClientCacheFactory().create();
    ClientRegionFactory factory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.LOCAL);
    Region region = factory.create("localRegion");
    numEntries = 10;
    for (int i = 0; i < numEntries; i++) {
      region.put(i, i);
    }
  }

  @After
  public void after() throws Exception {
    if (clientCache != null) {
      clientCache.close();
    }
    ignoredException.remove();
  }

  @Test
  public void testLocalQueryService() throws Exception {
    SelectResults sR =
        executeLocalQuery(clientCache, "SELECT * from " + SEPARATOR + "localRegion");

    assertEquals(numEntries, sR.size());
  }

  @Test
  public void testLocalQueryServiceWithTransaction() throws Exception {
    CacheTransactionManager cacheTransactionManager = clientCache.getCacheTransactionManager();
    cacheTransactionManager.begin();
    SelectResults selectResults =
        executeLocalQuery(clientCache, "SELECT * from " + SEPARATOR + "localRegion");

    assertEquals(numEntries, selectResults.size());

    cacheTransactionManager.commit();
  }

  private SelectResults executeLocalQuery(ClientCache c, String query) throws QueryException {
    QueryService qs = c.getLocalQueryService();
    Query q = qs.newQuery(query);
    return (SelectResults) q.execute();
  }
}
