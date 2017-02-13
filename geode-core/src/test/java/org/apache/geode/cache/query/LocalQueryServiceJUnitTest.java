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

import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class LocalQueryServiceJUnitTest {

  @Test
  public void testLocalQueryService() throws Exception {
    ClientCache c = null;
    try {
      c = new ClientCacheFactory().create();
      addExpectedException(c, IOException.class.getName());
      Region r = createRegion(c);
      int numEntries = 10;
      loadRegion(r, numEntries);
      SelectResults sR = executeLocalQuery(c, "SELECT * from /localRegion");
      assertEquals(numEntries, sR.size());
    } finally {
      if (c != null) {
        c.close();
      }
    }
  }

  @Test
  public void testLocalQueryServiceWithTransaction() throws Exception {
    ClientCache c = null;
    try {
      SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));
      c = new ClientCacheFactory().create();
      addExpectedException(c, IOException.class.getName());
      Region r = createRegion(c);
      int numEntries = 10;
      loadRegion(r, numEntries);
      CacheTransactionManager cacheTransactionManager = c.getCacheTransactionManager();
      cacheTransactionManager.begin();
      SelectResults sR = executeLocalQuery(c, "SELECT * from /localRegion");
      assertEquals(numEntries, sR.size());
      cacheTransactionManager.commit();
    } finally {
      if (c != null) {
        c.close();
      }
    }
  }

  private Region createRegion(ClientCache c) {
    ClientRegionFactory factory = c.createClientRegionFactory(ClientRegionShortcut.LOCAL);
    return factory.create("localRegion");
  }

  private void loadRegion(Region r, int numEntries) {
    for (int i = 0; i < numEntries; i++) {
      r.put(i, i);
    }
  }

  private SelectResults executeLocalQuery(ClientCache c, String qS) throws QueryException {
    QueryService qs = c.getLocalQueryService();
    Query q = qs.newQuery("SELECT * from /localRegion");
    return (SelectResults) q.execute();
  }

  private void addExpectedException(ClientCache c, String exception) {
    c.getLogger().info("<ExpectedException action=add>" + exception + "</ExpectedException>");
  }

  private void removeExpectedException(ClientCache c, String exception) {
    c.getLogger().info("<ExpectedException action=remove>" + exception + "</ExpectedException>");
  }
}
