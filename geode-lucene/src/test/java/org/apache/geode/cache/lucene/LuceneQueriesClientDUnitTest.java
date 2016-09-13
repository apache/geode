/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gemstone.gemfire.cache.lucene;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.REGION_NAME;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.test.dunit.SerializableCallableIF;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class LuceneQueriesClientDUnitTest extends LuceneQueriesBase {

  @Override public void postSetUp() throws Exception {
    super.postSetUp();
    SerializableCallableIF<Integer> launchServer = () ->
    {
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

  @Override
  protected void initAccessor(SerializableRunnableIF createIndex) throws Exception {
  }

  @Override
  protected void initDataStore(SerializableRunnableIF createIndex) throws Exception {
    createIndex.run();
    getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

}
