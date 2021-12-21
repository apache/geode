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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.cache.Region.SEPARATOR;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class PdxLocalQueryVersionedClassDUnitTest extends PDXQueryTestBase {



  /**
   * Testing the isRemote flag which could be inconsistent when bind queries are being executed in
   * multiple threads. Bug #49662 is caused because of this inconsistent behavior.
   *
   */
  @Test
  public void testIsRemoteFlagForRemoteQueries() throws Exception {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);

    final int numberOfEntries = 1000;
    final String name = SEPARATOR + regionName;

    final String query =
        "select distinct * from " + name + " where id > $1 and id < $2 and status = 'active'";

    // Start server
    final int port1 = (Integer) server.invoke(new SerializableCallable("Create Server") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start client and put version1 objects on server
    // Server does not have version1 classes in classpath
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);

        for (int i = 0; i < numberOfEntries; i++) {
          PdxInstanceFactory pdxInstanceFactory = PdxInstanceFactoryImpl
              .newCreator("PdxVersionedNewPortfolio", false, (InternalCache) cache);
          pdxInstanceFactory.writeInt("id", i);
          pdxInstanceFactory.writeString("status", (i % 2 == 0 ? "active" : "inactive"));
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          region.put("key-" + i, pdxInstance);
        }

        return null;
      }
    });

    // Execute same query remotely from client using 2 threads
    // Since this is a bind query, the query object will be shared
    // between the 2 threads.
    AsyncInvocation a1 = client.invokeAsync(new SerializableCallable("Query from client") {
      @Override
      public Object call() throws Exception {
        QueryService qs = null;
        SelectResults sr = null;
        // Execute query remotely
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        try {
          for (int i = 0; i < 100; i++) {
            sr = (SelectResults) qs.newQuery(query).execute(new Object[] {1, 1000});
          }
          Assert.assertTrue("Size of resultset should be greater than 0 for query: " + query,
              sr.size() > 0);
        } catch (Exception e) {
          Assert.fail("Failed executing query " + query, e);
        }

        return null;
      }
    });

    AsyncInvocation a2 = client.invokeAsync(new SerializableCallable("Query from client") {
      @Override
      public Object call() throws Exception {

        QueryService qs = null;
        SelectResults sr = null;
        // Execute query remotely
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        try {
          for (int i = 0; i < 100; i++) {
            sr = (SelectResults) qs.newQuery(query).execute(new Object[] {997, 1000});
          }
          Assert.assertTrue("Size of resultset should be greater than 0 for query: " + query,
              sr.size() > 0);
        } catch (Exception e) {
          Assert.fail("Failed executing query " + query, e);
        }

        return null;
      }
    });

    ThreadUtils.join(a1, 60 * 1000);
    ThreadUtils.join(a2, 60 * 1000);

    if (a1.exceptionOccurred()) {
      Assert.fail("Failed query execution " + a1.getException().getMessage());
    }

    if (a2.exceptionOccurred()) {
      Assert.fail("Failed query execution " + a2.getException());
    }

    closeClient(client);
    closeClient(server);

  }

}
