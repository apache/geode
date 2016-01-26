/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.query.dunit;

import org.junit.Ignore;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test for accessing query bind parameters from authorization callbacks
 * 
 * @author tnomulwar
 * 
 */
public class QueryParamsAuthorizationDUnitTest extends CacheTestCase {

  private final String regName = "exampleRegion";

  public QueryParamsAuthorizationDUnitTest(String name) {
    super(name);
  }

  public void testNothing() {
    // remove when Bug #51079 is fixed
  }
  @Ignore("Bug 51079")
  public void DISABLED_testQueryParamsInAuthCallback() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(1);
    // create servers and regions
    final int port = (Integer) server1.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        CacheFactory cf = new CacheFactory()
            .set("mcast-port", "0")
            .set("security-client-accessor",
                "com.gemstone.gemfire.cache.query.dunit.QueryAuthorization.create")
            .set("security-client-authenticator",
                "templates.security.DummyAuthenticator.create");
        Cache cache = getCache(cf);
        cache.createRegionFactory(RegionShortcut.REPLICATE).create(regName);
        CacheServer server = cache.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory()
            .addPoolServer(getServerHostName(server1.getHost()), port)
            .set("security-client-auth-init",
                "templates.security.UserPasswordAuthInit.create")
            .set("security-username", "root").set("security-password", "root");

        ClientCache cache = getClientCache(ccf);
        Region r1 = cache.createClientRegionFactory(
            ClientRegionShortcut.CACHING_PROXY).create(regName);

        for (int i = 0; i < 20; i++) {
          r1.put("key-" + i, new Portfolio(i));
        }

        QueryService qs = cache.getQueryService();
        Object[] params = new Object[] { "active", 0 };
        SelectResults sr = (SelectResults) qs.newQuery(
            "select * from " + r1.getFullPath()
                + " where status = $1 and ID > $2 ").execute(params);
        assertTrue("Result size should be greater than 0 ", sr.size() > 0);
        return null;
      }
    });
  }

}
