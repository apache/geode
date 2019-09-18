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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PREFIX;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.net.AvailablePortHelper;
import org.apache.geode.security.templates.DummyAuthenticator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * Test for accessing query bind parameters from authorization callbacks
 *
 *
 */
@Category({OQLQueryTest.class})
public class QueryParamsAuthorizationDUnitTest extends JUnit4CacheTestCase {

  private final String regName = "exampleRegion";
  private final AvailablePortHelper availablePortHelper = AvailablePortHelper.create();

  @Ignore("Bug 51079")
  @Test
  public void testQueryParamsInAuthCallback() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(1);
    // create servers and regions
    final int port = server1.invoke("Create Server1", () -> {
      CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0")
          .set(SECURITY_CLIENT_ACCESSOR,
              "org.apache.geode.cache.query.dunit.QueryAuthorization.create")
          .set(SECURITY_CLIENT_AUTHENTICATOR, DummyAuthenticator.class.getName() + ".create");
      Cache cache = getCache(cf);
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(regName);
      CacheServer server = cache.addCacheServer();
      int serverPort = availablePortHelper.getRandomAvailablePortForDUnitSite();
      server.setPort(serverPort);
      server.start();
      return serverPort;
    });

    // create client
    client.invoke("Create client", () -> {
      ClientCacheFactory ccf = new ClientCacheFactory()
          .addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port)
          .set(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create")
          .set(SECURITY_PREFIX + "username", "root").set(SECURITY_PREFIX + "password", "root");

      ClientCache cache = getClientCache(ccf);
      Region r1 =
          cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName);

      for (int i = 0; i < 20; i++) {
        r1.put("key-" + i, new Portfolio(i));
      }

      QueryService qs = cache.getQueryService();
      Object[] params = new Object[] {"active", 0};
      SelectResults sr = (SelectResults) qs
          .newQuery("select * from " + r1.getFullPath() + " where status = $1 and ID > $2 ")
          .execute(params);
      assertTrue("Result size should be greater than 0 ", sr.size() > 0);
      return null;
    });
  }

}
