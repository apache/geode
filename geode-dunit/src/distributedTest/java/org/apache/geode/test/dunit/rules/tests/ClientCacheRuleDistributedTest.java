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
package org.apache.geode.test.dunit.rules.tests;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.Serializable;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipListener;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class ClientCacheRuleDistributedTest implements Serializable {

  private static ClientMembershipListener spyClientMembershipListener;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private String serverHost;
  private int serverPort;

  private String regionName;

  private VM serverVM;
  private VM clientVM;

  @Before
  public void setUp() {
    serverVM = getVM(0);
    clientVM = getVM(1);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();

    serverPort = serverVM.invoke(() -> {
      cacheRule.createCache();
      cacheRule.getCache().createRegionFactory(REPLICATE).create(regionName);

      spyClientMembershipListener = spy(ClientMembershipListener.class);
      ClientMembership.registerClientMembershipListener(spyClientMembershipListener);

      CacheServer cacheServer = cacheRule.getCache().addCacheServer();
      cacheServer.setPort(0);
      cacheServer.start();
      return cacheServer.getPort();
    });

    serverHost = getHostName();
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> spyClientMembershipListener = null);
  }

  @Test
  public void createClient() {
    clientVM.invoke(() -> {
      clientCacheRule
          .createClientCache(new ClientCacheFactory().addPoolServer(serverHost, serverPort));

      Region<String, String> region =
          clientCacheRule.getClientCache().<String, String>createClientRegionFactory(
              CACHING_PROXY).create(regionName);
      region.put("KEY", "VALUE");
    });

    serverVM.invoke(() -> {
      verify(spyClientMembershipListener, timeout(MINUTES.toMillis(2))).memberJoined(any());
    });
  }
}
