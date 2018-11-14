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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.ThrowableAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class InternalCacheForClientAccessDUnitTest<tearDown> implements java.io.Serializable {
  public static final long serialVersionUID = 1l;

  @Rule
  public DistributedRule dunitRule = new DistributedRule();

  VM serverVM;
  VM clientVM;
  static GemFireCache cache;

  @Before
  public void setup() {
    serverVM = VM.getVM(0);
    clientVM = VM.getVM(1);

    final int locatorPort = DistributedTestUtils.getDUnitLocatorPort();
    // create a server cache and a client cache
    serverVM.invoke(() -> {
      String locatorSpec = "localhost[" + locatorPort + "]";
      CacheFactory config = new CacheFactory();
      config.set(
          ConfigurationProperties.LOCATORS, locatorSpec);
      config.set(
          ConfigurationProperties.NAME, "server1");

      Cache serverCache = config.create();
      cache = serverCache;

      CacheServer cacheServer = serverCache.addCacheServer();
      cacheServer.start();
    });
    clientVM.invoke(() -> {
      ClientCacheFactory config = new ClientCacheFactory();
      config.addPoolLocator("localhost", locatorPort);
      config.setPoolSubscriptionEnabled(true);
      cache = config.create();
    });
  }

  @After
  public void tearDown() {
    serverVM.invoke(() -> {
      DistributedSystem system = cache.getDistributedSystem();
      cache.close();
      cache = null;
      assertThat(system.isConnected()).isFalse();
    });
    clientVM.invoke(() -> {
      cache.close();
      cache = null;
    });
  }

  @Test
  public void serverUsesFilteredCache() {
    serverVM.invoke(() -> {
      Cache serverCache = (Cache) cache;
      serverCache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
    });
    clientVM.invoke(() -> {
      ClientCache clientCache = (ClientCache) cache;
      Region<String, String> region =
          clientCache.<String, String>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
              .create("region");
      region.put("Object1", "Value1");
    });
    serverVM.invoke(() -> {
      Cache serverCache = (Cache) cache;
      CacheServer server = serverCache.getCacheServers().stream().findFirst().get();
      CacheServerImpl impl = (CacheServerImpl) server;
      assertThat(impl.getAcceptor().getCachedRegionHelper().getCache())
          .isInstanceOf(InternalCacheForClientAccess.class);
    });
  }

  @Test
  public void invokeClientOperationsOnInternalRegion() {
    serverVM.invoke(() -> {
      // we need to use internal APIs to create an "internal" region
      GemFireCacheImpl serverCache = (GemFireCacheImpl) cache;
      InternalRegionArguments internalRegionArguments = new InternalRegionArguments();
      internalRegionArguments.setIsUsedForPartitionedRegionAdmin(true);
      RegionAttributes<String, String> attributes =
          serverCache.getRegionAttributes(RegionShortcut.REPLICATE.toString());
      LocalRegion region = (LocalRegion) serverCache.createVMRegion("internalRegion", attributes,
          internalRegionArguments);
      assertThat(region.isInternalRegion()).isTrue();
    });
    clientVM.invoke(this::testAllOperations);
  }

  private void testAllOperations() {
    ClientCache clientCache = (ClientCache) cache;
    Region<String, String> region =
        clientCache.<String, String>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create("internalRegion");

    assertFailure(() -> region.create("Object1", "Value1"));
    assertFailure(() -> region.put("Object1", "Value1"));
    assertFailure(() -> region.putIfAbsent("Object1", "Value1"));

    assertFailure(() -> region.get("Object1"));

    Map<String, String> map = new HashMap<>();
    map.put("Object1", "Value1");
    assertFailure(() -> region.putAll(map));

    List<String> list = new ArrayList<>();
    list.add("Object1");
    assertFailure(() -> region.getAll(list));
    assertFailure(() -> region.removeAll(list));

    assertFailure(() -> region.destroy("Object1"));
    assertFailure(() -> region.remove("Object1"));
    assertFailure(() -> region.replace("Object1", "oldValue", "newValue"));
    assertFailure(() -> region.invalidate("Object1"));
    assertFailure(region::keySetOnServer);

    assertFailure(() -> region.registerInterest("Object1"));
  }

  private void assertFailure(final ThrowableAssert.ThrowingCallable callable) {
    assertThatExceptionOfType(ServerOperationException.class).isThrownBy(callable)
        .withCauseInstanceOf(
            NotAuthorizedException.class)
        .withMessageContaining(
            "The region internalRegion is an internal region that a client is never allowed to access");

  }
}
