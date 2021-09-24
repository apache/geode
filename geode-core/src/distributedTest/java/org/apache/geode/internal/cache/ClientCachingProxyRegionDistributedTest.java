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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class ClientCachingProxyRegionDistributedTest implements Serializable {
  protected static final List<String> KEYS = Arrays.asList("Key1", "Key2");
  protected static final List<String> VALUES = Arrays.asList("Value1", "Value2");
  protected MemberVM server;
  protected ClientVM client;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server = cluster
        .startServerVM(1, cf -> cf.withRegion(RegionShortcut.REPLICATE, testName.getMethodName()));

    client = cluster.startClientVM(2, ccf -> ccf
        .withPoolSubscription(true)
        .withServerConnection(server.getPort()));

    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      Region<String, String> clientRegion = clientCache
          .<String, String>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(testName.getMethodName());

      IntStream.range(0, KEYS.size()).forEach(i -> clientRegion.put(KEYS.get(i), VALUES.get(i)));
    });
  }

  /**
   * See GEODE-7535.
   * This test executes a getAll operation from the client and, between the internal retrievals of
   * the actual entry within the {@link LocalRegion#basicGetAll(Collection, Object)} method, locally
   * destroy the entry to simulate an eviction/expiration action.
   * The region should try to get the entry from the server instead of failing with
   * {@link EntryDestroyedException}.
   */
  @Test
  public void getAllShouldNotThrowExceptionWhenEntryIsLocallyDeletedBetweenFetches() {
    client.invoke(() -> {
      final String testKey = KEYS.get(0);
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      LocalRegion clientRegion = spy((LocalRegion) ClusterStartupRule.getClientCache()
          .getRegion(testName.getMethodName()));

      doAnswer(invocation -> {
        // Retrieve the original entry.
        Object entry = invocation.callRealMethod();
        // Destroy the entry locally to simulate an expiration/eviction.
        clientRegion.localDestroy(testKey);
        // Return the original entry.
        return entry;
      }).when(clientRegion).accessEntry(KEYS.get(0), true);

      @SuppressWarnings("unchecked")
      Map<String, String> getAllResult = (Map<String, String>) clientRegion.getAll(KEYS);
      // Locally destroyed entries should be retrieved from the server.
      IntStream.range(0, KEYS.size())
          .forEach(i -> assertThat(getAllResult.get(KEYS.get(i))).isEqualTo(VALUES.get(i)));
    });
  }
}
