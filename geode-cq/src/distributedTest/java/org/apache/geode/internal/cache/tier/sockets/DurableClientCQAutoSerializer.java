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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.AutoSerializableManager;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.categories.SerializationTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({ClientSubscriptionTest.class, SerializationTest.class})
public class DurableClientCQAutoSerializer implements Serializable {
  private static final String REPLICATE_REGION_NAME = "ReplicateRegion";
  private static final String PARTITION_REGION_NAME = "PartitionRegion";

  private MemberVM server;
  private MemberVM server2;
  private MemberVM locator;
  private ClientVM client;
  private ClientVM client2;

  private static TestAutoSerializerCqListener cqListener = null;

  private static final String TEST_OBJECT1_CLASS_PATH =
      "org.apache.geode.internal.cache.tier.sockets.TestAutoSerializerObject1";
  private static final String TEST_OBJECT2_CLASS_PATH =
      "org.apache.geode.internal.cache.tier.sockets.TestAutoSerializerObject2";
  private static final String TEST_FAULTY_CLASS_PATH =
      "org.apache.geode.internal.cache.tier.sockets.TestAutoSerializerObject2Faulty";
  private static final String DURABLE_CLIENT_ID = "durableClient";

  // Traffic data
  static final Map<String, TestAutoSerializerObject1> LIST_TEST_OBJECT1 = ImmutableMap.of(
      "key1", new TestAutoSerializerObject1("aa", "bb", 300),
      "key2", new TestAutoSerializerObject1("aa", "bb", 600),
      "key3", new TestAutoSerializerObject1("aaa", "bbb", 500));

  static final Map<String, TestAutoSerializerObject2> LIST_TEST_OBJECT2 = ImmutableMap.of(
      "key1", new TestAutoSerializerObject2("cc", "ddd", 300),
      "key2", new TestAutoSerializerObject2("cc", "dddd", 400));

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(5);

  @Before
  public void setUp() throws Exception {
    Invoke.invokeInEveryVM(
        () -> System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true"));

    locator =
        cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    server = cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort));

    server2 = cluster.startServerVM(2,
        s -> s.withConnectionToLocator(locatorPort));

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "configure pdx --auto-serializable-classes='" + TEST_OBJECT1_CLASS_PATH + ", "
            + TEST_OBJECT2_CLASS_PATH + "'")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=" + REPLICATE_REGION_NAME + " --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=" + PARTITION_REGION_NAME + " --type=PARTITION")
        .statusIsSuccess();

    locator.invoke(() -> {
      ClusterStartupRule.memberStarter
          .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + REPLICATE_REGION_NAME, 2);
      ClusterStartupRule.memberStarter
          .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + PARTITION_REGION_NAME, 2);
    });
  }

  private void startTrafficClient(String... patterns) throws Exception {
    int locatorPort = locator.getPort();
    client2 = cluster.startClientVM(4, ccf -> ccf
        .withLocatorConnection(locatorPort).withCacheSetup(c -> c
            .setPdxSerializer(new ReflectionBasedAutoSerializer(patterns))));
  }

  private void startDurableClientCq(String... patterns)
      throws Exception {
    int locatorPort = locator.getPort();
    client = cluster.startClientVM(3, ccf -> ccf
        .withPoolSubscription(true).withLocatorConnection(locatorPort).withCacheSetup(c -> c
            .setPdxSerializer(new ReflectionBasedAutoSerializer(patterns))
            .set("durable-client-id", DURABLE_CLIENT_ID)));
  }

  private void createDurableCQs(String... queries) {
    client.invoke(() -> {
      TestAutoSerializerCqListener cqListener = new TestAutoSerializerCqListener();
      DurableClientCQAutoSerializer.cqListener = cqListener;
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
      cqAttributesFactory.addCqListener(cqListener);

      for (String query : queries) {
        CqQuery cq = queryService.newCq(query, cqAttributesFactory.create(), true);
        cq.execute();
      }
      ClusterStartupRule.getClientCache().readyForEvents();
    });
  }

  private static void verifyDurableClientOnServer(MemberVM server, boolean isExpected) {
    server.invoke(() -> {
      CacheServerImpl cacheServer =
          (CacheServerImpl) ClusterStartupRule.getCache().getCacheServers().iterator().next();
      assertNotNull(cacheServer);

      // Get the CacheClientNotifier
      CacheClientNotifier notifier = cacheServer.getAcceptor().getCacheClientNotifier();

      // Get the CacheClientProxy or not (if proxy set is empty)
      CacheClientProxy proxy = null;
      Iterator i = notifier.getClientProxies().iterator();
      if (i.hasNext()) {
        proxy = (CacheClientProxy) i.next();
      }

      if (isExpected) {
        assertNotNull(proxy);
        assertThat(proxy.getDurableId().equals(DURABLE_CLIENT_ID));
      } else {
        if (proxy != null) {
          assertNotEquals(proxy.getDurableId(), DURABLE_CLIENT_ID);
        }
      }
    });
  }

  boolean isPrimaryServer(int primaryPort, MemberVM member) {
    return primaryPort == member.getPort();
  }

  private void verifyOnlyPrimaryServerHostDurableSubscription() {
    int primPort = getPrimaryServerPort(client);

    verifyDurableClientOnServer(server, isPrimaryServer(primPort, server));
    verifyDurableClientOnServer(server2, isPrimaryServer(primPort, server2));
  }

  private void checkCqEvents(int expectedTestAutoSerializerObject1,
      int expectedTestAutoSerializerObject2) {
    // Check if number of events is correct
    client.invoke(() -> {
      await().untilAsserted(() -> assertThat(
          DurableClientCQAutoSerializer.cqListener.getNumEvents())
              .isEqualTo(expectedTestAutoSerializerObject1 + expectedTestAutoSerializerObject2));

      // Check if events are deserialized correctly
      if (expectedTestAutoSerializerObject1 != 0) {
        assertEquals(DurableClientCQAutoSerializer.cqListener.testAutoSerializerObject1,
            LIST_TEST_OBJECT1);
      }
      if (expectedTestAutoSerializerObject2 != 0) {
        assertEquals(DurableClientCQAutoSerializer.cqListener.testAutoSerializerObject2,
            LIST_TEST_OBJECT2);
      }
    });
  }

  private void provisionRegionsWithData() {
    client2.invoke(() -> {
      ClientRegionFactory factory =
          ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, TestAutoSerializerObject1> region = factory.create(REPLICATE_REGION_NAME);

      // provision TestAutoSerializerObject1 data
      for (Map.Entry<String, TestAutoSerializerObject1> entry : LIST_TEST_OBJECT1.entrySet()) {
        region.put(entry.getKey(), entry.getValue());
      }

      Region<String, TestAutoSerializerObject2> region2 = factory.create(PARTITION_REGION_NAME);
      // provision TestAutoSerializerObject2 data
      for (Map.Entry<String, TestAutoSerializerObject2> entry : LIST_TEST_OBJECT2.entrySet()) {
        region2.put(entry.getKey(), entry.getValue());
      }
    });
  }

  private int getPrimaryServerPort(ClientVM client) {
    return client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      PoolImpl pool = (PoolImpl) cache.getDefaultPool();
      return pool.getPrimaryPort();
    });
  }

  @Test
  public void correctClassPathsAutoSerialize()
      throws Exception {

    String query1 = "SELECT * FROM " + SEPARATOR + REPLICATE_REGION_NAME;
    String query2 = "SELECT * FROM " + SEPARATOR + PARTITION_REGION_NAME;
    startDurableClientCq(TEST_OBJECT1_CLASS_PATH, TEST_OBJECT2_CLASS_PATH);
    createDurableCQs(query1, query2);

    startTrafficClient(TEST_OBJECT1_CLASS_PATH, TEST_OBJECT2_CLASS_PATH);
    provisionRegionsWithData();

    checkCqEvents(LIST_TEST_OBJECT1.size(), LIST_TEST_OBJECT2.size());
    verifyOnlyPrimaryServerHostDurableSubscription();
  }

  @Test
  public void faultyClassPathAutoSerializer()
      throws Exception {
    String query1 = "SELECT * FROM " + SEPARATOR + REPLICATE_REGION_NAME;
    String query2 = "SELECT * FROM " + SEPARATOR + PARTITION_REGION_NAME;
    startDurableClientCq(TEST_FAULTY_CLASS_PATH, TEST_OBJECT2_CLASS_PATH);
    createDurableCQs(query1, query2);

    startTrafficClient(TEST_OBJECT1_CLASS_PATH, TEST_OBJECT2_CLASS_PATH);
    provisionRegionsWithData();

    checkCqEvents(0, LIST_TEST_OBJECT2.size());
    verifyOnlyPrimaryServerHostDurableSubscription();
  }
}
