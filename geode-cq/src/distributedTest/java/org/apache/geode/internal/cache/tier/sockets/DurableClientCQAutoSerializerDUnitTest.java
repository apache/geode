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
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

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
public class DurableClientCQAutoSerializerDUnitTest implements Serializable {
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

  @Test
  public void testCorrectClassPathsAutoSerializer()
      throws Exception {

    String query1 = "SELECT * FROM " + SEPARATOR + REPLICATE_REGION_NAME;
    String query2 = "SELECT * FROM " + SEPARATOR + PARTITION_REGION_NAME;

    startDurableClient(TEST_OBJECT1_CLASS_PATH, TEST_OBJECT2_CLASS_PATH);
    createDurableCQs(query1, query2);
    verifyThatOnlyOneServerHostDurableSubscription();

    // Start another client and provision data with traffic that should trigger CQs
    startDataProvisionClient(TEST_OBJECT1_CLASS_PATH, TEST_OBJECT2_CLASS_PATH);
    provisionRegionsWithData();

    // Check that all events are received and successfully deserialized in cq listener
    checkCqEvents(LIST_TEST_OBJECT1.size(), LIST_TEST_OBJECT2.size());
    verifyThatOnlyOneServerHostDurableSubscription();
  }

  @Test
  public void testFaultyClassPathAutoSerializer()
      throws Exception {
    String query1 = "SELECT * FROM " + SEPARATOR + REPLICATE_REGION_NAME;
    String query2 = "SELECT * FROM " + SEPARATOR + PARTITION_REGION_NAME;
    startDurableClient(TEST_FAULTY_CLASS_PATH, TEST_OBJECT2_CLASS_PATH);
    createDurableCQs(query1, query2);
    verifyThatOnlyOneServerHostDurableSubscription();

    // Start another client and provision data with traffic that should trigger CQs
    startDataProvisionClient(TEST_OBJECT1_CLASS_PATH, TEST_OBJECT2_CLASS_PATH);
    provisionRegionsWithData();

    // Check that only events for which ReflectionBasedAutoSerializer is correctly set are received
    // and successfully deserialized in cq listener
    checkCqEvents(0, LIST_TEST_OBJECT2.size());
    verifyThatOnlyOneServerHostDurableSubscription();
  }

  private void startDataProvisionClient(String... patterns) throws Exception {
    int locatorPort = locator.getPort();
    client2 = cluster.startClientVM(4, ccf -> ccf
        .withLocatorConnection(locatorPort).withCacheSetup(c -> c
            .setPdxSerializer(new ReflectionBasedAutoSerializer(patterns))));
  }

  private void startDurableClient(String... patterns)
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
      DurableClientCQAutoSerializerDUnitTest.cqListener = cqListener;
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

  boolean isPrimaryServer(int primaryPort, MemberVM member) {
    return primaryPort == member.getPort();
  }

  private void verifyThatOnlyOneServerHostDurableSubscription() {
    int primPort = getPrimaryServerPort(client);
    verifyDurableClientPresence(server, isPrimaryServer(primPort, server));
    verifyDurableClientPresence(server2, isPrimaryServer(primPort, server2));
  }

  private void checkCqEvents(int expectedTestAutoSerializerObject1,
      int expectedTestAutoSerializerObject2) {
    // Check if number of events is correct
    client.invoke(() -> {
      await().untilAsserted(() -> assertThat(
          DurableClientCQAutoSerializerDUnitTest.cqListener.getNumEvents())
              .isEqualTo(expectedTestAutoSerializerObject1 + expectedTestAutoSerializerObject2));

      // Check if events are deserialized correctly
      if (expectedTestAutoSerializerObject1 != 0) {
        assertEquals(DurableClientCQAutoSerializerDUnitTest.cqListener.testAutoSerializerObject1,
            LIST_TEST_OBJECT1);
      }
      if (expectedTestAutoSerializerObject2 != 0) {
        assertEquals(DurableClientCQAutoSerializerDUnitTest.cqListener.testAutoSerializerObject2,
            LIST_TEST_OBJECT2);
      }
    });
  }

  private void verifyDurableClientPresence(MemberVM serverVM, boolean isExpected) {
    serverVM.invoke(() -> {
      await()
          .until(() -> isExpected == (getNumberOfClientProxies() == 1));

      if (isExpected) {
        // Get the CacheClientProxy or not (if proxy set is empty)
        CacheClientProxy proxy = getClientProxy();
        assertThat(proxy).isNotNull();
        // Verify that it is durable and its properties are correct
        assertThat(proxy.isDurable()).isTrue();
        assertThat(DURABLE_CLIENT_ID).isEqualTo(proxy.getDurableId());
      }
    });
  }

  private static CacheClientProxy getClientProxy() {
    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    java.util.Iterator<CacheClientProxy> i = getCacheClientNotifier().getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = i.next();
    }
    return proxy;
  }

  private static CacheClientNotifier getCacheClientNotifier() {
    // Get the CacheClientNotifier
    CacheServerImpl cacheServer = (CacheServerImpl) Objects
        .requireNonNull(ClusterStartupRule.getCache()).getCacheServers().iterator().next();
    assertNotNull(cacheServer);

    // Get the CacheClientNotifier
    return cacheServer.getAcceptor().getCacheClientNotifier();
  }

  private static int getNumberOfClientProxies() {
    return getCacheClientNotifier().getClientProxies().size();
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
}
