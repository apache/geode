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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.beans.QueryDataFunction;
import org.apache.geode.management.internal.cli.json.TypedJson;
import org.apache.geode.management.model.EmptyObject;
import org.apache.geode.management.model.Item;
import org.apache.geode.management.model.Order;
import org.apache.geode.management.model.SubOrder;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Functional integration tests for {@link QueryDataFunction}.
 *
 * TODO: this test should really have some assertions
 *
 * @since GemFire 8.1
 */
@Category({IntegrationTest.class, GfshTest.class})
public class QueryDataFunctionIntegrationTest {

  private static final String REPLICATED_REGION = "exampleRegion";
  private static final String QUERY_1 = "SELECT * FROM /exampleRegion";

  /**
   * Number of rows queryData operation will return. By default it will be 1000
   */
  private static final int queryResultSetLimit = ManagementConstants.DEFAULT_QUERY_LIMIT;

  /**
   * Number of elements to be shown in queryData operation if query results contain collections like
   * Map, List etc.
   */
  private static final int queryCollectionsDepth = TypedJson.DEFAULT_COLLECTION_ELEMENT_LIMIT;

  private InternalDistributedSystem system;
  private Region<Object, Object> replicatedRegion;
  private DistributedMember member;
  private Cache cache;

  @Before
  public void setUp() throws Exception {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(ENABLE_TIME_STATISTICS, "true");
    config.setProperty(STATISTIC_SAMPLING_ENABLED, "false");
    config.setProperty(STATISTIC_SAMPLE_RATE, "60000");
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "true");
    config.setProperty(JMX_MANAGER_HTTP_PORT, "0");
    config.setProperty(JMX_MANAGER_PORT, "0");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);
    member = system.getDistributedMember();
    Cache cache = new CacheFactory().create();

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    replicatedRegion = regionFactory.<String, Object>create(REPLICATED_REGION);
  }

  @After
  public void tearDown() throws Exception {
    system.disconnect();
  }

  /**
   * Tests a model where Objects have a circular reference with object reference. e.g. Order1--
   * Has--> Items each Item --Has --> OrderN where (OrderN == Order1)
   * <p>
   *
   * RegressionTest for TRAC #51048: Disk Read/ Write shows negative at cluster level JMX
   */
  @Test
  public void testCyclicWithNestedObjectReference() throws Exception {
    Order order = new Order();
    order.setId("test");

    for (int subOrderIndex = 1; subOrderIndex <= 5; subOrderIndex++) {
      Item item = new Item(order, "ID_" + subOrderIndex, "Book");
      order.addItem(item);
    }

    replicatedRegion.put("order1", order);

    queryData(QUERY_1, "", 0);
  }

  /**
   * Tests a model where Objects have a circular reference with their class types. e.g. Order1--
   * Has--> Items each Item --Has --> OrderN where (OrderN != Order1)
   */
  @Test
  public void testCyclicWithNestedClasses() throws Exception {
    Order order = new Order();
    order.setId("test");

    for (int subOrderIndex = 1; subOrderIndex <= 5; subOrderIndex++) {
      Order subOrder = new Order();
      subOrder.setId("ORDER_ID_" + subOrderIndex);

      Item item = new Item(subOrder, "ID_" + subOrderIndex, "Book");
      order.addItem(item);
    }

    replicatedRegion.put("order1", order);

    queryData(QUERY_1, "", 0);
  }

  /**
   * Tests a model where Objects have a circular reference with their class types. e.g. Order1--
   * Has--> Items each Item --Has --> OrderN where (OrderN != Order1)
   */
  @Test
  public void testCyclicWithNestedRefernce2ndLayer() throws Exception {
    Collection<Item> items = new ArrayList<>();
    Order order = new Order("ORDER_TEST", items);

    for (int subOrderIndex = 1; subOrderIndex <= 5; subOrderIndex++) {
      Order subOrder = new Order();
      subOrder.setId("ORDER_ID_" + subOrderIndex);
      subOrder.setItems(items);

      Item item = new Item(subOrder, "ID_" + subOrderIndex, "Book");
      order.addItem(item);
    }

    replicatedRegion.put("order1", order);

    queryData(QUERY_1, "", 0);
  }

  @Test
  public void testCyclicWithCollection1stLayer() throws Exception {
    Collection<Item> items = new ArrayList<>();
    Order order = new Order("ORDER_TEST", items);

    for (int subOrderIndex = 1; subOrderIndex <= 5; subOrderIndex++) {
      Order subOrder = new Order();
      subOrder.setId("ORDER_ID_" + subOrderIndex);
      subOrder.setItems(items);

      Item item = new Item(subOrder, "ID_" + subOrderIndex, "Book");
      order.addItem(item);
    }

    replicatedRegion.put("items", items);

    queryData(QUERY_1, "", 0);
  }

  @Test
  public void testCyclicCollectionWithMultipleObjects() throws Exception {
    for (int orderIndex = 1; orderIndex <= 5; orderIndex++) {
      Collection<Item> items = new ArrayList<>();
      Order order = new Order("ORDER_TEST_" + orderIndex, items);

      for (int subOrderIndex = 1; subOrderIndex <= 5; subOrderIndex++) {
        Order subOrder = new Order();
        subOrder.setId("ORDER_ID_" + subOrderIndex);
        subOrder.setItems(items);

        Item item = new Item(subOrder, "ID_" + subOrderIndex, "Book");
        order.addItem(item);
      }

      replicatedRegion.put("items_" + orderIndex, items);
    }

    queryData(QUERY_1, "", 0);
  }

  @Test
  public void testCyclicArrayMultipleObjects() throws Exception {
    for (int orderIndex = 1; orderIndex <= 5; orderIndex++) {
      Collection<Item> items = new ArrayList<>();
      Order order = new Order("ORDER_TEST_" + orderIndex, items);

      for (int subOrderIndex = 1; subOrderIndex <= 5; subOrderIndex++) {
        Order subOrder = new Order();
        subOrder.setId("ORDER_ID_" + subOrderIndex);
        subOrder.setItems(items);

        Item item = new Item(subOrder, "ID_" + subOrderIndex, "Book");
        order.addItem(item);
      }

      replicatedRegion.put("items_" + orderIndex, items);
    }

    queryData(QUERY_1, "", 0);
  }

  @Test
  public void testCyclicArrayMultipleObjectsMemberWise() throws Exception {
    Portfolio[] portfolios = createPortfoliosAndPositions(1);
    int portfolioId = 1;
    for (Portfolio portfolio : portfolios) {
      replicatedRegion.put(portfolioId, portfolio);
      portfolioId++;
    }

    queryData(QUERY_1, member.getId(), 0);
  }

  @Test
  public void testEmptyObject() throws Exception {
    EmptyObject emptyObject = new EmptyObject();
    replicatedRegion.put("port", emptyObject);
    queryData(QUERY_1, "", 0);
  }

  @Test
  public void testSubClassOverridingMethods() throws Exception {
    SubOrder subOrder = new SubOrder();
    replicatedRegion.put("port", subOrder);
    queryData(QUERY_1, "", 0);
  }

  @Test
  public void testNestedPDXObject() throws Exception {
    PdxInstanceFactory factory = PdxInstanceFactoryImpl.newCreator("Portfolio", false,
        (InternalCache) replicatedRegion.getCache());

    factory.writeInt("ID", 111);
    factory.writeString("status", "active");
    factory.writeString("secId", "IBM");

    PdxInstance pdxInstance = factory.create();

    replicatedRegion.put("port", pdxInstance);

    queryData(QUERY_1, "", 0);
  }

  @Test
  public void testArrayWithNullValues() throws Exception {
    SubOrder[] subOrderArray = new SubOrder[2];
    subOrderArray[0] = new SubOrder();
    subOrderArray[1] = null;

    replicatedRegion.put("p1", subOrderArray);

    queryData(QUERY_1, "", 0);
  }

  @Test
  public void testWithSqlDate() throws Exception {
    SubOrder[] subOrderArray = new SubOrder[2];
    subOrderArray[0] = new SubOrder();
    subOrderArray[1] = null;

    replicatedRegion.put("p1", subOrderArray);

    queryData(QUERY_1, "", 0);
  }

  private void queryData(final String query, final String members, final int limit)
      throws Exception {
    Object result = QueryDataFunction.queryData(query, members, limit, false, queryResultSetLimit,
        queryCollectionsDepth);
    String queryResult = (String) result;
    System.out.println("Query Result: " + queryResult);

    // If not correct JSON format this will throw a JSONException
    JSONObject jsonObject = new JSONObject(queryResult);
    System.out.println("Query Result: " + jsonObject);
  }

  private Portfolio[] createPortfoliosAndPositions(final int count) {
    Position.cnt = 0; // reset Portfolio counter
    Portfolio[] portfolios = new Portfolio[count];
    for (int i = 0; i < count; i++) {
      portfolios[i] = new Portfolio(i);
    }
    return portfolios;
  }
}
