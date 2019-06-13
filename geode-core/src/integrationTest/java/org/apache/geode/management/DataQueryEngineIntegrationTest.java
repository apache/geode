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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.beans.DataQueryEngine;
import org.apache.geode.management.internal.beans.QueryDataFunction;
import org.apache.geode.management.model.EmptyObject;
import org.apache.geode.management.model.Item;
import org.apache.geode.management.model.Order;
import org.apache.geode.management.model.SubOrder;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * Functional integration tests for {@link DataQueryEngine}.
 */
@Category(GfshTest.class)
public class DataQueryEngineIntegrationTest {

  private static final String REGION_NAME = "exampleRegion";
  private static final String QUERY_1 = "SELECT * FROM /exampleRegion";

  /**
   * Number of rows queryData operation will return. By default it will be 1000
   */
  private static final int queryResultSetLimit = ManagementConstants.DEFAULT_QUERY_LIMIT;

  /**
   * Number of elements to be shown in queryData operation if query results contain collections like
   * Map, List etc.
   */
  private static final int queryCollectionsDepth =
      QueryDataFunction.DEFAULT_COLLECTION_ELEMENT_LIMIT;

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withNoCacheServer()
      .withJMXManager().withRegion(RegionShortcut.REPLICATE, REGION_NAME).withAutoStart();
  private Region<Object, Object> region;
  private DataQueryEngine queryEngine;

  @Before
  public void setUp() throws Exception {
    region = server.getCache().getRegion(REGION_NAME);
    queryEngine = new DataQueryEngine((SystemManagementService) server.getManagementService(),
        server.getCache());
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

    region.put("order1", order);

    String expectedResult =
        "{\"result\":[[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"test\"],\"items\":[\"java.util.ArrayList\",{\"0\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_1\"],\"order\":\"duplicate org.apache.geode.management.model.Order\"}],\"1\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_2\"],\"order\":\"duplicate org.apache.geode.management.model.Order\"}],\"2\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_3\"],\"order\":\"duplicate org.apache.geode.management.model.Order\"}],\"3\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_4\"],\"order\":\"duplicate org.apache.geode.management.model.Order\"}],\"4\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_5\"],\"order\":\"duplicate org.apache.geode.management.model.Order\"}]}]}]]}";
    String queryResult =
        queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit, queryCollectionsDepth);

    assertThat(queryResult).isEqualToIgnoringWhitespace(expectedResult);
    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
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

    region.put("order1", order);

    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);

    String expectedResult =
        "{\"result\":[[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"test\"],\"items\":[\"java.util.ArrayList\",{\"0\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_1\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_1\"],\"items\":[\"java.util.ArrayList\",{}]}]}],\"1\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_2\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_2\"],\"items\":[\"java.util.ArrayList\",{}]}]}],\"2\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_3\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_3\"],\"items\":[\"java.util.ArrayList\",{}]}]}],\"3\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_4\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_4\"],\"items\":[\"java.util.ArrayList\",{}]}]}],\"4\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_5\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_5\"],\"items\":[\"java.util.ArrayList\",{}]}]}]}]}]]}";
    assertThat(queryResult).isEqualToIgnoringWhitespace(expectedResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
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

    region.put("order1", order);

    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);
    String expectedResult =
        "{\"result\":[[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_TEST\"],\"items\":[\"java.util.ArrayList\",{\"0\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_1\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_1\"],\"items\":[\"java.util.ArrayList\",{\"0\":\"duplicate org.apache.geode.management.model.Item\",\"1\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_2\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_2\"],\"items\":[\"java.util.ArrayList\",{\"0\":\"duplicate org.apache.geode.management.model.Item\",\"1\":\"duplicate org.apache.geode.management.model.Item\",\"2\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_3\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_3\"],\"items\":[\"java.util.ArrayList\",{\"0\":\"duplicate org.apache.geode.management.model.Item\",\"1\":\"duplicate org.apache.geode.management.model.Item\",\"2\":\"duplicate org.apache.geode.management.model.Item\",\"3\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_4\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_4\"],\"items\":[\"java.util.ArrayList\",{\"0\":\"duplicate org.apache.geode.management.model.Item\",\"1\":\"duplicate org.apache.geode.management.model.Item\",\"2\":\"duplicate org.apache.geode.management.model.Item\",\"3\":\"duplicate org.apache.geode.management.model.Item\",\"4\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"Book\"],\"itemId\":[\"java.lang.String\",\"ID_5\"],\"order\":[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"ORDER_ID_5\"],\"items\":[\"java.util.ArrayList\",{\"0\":\"duplicate org.apache.geode.management.model.Item\",\"1\":\"duplicate org.apache.geode.management.model.Item\",\"2\":\"duplicate org.apache.geode.management.model.Item\",\"3\":\"duplicate org.apache.geode.management.model.Item\",\"4\":\"duplicate org.apache.geode.management.model.Item\"}]}]}]}]}]}],\"4\":\"duplicate org.apache.geode.management.model.Item\"}]}]}],\"3\":\"duplicate org.apache.geode.management.model.Item\",\"4\":\"duplicate org.apache.geode.management.model.Item\"}]}]}],\"2\":\"duplicate org.apache.geode.management.model.Item\",\"3\":\"duplicate org.apache.geode.management.model.Item\",\"4\":\"duplicate org.apache.geode.management.model.Item\"}]}]}],\"1\":\"duplicate org.apache.geode.management.model.Item\",\"2\":\"duplicate org.apache.geode.management.model.Item\",\"3\":\"duplicate org.apache.geode.management.model.Item\",\"4\":\"duplicate org.apache.geode.management.model.Item\"}]}]]}";
    assertThat(queryResult).isEqualToIgnoringWhitespace(expectedResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
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

    region.put("items", items);

    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);
    System.out.println("Query Result: " + queryResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
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

      region.put("items_" + orderIndex, items);
    }

    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);
    System.out.println("Query Result: " + queryResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
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

      region.put("items_" + orderIndex, items);
    }

    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);
    System.out.println("Query Result: " + queryResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);

  }

  @Test
  public void testCyclicArrayMultipleObjectsMemberWise() throws Exception {
    region.put(1, new Portfolio(0));

    String queryResult = queryEngine.queryForJsonResult(QUERY_1, "server", 0, queryResultSetLimit,
        queryCollectionsDepth);
    System.out.println(queryResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
  }

  @Test
  public void testEmptyObject() throws Exception {
    EmptyObject emptyObject = new EmptyObject();
    region.put("port", emptyObject);
    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);
    String expectedResult = "{\"result\":[[\"org.apache.geode.management.model.EmptyObject\",{}]]}";
    assertThat(queryResult).isEqualToIgnoringWhitespace(expectedResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
  }

  @Test
  public void testSubClassOverridingMethods() throws Exception {
    SubOrder subOrder = new SubOrder();
    region.put("port", subOrder);
    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);
    String expectedResult =
        "{\"result\":[[\"org.apache.geode.management.model.SubOrder\",{\"id\":[\"java.lang.String\",\"null1\"],\"items\":[\"java.util.ArrayList\",{}]}]]}";
    assertThat(queryResult).isEqualToIgnoringWhitespace(expectedResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
  }

  @Test
  public void testNestedPDXObject() throws Exception {
    PdxInstanceFactory factory = PdxInstanceFactoryImpl.newCreator("Portfolio", false,
        (InternalCache) region.getRegionService());

    factory.writeInt("ID", 111);
    factory.writeString("status", "active");
    factory.writeString("secId", "IBM");

    PdxInstance pdxInstance = factory.create();

    region.put("port", pdxInstance);

    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);
    String expectedResult =
        "{\"result\":[[\"org.apache.geode.pdx.PdxInstance\",{\"ID\":[\"java.lang.Integer\",111],\"status\":[\"java.lang.String\",\"active\"],\"secId\":[\"java.lang.String\",\"IBM\"]}]]}";
    assertThat(queryResult).isEqualToIgnoringWhitespace(expectedResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
  }

  @Test
  public void testArrayWithNullValues() throws Exception {
    SubOrder[] subOrderArray = new SubOrder[2];
    subOrderArray[0] = new SubOrder();
    subOrderArray[1] = null;

    region.put("p1", subOrderArray);

    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);
    String expectedResult =
        "{\"result\":[[\"org.apache.geode.management.model.SubOrder[]\",[{\"id\":[\"java.lang.String\",\"null1\"],\"items\":[\"java.util.ArrayList\",{}]},null]]]}";
    assertThat(queryResult).isEqualToIgnoringWhitespace(expectedResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
  }

  @Test
  public void testWithSqlDate() throws Exception {
    SubOrder[] subOrderArray = new SubOrder[2];
    subOrderArray[0] = new SubOrder();
    subOrderArray[1] = null;

    region.put("p1", subOrderArray);

    String queryResult = queryEngine.queryForJsonResult(QUERY_1, 0, queryResultSetLimit,
        queryCollectionsDepth);
    String expectedResult =
        "{\"result\":[[\"org.apache.geode.management.model.SubOrder[]\",[{\"id\":[\"java.lang.String\",\"null1\"],\"items\":[\"java.util.ArrayList\",{}]},null]]]}";
    assertThat(queryResult).isEqualToIgnoringWhitespace(expectedResult);

    // If not correct JSON format this will throw a JSONException
    new ObjectMapper().readTree(queryResult);
  }

  @Test
  public void testWithUnknownRegion() throws Exception {
    String queryResult = queryEngine.queryForJsonResult("select * from /unknonwn", 1, 1, 1);
    assertThat(queryResult)
        .isEqualTo("{\"message\":\"Cannot find regions /unknonwn in any of the members\"}");
  }


  public static class Root {
    public Root getRoot() {
      return this;
    }

    public Child getChild() { // getter that returns unique instances
      return new Child();
    }
  }

  public static class Child {
    public Child getChild() {
      return this;
    }
  }

}
