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
package com.gemstone.gemfire.management;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.beans.QueryDataFunction;
import com.gemstone.gemfire.management.internal.cli.json.TypedJson;
import com.gemstone.gemfire.management.model.EmptyObject;
import com.gemstone.gemfire.management.model.Item;
import com.gemstone.gemfire.management.model.Order;
import com.gemstone.gemfire.management.model.SubOrder;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;
import com.gemstone.gemfire.pdx.internal.PdxInstanceFactoryImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.fail;

/**
 * @since GemFire 8.1
 *
 */
@Category(IntegrationTest.class)
public class DataBrowserJSONValidationJUnitTest {

  protected static final long SLEEP = 100;
  protected static final long TIMEOUT = 4 * 1000;

  protected InternalDistributedSystem system;

  private Cache cache;

  Region replicatedRegion;

  private static final String REPLICATED_REGION = "exampleRegion";

  /**
   * Number of rows queryData operation will return. By default it will be 1000
   */
  private int queryResultSetLimit = ManagementConstants.DEFAULT_QUERY_LIMIT;

  /**
   * NUmber of elements to be shown in queryData operation if query results
   * contain collections like Map, List etc.
   */
  private int queryCollectionsDepth = TypedJson.DEFAULT_COLLECTION_ELEMENT_LIMIT;

  private String QUERY_1 = "SELECT * FROM /exampleRegion";

  @After
  public void tearDown() throws Exception {
    this.system.disconnect();
    this.system = null;
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
    // System.setProperty("gemfire.stats.debug.debugSampleCollector", "true");

    final Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "false");
    props.setProperty(STATISTIC_SAMPLE_RATE, "60000");
    props.setProperty(JMX_MANAGER, "true");
    props.setProperty(JMX_MANAGER_START, "true");
    props.setProperty(JMX_MANAGER_HTTP_PORT, "0");
    props.setProperty(JMX_MANAGER_PORT, "0");

    this.system = (InternalDistributedSystem) DistributedSystem.connect(props);

    this.cache = new CacheFactory().create();
    RegionFactory replicatedRegionFac = cache.createRegionFactory(RegionShortcut.REPLICATE);
    replicatedRegion = replicatedRegionFac.create(REPLICATED_REGION);

  }

  private void queryData(String query, String members, int limit) {

    try {
      Object result = QueryDataFunction.queryData(query, members, limit, false, queryResultSetLimit,
          queryCollectionsDepth);
      String queryResult = (String) result;

      System.out.println("Query Result :" + queryResult.toString());
      JSONObject jobj = new JSONObject(queryResult);// If not correct JSON
                                                    // format this will throw a
                                                    // JSONException
      System.out.println("Query Result :" + jobj.toString());

    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }

  }

  /**
   * #Issue 51048 Tests a model where Objects have a circular reference with
   * object reference. e.g. Order1-- Has--> Items each Item --Has --> OrderN
   * where (OrderN == Order1)
   */
  @Test
  public void testCyclicWithNestedObjectReference() {

    Order order = new Order();

    order.setId("test");

    for (int i = 1; i <= 5; i++) {
      Item item = new Item(order, "ID_" + i, "Book");
      order.addItem(item);
    }
    replicatedRegion.put("oreder1", order);

    queryData(QUERY_1, "", 0);

  }

  /**
   * Tests a model where Objects have a circular reference with their class
   * types. e.g. Order1-- Has--> Items each Item --Has --> OrderN where (OrderN
   * != Order1)
   */
  @Test
  public void testCyclicWithNestedClasses() {

    Order order = new Order();
    order.setId("test");

    for (int i = 1; i <= 5; i++) {
      Order ord = new Order();
      ord.setId("ORDER_ID_" + i);
      Item item = new Item(ord, "ID_" + i, "Book");
      order.addItem(item);
    }

    replicatedRegion.put("oreder1", order);

    queryData(QUERY_1, "", 0);

  }

  /**
   * Tests a model where Objects have a circular reference with their class
   * types. e.g. Order1-- Has--> Items each Item --Has --> OrderN where (OrderN
   * != Order1)
   */
  @Test
  public void testCyclicWithNestedRefernce2ndLayer() {

    Collection<Item> items = new ArrayList<Item>();
    Order order = new Order("ORDER_TEST", items);

    for (int i = 1; i <= 5; i++) {
      Order ord = new Order();
      ord.setId("ORDER_ID_" + i);
      ord.setItems(items);
      Item item = new Item(ord, "ID_" + i, "Book");
      order.addItem(item);

    }

    replicatedRegion.put("oreder1", order);
    queryData(QUERY_1, "", 0);

  }

  @Test
  public void testCyclicWithCollection1stLayer() {

    Collection<Item> items = new ArrayList<Item>();
    Order order = new Order("ORDER_TEST", items);

    for (int i = 1; i <= 5; i++) {
      Order ord = new Order();
      ord.setId("ORDER_ID_" + i);
      ord.setItems(items);
      Item item = new Item(ord, "ID_" + i, "Book");
      order.addItem(item);

    }

    replicatedRegion.put("items", items);
    queryData(QUERY_1, "", 0);

  }

  @Test
  public void testCyclicCollectionWithMultipleObjects() {

    for (int j = 1; j <= 5; j++) {
      Collection<Item> items = new ArrayList<Item>();
      Order order = new Order("ORDER_TEST_" + j, items);

      for (int i = 1; i <= 5; i++) {
        Order ord = new Order();
        ord.setId("ORDER_ID_" + i);
        ord.setItems(items);
        Item item = new Item(ord, "ID_" + i, "Book");
        order.addItem(item);
      }
      replicatedRegion.put("items_" + j, items);
    }

    queryData(QUERY_1, "", 0);

  }

  @Test
  public void testCyclicArrayMultipleObjects() {

    for (int j = 1; j <= 5; j++) {
      Collection<Item> items = new ArrayList<Item>();
      Order order = new Order("ORDER_TEST_" + j, items);

      for (int i = 1; i <= 5; i++) {
        Order ord = new Order();
        ord.setId("ORDER_ID_" + i);
        ord.setItems(items);
        Item item = new Item(ord, "ID_" + i, "Book");
        order.addItem(item);
      }
      replicatedRegion.put("items_" + j, items);
    }

    queryData(QUERY_1, "", 0);

  }

  public Portfolio[] createPortfoliosAndPositions(int count) {
    Position.cnt = 0; // reset Portfolio counter
    Portfolio[] portfolios = new Portfolio[count];
    for (int i = 0; i < count; i++) {
      portfolios[i] = new Portfolio(i);
    }
    return portfolios;
  }

  @Test
  public void testCyclicArrayMultipleObjectsMemberWise() {

    Portfolio[] ports = createPortfoliosAndPositions(1);
    int i = 1;
    for (Portfolio p : ports) {
      replicatedRegion.put(new Integer(i), p);
      i++;
    }

    queryData(QUERY_1, cache.getDistributedSystem().getMemberId() + "," + cache.getDistributedSystem().getMemberId(), 0);

  }

  @Test
  public void testEmptyObject() {

    EmptyObject p = new EmptyObject();

    replicatedRegion.put("port", p);

    queryData(QUERY_1, "", 0);

  }

  @Test
  public void testSubClassOverridingMethods() {

    SubOrder so = new SubOrder();

    replicatedRegion.put("port", so);

    queryData(QUERY_1, "", 0);

  }
  
  @Test
  public void testNestedPDXObject() {

    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);

    pf.writeInt("ID", 111);
    pf.writeString("status", "active");
    pf.writeString("secId", "IBM");
    PdxInstance pi = pf.create();

    replicatedRegion.put("port", pi);
    queryData(QUERY_1, "", 0);

  }
  
  @Test
  public void testArrayWithNullValues() {

    SubOrder[] soArr = new SubOrder[2];
    soArr[0] = new SubOrder();
    soArr[1] = null;

    replicatedRegion.put("p1", soArr);
    queryData(QUERY_1, "", 0);

  }
  
  @Test
  public void testWithSqlDate() {

    SubOrder[] soArr = new SubOrder[2];
    soArr[0] = new SubOrder();
    soArr[1] = null;

    replicatedRegion.put("p1", soArr);
    queryData(QUERY_1, "", 0);

  }

}
