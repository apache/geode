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
/*
 * InOperatorTest.java
 *
 * Created on March 24, 2005, 5:08 PM
 */
package org.apache.geode.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Utils;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class INOperatorJUnitTest {
  
  
  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }
  
  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
  // For decomposition of IN expressions, need to unit-test the following:
  // 1) IN expr gets decomposed on indexed expression with func index (done)
  // 2) index gets used on IN clause (done)
  // 3) IN expr does not get decomposed on unindexed expression (not done)
  // 4) Decomposed IN expr works with bind parameters (not done)
  // 5) IN expr does get decomposed on indexed expression with pk index (not done)
  // 6) pk index gets used with IN expression (not done)
  // 7) decomposition (or not) of nested IN expressions (not done)
  // 8) test IN clauses with:
  //   a) zero elements (should shortcircuit to return false ideally) (not done)
  //   b) one element (not done)
  //   c) more than one element (done) 
   
  /**
   * Test the decomposition of IN SET(..) that gets decomposed
   * into ORs so an index can be used
   */
  @Ignore
  @Test
  public void testInDecompositionWithFunctionalIndex() throws Exception {
 
  }
  
  @Test
  public void testRegionBulkGet() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();
    
    Region region = cache.createRegion("pos",regionAttributes);
    
    region.put("6", new Integer(6));
    region.put("10", new Integer(10));
    region.put("12", new Integer(10));
    
    QueryService qs = cache.getQueryService();
    
    Query q;
    SelectResults results;
    Object[] keys;
    Set expectedResults;
    
    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
    keys = new Object[] { "5", "6", "10", "45" };
    results = (SelectResults)q.execute(new Object[] {keys});
    expectedResults = new HashSet();
    expectedResults.add(new Integer(6));
    expectedResults.add(new Integer(10));
    assertEquals(expectedResults, results.asSet());
    
    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
    keys = new Object[] { "42" };
    results = (SelectResults)q.execute(new Object[] {keys});
    expectedResults = new HashSet();
    assertEquals(expectedResults, results.asSet());    
    
    for (int i = 0; i < 1000; i++) {
      region.put(String.valueOf(i), new Integer(i));
    }
    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
    keys = new Object[] { "5", "6", "10", "45" };
    results = (SelectResults)q.execute(new Object[] {keys});
    expectedResults = new HashSet();
    expectedResults.add(new Integer(5));
    expectedResults.add(new Integer(6));
    expectedResults.add(new Integer(10));
    expectedResults.add(new Integer(45));
    assertEquals(expectedResults, results.asSet());
    
    q = qs.newQuery("SELECT e.key, e.value FROM /pos.entrySet e WHERE e.key IN $1");
    keys = new Object[] { "5", "6", "10", "45" };
    results = (SelectResults)q.execute(new Object[] {keys});
    assertEquals(4, results.size());    

    region.destroyRegion();
  }
  
  
  @Test
  public void testIntSet() throws Exception {
    
    Query q = CacheUtils.getQueryService().newQuery("2 IN SET(1,2,3)");
    
    Object result = q.execute();
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for IN operator");
  }
  @Test
  public void testStringSet() throws Exception {
    
    Query q = CacheUtils.getQueryService().newQuery("'a' IN SET('x','y','z')");
    
    Object result = q.execute();
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(Boolean.FALSE))
      fail("Failed for StringSet with IN operator");
  }

  @Test
  public void testShortNumSet() throws Exception {
    Short num = Short.valueOf("1");
    Object params[]=new Object[1];
    params[0]= num;
    
    Query q = CacheUtils.getQueryService().newQuery("$1 IN SET(1,2,3)");
    
    Object result = q.execute(params);
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for ShortNum with IN operator");
  }
 
  @Test
  public void testCollection() throws Exception {
    Object e1 = new Object();
    Object e2 = new Object();
    Object e3 = new Object();
    HashSet C1 = new HashSet();
    C1.add(e1);
    C1.add(e2);
    C1.add(e3);
    Object params[]=new Object[3];
    params[0]= e1;
    params[1]= C1;
    params[2]= e2;
    
    Query q = CacheUtils.getQueryService().newQuery("$3 IN $2");
    Object result = q.execute(params);
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for Collection with IN operator");
  }
  
  @Test
  public void testWithSet() throws Exception {
    String s1 = "Hello";
    String s2 = "World";
    HashSet H1 = new HashSet();
    H1.add(s1);
    H1.add(s2);
    Object params[]=new Object[2];
    params[0]= s1;
    params[1]= H1;
    Query q = CacheUtils.getQueryService().newQuery("$1 IN $2");
    Object result = q.execute(params);
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for String set with IN operator");
  }
  @Test
  public void testArrayList() throws Exception {
    String s1 = "sss";
    String s2 = "ddd";
    ArrayList AL1 = new ArrayList();
    AL1.add(s1);
    AL1.add(s2);
    Object params[]=new Object[3];
    params[0]= s1;
    params[1]= s2;
    params[2]= AL1;
    Query q = CacheUtils.getQueryService().newQuery("$1 IN $3");
    Object result = q.execute(params);
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for ArrayList with IN operator");
  }
  
  @Test
  public void testNULL() throws Exception {
    Query q = CacheUtils.getQueryService().newQuery(" null IN SET('x','y','z')");
    Object result = q.execute();
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(Boolean.FALSE))
      fail("Failed for NULL in IN operator Test");
    
    q = CacheUtils.getQueryService().newQuery(" null IN SET(null)");
    result = q.execute();
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for NULL in IN operator Test");
    
  }
  
  @Test
  public void testUNDEFINED() throws Exception {
    Query q = CacheUtils.getQueryService().newQuery(" UNDEFINED IN SET(1,2,3)");
    Object result = q.execute();
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(Boolean.FALSE))
      fail("Failed for UNDEFINED with IN operator");
    
    q = CacheUtils.getQueryService().newQuery(" UNDEFINED IN SET(UNDEFINED)");
    result = q.execute();
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(QueryService.UNDEFINED))
      fail("Failed for UNDEFINED with IN operator");
    
    q = CacheUtils.getQueryService().newQuery(" UNDEFINED IN SET(UNDEFINED,UNDEFINED)");
    result = q.execute();
    CacheUtils.log(Utils.printResult(result));
    if(!result.equals(QueryService.UNDEFINED))
      fail("Failed for UNDEFINED with IN operator");
  }
  
  @Test
  public void testMiscSet() throws Exception {
    Query q = CacheUtils.getQueryService().newQuery(" $1 IN SET(1, 'a', $2, $3, $4, $5)");
    Object params[] = {null, new Integer(0), "str", null, new Object()};
    
    for(int i=1;i<params.length;i++){
      params[0] = params[i];
      Object result = q.execute(params);
      CacheUtils.log(Utils.printResult(result));
      if(!result.equals(Boolean.TRUE))
        fail("Failed for Mix set with IN operator");
    }
    
  }
  
  @Test
  public void testIndexUsageWithIn() throws Exception {
	    Cache cache = CacheUtils.getCache();
	    AttributesFactory attributesFactory = new AttributesFactory();
	    RegionAttributes regionAttributes = attributesFactory.create();
	    
	    Region region = cache.createRegion("pos",regionAttributes);
	    
	    region.put("6", new Integer(6));
	    region.put("10", new Integer(10));
	    region.put("12", new Integer(10));
	    
	    QueryService qs = cache.getQueryService();
	    qs.createIndex("In Index", IndexType.FUNCTIONAL, "e.key","/pos.entrySet e");
	    Query q;
	    SelectResults results;
	    Object[] keys;
	    Set expectedResults;
	    
	    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
	    keys = new Object[] { "5", "6", "10", "45" };
	    results = (SelectResults)q.execute(new Object[] {keys});
	    expectedResults = new HashSet();
	    expectedResults.add(new Integer(6));
	    expectedResults.add(new Integer(10));
	    assertEquals(expectedResults, results.asSet());
	    
	    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
	    keys = new Object[] { "42" };
	    results = (SelectResults)q.execute(new Object[] {keys});
	    expectedResults = new HashSet();
	    assertEquals(expectedResults, results.asSet());    
	    
	    for (int i = 0; i < 1000; i++) {
	      region.put(String.valueOf(i), new Integer(i));
	    }
	    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
	    keys = new Object[] { "5", "6", "10", "45" };
	    results = (SelectResults)q.execute(new Object[] {keys});
	    expectedResults = new HashSet();
	    expectedResults.add(new Integer(5));
	    expectedResults.add(new Integer(6));
	    expectedResults.add(new Integer(10));
	    expectedResults.add(new Integer(45));
	    assertEquals(expectedResults, results.asSet());
	    
	    q = qs.newQuery("SELECT e.key, e.value FROM /pos.entrySet e WHERE e.key IN $1");
	    keys = new Object[] { "5", "6", "10", "45" };
	    results = (SelectResults)q.execute(new Object[] {keys});
	    assertEquals(4, results.size());    
	  }
	  
  
  /**
   * Tests optimization of compiled in where we no longer evaluate on every iteration
   * The set is saved off into the query context and reused
   * Each query should have it's own query context
   * @throws Exception
   */
  @Test
  public void testCacheEvalCollnWithIn() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();
    QueryService qs = cache.getQueryService();
    Region customersRegion = cache.createRegionFactory(regionAttributes).create("customers");
    Region receiptsRegion = cache.createRegionFactory(regionAttributes).create("receipts");
    
    qs.createIndex("receiptsByProduct", "i.productId", "/receipts r, r.items i");
    qs.createIndex("receiptsByCustomer", "r.productId", "/receipts r");
    qs.createIndex("customersByProfile", "c.profile", "/customers c");    
   
    int numReceiptsPerCustomer = 10;
    for (int i = 0; i < 1000; i++) {
      customersRegion.put(i, new Customer(i, i % 2 == 0 ? "PremiumIndividual": "AverageJoe"));
      for (int j = 0; j < numReceiptsPerCustomer; j++) {
        int receiptId = i * numReceiptsPerCustomer + j;
        receiptsRegion.put(receiptId, new Receipt(receiptId, i));
      }
    }
   
    Query q = qs.newQuery("<trace>select r from /receipts r, r.items i where i.productId = 8 and r.customerId in (select c.id from /customers c where c.profile = 'PremiumIndividual')");
    SelectResults results = (SelectResults) q.execute();
    assertEquals("Not the same size", 500, results.size());
    
    for (int i = 1000; i < 1100; i++) {
      customersRegion.put(i, new Customer(i, i % 2 == 0 ? "PremiumIndividual": "AverageJoe"));
      for (int j = 0; j < numReceiptsPerCustomer; j++) {
        int receiptId = i * numReceiptsPerCustomer + j;
        receiptsRegion.put(receiptId, new Receipt(receiptId, i));
      }
    }
    results = (SelectResults)q.execute();
    assertEquals("Not the same size after new inserts", 550, results.size() );
  }
  
  /**
   * Tests optimization of compiled in where we no longer evaluate on every iteration
   * The set is saved off into the query context and reused
   * Each query should have it's own query context
   * @throws Exception
   */
  @Test
  public void testCacheEvalCollnWithInWithMultipleNestedIn() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();
    QueryService qs = cache.getQueryService();
    Region customersRegion = cache.createRegionFactory(regionAttributes).create("customers");
    Region receiptsRegion = cache.createRegionFactory(regionAttributes).create("receipts");
    
    qs.createIndex("receiptsByProduct", "i.productId", "/receipts r, r.items i");
    qs.createIndex("receiptsByCustomer", "r.productId", "/receipts r");
    qs.createIndex("customersByProfile", "c.profile", "/customers c");    
   
    int numReceiptsPerCustomer = 10;
    for (int i = 0; i < 1000; i++) {
      customersRegion.put(i, new Customer(i, i % 2 == 0 ? "PremiumIndividual": "AverageJoe"));
      for (int j = 0; j < numReceiptsPerCustomer; j++) {
        int receiptId = i * numReceiptsPerCustomer + j;
        receiptsRegion.put(receiptId, new Receipt(receiptId, i));
      }
    }
   
    Query q = qs.newQuery("<trace>select r from /receipts r, r.items i where i.productId = 8 and r.customerId in (select c.id from /customers c where c.id in (select d.id from /customers d where d.profile='PremiumIndividual'))");
    SelectResults results = (SelectResults) q.execute();
    assertEquals("Not the same size", 500, results.size());
    
    for (int i = 1000; i < 1100; i++) {
      customersRegion.put(i, new Customer(i, i % 2 == 0 ? "PremiumIndividual": "AverageJoe"));
      for (int j = 0; j < numReceiptsPerCustomer; j++) {
        int receiptId = i * numReceiptsPerCustomer + j;
        receiptsRegion.put(receiptId, new Receipt(receiptId, i));
      }
    }
    results = (SelectResults)q.execute();
    assertEquals("Not the same size after new inserts", 550, results.size() );
  }
  
  public class Customer {
    public int id;
    public String profile;
    
    public Customer(int id, String profile) {
      this.id = id;
      this.profile = profile;
    }
    
    public int getId() {
      return id;
    }
    
    public String getProfile() {
      return profile;
    }
  }
  
  public class Receipt {
    public int customerId;
    public Item[] items;
    
    public Receipt(int receiptId, int customerId) {
      this.customerId = customerId;
      items = new Item[] { new Item(receiptId % 10)};
    }
    
    public int getCustomerId() {
      return customerId;
    }
    
    public Item[] getItems() {
      return items;
    }
  }
  
  public class Item {
    public int productId;
    
    public Item(int productId) {
      this.productId = productId;
    }
    
    public int getProductId() {
      return productId;
    }
  }
}
