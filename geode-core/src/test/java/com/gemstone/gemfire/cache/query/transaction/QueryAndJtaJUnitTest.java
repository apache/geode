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
 * QueryAndJtaJUnitTest.java
 *
 * Created on June 15, 2005, 12:49 PM
 */
package com.gemstone.gemfire.cache.query.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;

import javax.naming.Context;
import javax.transaction.RollbackException;
import javax.transaction.UserTransaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.jta.CacheUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class QueryAndJtaJUnitTest {

  private Region currRegion;
  private Cache cache;
  private QueryService qs;
  private Query q;
  private int tblIDFld;
  private String tblNameFld;
  private String tblName;

  @Before
  public void setUp() throws Exception {
    tblName = CacheUtils.init("CacheTest");
    cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = attributesFactory.create();
    currRegion = cache.createRegion("portfolios", regionAttributes);
    qs = CacheUtils.getQueryService();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.destroyTable(tblName);
    cache.close();
  }
  
  @Test
  public void testScenario1() throws Exception {
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    // Connection conn = null;
    try {
      qs.createIndex("iIndex", IndexType.FUNCTIONAL, "ID", "/portfolios");
      // PUT 4 objects in region, QUERY, CREATEINDEX
      for (int i = 0; i < 4; i++) {
        currRegion.put("key" + i, new Portfolio(i));
      }
      QueryObserverImpl observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs.newQuery("select distinct * from /portfolios where ID != 53");
      Object r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 4) {
        fail("Query result not of expected size");
      }
      // print("Size of query result :"+ ((Collection)r).size());
      // print("Result of query =" + Utils.printResult(r));

      // print("Index IS: " + ((RangeIndex)i2).dump());

      // BEGIN TX PUT new 4 objects in region, QUERY,CREATEINDEX, ROLLBACK
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
      ta.begin();
      for (int i = 9; i < 13; i++) {
        currRegion.put("key" + i, new Portfolio(i));
      }
      observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs.newQuery("select distinct * from /portfolios where ID != 53");
      r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 4) {
        fail("Query result not of expected size");
      }
      // print("Size of query result :"+ ((Collection)r).size());
      // print("Result of query =" + Utils.printResult(r));
      Index i1 = qs.createIndex("tIndex", IndexType.FUNCTIONAL, "status",
          "/portfolios");
      // print("Index IS: " + ((RangeIndex)i1).dump());
      observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs
          .newQuery("select distinct * from /portfolios where status = 'active'");
      r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 2) {
        fail("Query result not of expected size");
      }
      observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs
          .newQuery("select distinct * from /portfolios where status = 'inactive'");
      r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 2) {
        fail("Query result not of expected size");
      }
      ta.rollback();
      // PRINT INDEX AFTER ROLLBACK, REMOVEINDEX.
      observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs
          .newQuery("select distinct * from /portfolios where status = 'active'");
      r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 2) {
        fail("Query result not of expected size");
      }
      // print("AfterRollback \n"+currRegion.values());
      // print("Index IS: " + ((RangeIndex)i1).dump());
      qs.removeIndex(i1);
      // BEGIN TX PUT new 4 objects in region,CREATEINDEX, QUERY ,COMMIT
      ta.begin();
      for (int i = 9; i < 13; i++) {
        currRegion.put("key" + i, new Portfolio(i));
      }
      i1 = qs.createIndex("tIndex", IndexType.FUNCTIONAL, "status",
          "/portfolios");
      // print("Index IS: " + ((RangeIndex)i1).dump());
      observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs
          .newQuery("select distinct * from /portfolios where status = 'active'");
      r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 2) {
        fail("Query result not of expected size");
      }
      for (int i = 9; i < 13; i++) {
        currRegion.put("key" + i, new Portfolio(i));
      }
      observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs
          .newQuery("select distinct * from /portfolios where status = 'active'");
      r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 2) {
        fail("Query result not of expected size");
      }
      // print("Size of query result :"+ ((Collection)r).size());
      // print("Result of query =" + Utils.printResult(r));
      // print("Index on status IS: " + ((RangeIndex)i1).dump());
      // print("Index On ID IS: " + ((RangeIndex)i2).dump());
      ta.commit();
      // WAIT FOR 2 secs DISPLAYINDEX, QUERY

      Thread.sleep(2000);

      // print("Aftercommit \n"+currRegion.values());
      // print("Index IS: " + ((RangeIndex)i1).dump());
      observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs
          .newQuery("select distinct * from /portfolios where status = 'active'");
      r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 4) {
        fail("Query result not of expected size");
      }
      observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs
          .newQuery("select distinct * from /portfolios where status = 'inactive'");
      r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 4) {
        fail("Query result not of expected size");
      }
      observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      q = qs.newQuery("select distinct * from /portfolios where ID != 53");
      r = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("NO INDEX WAS USED, IT WAS EXPECTED TO BE USED");
      }
      if (((Collection) r).size() != 8) {
        fail("Query result not of expected size");
      }
      // print("Size of query result :"+ ((Collection)r).size());
      // print("Result of query =" + Utils.printResult(r));
      // print("Index On ID IS: " + ((RangeIndex)i2).dump());
    } catch (RollbackException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
      ta.rollback();
    }
  }

  private static void print(String str) {
    CacheUtils.log("\n" + str + "\n");
  }

  private static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }

  /**
   * verify that queries on indexes work with transaction
   * 
   * @throws Exception
   */
  @Test
  public void testIndexOnCommitForPut() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    Region region = cache.createRegion("sample", af.create());
    qs.createIndex("foo", IndexType.FUNCTIONAL, "age", "/sample");
    Context ctx = cache.getJNDIContext();
    UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
    Integer x = new Integer(0);
    utx.begin();
    region.create(x, new Person("xyz", 45));
    utx.commit();
    Query q = qs.newQuery("select * from /sample where age < 50");
    assertEquals(1, ((SelectResults) q.execute()).size());
    Person dsample = (Person) CopyHelper.copy(region.get(x));
    dsample.setAge(55);
    utx.begin();
    region.put(x, dsample);
    utx.commit();
    CacheUtils.log(((Person) region.get(x)));
    assertEquals(0, ((SelectResults) q.execute()).size());
  }

  @Test
  public void testIndexOnCommitForInvalidate() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    Region region = cache.createRegion("sample", af.create());
    qs.createIndex("foo", IndexType.FUNCTIONAL, "age", "/sample");
    Context ctx = cache.getJNDIContext();
    UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
    Integer x = new Integer(0);
    utx.begin();
    region.create(x, new Person("xyz", 45));
    utx.commit();
    Query q = qs.newQuery("select * from /sample where age < 50");
    assertEquals(1, ((SelectResults) q.execute()).size());
    Person dsample = (Person) CopyHelper.copy(region.get(x));
    dsample.setAge(55);
    utx.begin();
    region.invalidate(x);
    utx.commit();
    CacheUtils.log(((Person) region.get(x)));
    assertEquals(0, ((SelectResults) q.execute()).size());
  }

  @Test
  public void testAllIndexesOnCommitForPut() throws Exception {
    // create region
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    Region region = cache.createRegion("sample", af.create());

    // put data
    for (int i = 0; i < 10; i++) {
      region.put(i, new Portfolio(i));
    }

    String[] queries = {
        "select * from /sample where ID = 5",
        "select ID from /sample where ID < 5",
        "select ID from /sample where ID > 5",
        "select ID from /sample where ID != 5",
        "select status from /sample where status = 'active'",
        "select status from /sample where status > 'active'",
        "select status from /sample where status < 'active'",
        "select status from /sample where status != 'active'",
        "select pos.secId from /sample p, p.positions.values pos where pos.secId = 'IBM'",
        "select pos.secId from /sample p, p.positions.values pos where pos.secId < 'VMW'",
        "select pos.secId from /sample p, p.positions.values pos where pos.secId > 'IBM'",
        "select pos.secId from /sample p, p.positions.values pos where pos.secId != 'IBM'" };

    SelectResults[][] sr = new SelectResults[queries.length][2];

    // execute queries without indexes
    for (int i = 0; i < queries.length; i++) {
      sr[i][0] = (SelectResults) qs.newQuery(queries[i]).execute();
    }

    // create indexes
    qs.createKeyIndex("IDIndex", "ID", "/sample");
    qs.createIndex("statusIndex", "status", "/sample");
    qs.createIndex("secIdIndex", "pos.secId",
        "/sample p, p.positions.values pos");

    // begin transaction
    Context ctx = cache.getJNDIContext();
    UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
    utx.begin();

    // update data
    for (int i = 0; i < 10; i++) {
      region.put(i, new Portfolio(i));
    }

    // execute queries with indexes during transaction
    for (int i = 0; i < queries.length; i++) {
      sr[i][1] = (SelectResults) qs.newQuery(queries[i]).execute();
    }

    // complete transaction
    utx.commit();

    // verify results
    com.gemstone.gemfire.cache.query.CacheUtils
        .compareResultsOfWithAndWithoutIndex(sr);
  }

  @Test
  public void testIndexOnCommitForDestroy() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    Region region = cache.createRegion("sample", af.create());
    qs.createIndex("foo", IndexType.FUNCTIONAL, "age", "/sample");
    Context ctx = cache.getJNDIContext();
    UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
    Integer x = new Integer(0);
    utx.begin();
    region.create(x, new Person("xyz", 45));
    utx.commit();
    Query q = qs.newQuery("select * from /sample where age < 50");
    assertEquals(1, ((SelectResults) q.execute()).size());
    Person dsample = (Person) CopyHelper.copy(region.get(x));
    dsample.setAge(55);
    utx.begin();
    region.destroy(x);
    utx.commit();
    CacheUtils.log(((Person) region.get(x)));
    assertEquals(0, ((SelectResults) q.execute()).size());
  }

  /*
   * Enable this test when indexes are made transactional.
   */
  //@Test
  public void _testFailedIndexUpdateOnJTACommitForPut() throws Exception {
    Person.THROW_ON_INDEX = true;
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    Region region = cache.createRegion("sample", af.create());
    qs.createIndex("foo", IndexType.FUNCTIONAL, "index", "/sample");
    Context ctx = cache.getJNDIContext();
    UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
    Integer x = new Integer(0);
    utx.begin();
    region.create(x, new Person("xyz", 45));
    try {
      utx.commit();
      fail("Commit should have thrown an exception because the index update threw");
    } catch (Exception e) {
      // this is desired
    }
  }

  @Test
  public void testFailedIndexUpdateOnCommitForPut() throws Exception {
    Person.THROW_ON_INDEX = true;
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    SimpleListener sl = new SimpleListener();
    af.setCacheListener(sl);
    Region region = cache.createRegion("sample", af.create());
    qs.createIndex("foo", IndexType.FUNCTIONAL, "index", "/sample");
    Context ctx = cache.getJNDIContext();

    Integer x = new Integer(0);
    region.getCache().getCacheTransactionManager().begin();
    region.create(x, new Person("xyz", 45));
    try {
      region.getCache().getCacheTransactionManager().commit();
      fail("commit should have thrown an exception because the index maintenance threw");
    } catch (com.gemstone.gemfire.cache.query.IndexMaintenanceException ie) {
      // this is the desired case
    }
    Person p = (Person) region.get(x);
    assertEquals("object shouldn't have made it into region", null, p);
    assertEquals(0, sl.creates);
    assertEquals(0, sl.updates);
  }

  private static class SimpleListener extends CacheListenerAdapter {

    public int creates = 0;
    public int updates = 0;

    public void afterCreate(EntryEvent event) {
      // TODO Auto-generated method stub
      CacheUtils.log("SimpleListener.create!:" + event);
      creates++;
    }

    public void afterUpdate(EntryEvent event) {
      // TODO Auto-generated method stub
      updates++;
    }

  }
}
