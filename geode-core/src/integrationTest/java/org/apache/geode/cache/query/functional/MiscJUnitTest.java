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
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import parReg.query.unittest.NewPortfolio;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.Data;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class MiscJUnitTest {

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Ignore("TODO: test is disabled")
  @Test
  public void testNestQueryInFromClause() throws Exception {
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    Query query = CacheUtils.getQueryService().newQuery(
        "SELECT DISTINCT * FROM (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios where status = 'active') p  where p.ID = 0");
    Collection result = (Collection) query.execute();
    Portfolio p = (Portfolio) (result.iterator().next());
    if (!p.status.equals("active") || p.getID() != 0) {
      fail(query.getQueryString());
    }
  }

  @Ignore("TODO: test is disabled")
  @Test
  public void testNestQueryInWhereClause() throws Exception {
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    Query query = CacheUtils.getQueryService().newQuery(
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios WHERE NOT (SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty");
    Collection result = (Collection) query.execute();
    Portfolio p = (Portfolio) (result.iterator().next());
    if (!p.positions.containsKey("IBM")) {
      fail(query.getQueryString());
    }
  }

  @Ignore("TODO: test is disabled")
  @Test
  public void testVoidMethods() throws Exception {
    Region region = CacheUtils.createRegion("Data", Data.class);
    region.put("0", new Data());
    Query query =
        CacheUtils.getQueryService()
            .newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Data where voidMethod");
    Collection result = (Collection) query.execute();
    if (result.size() != 0) {
      fail(query.getQueryString());
    }
    query = CacheUtils.getQueryService()
        .newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Data where voidMethod = null ");
    result = (Collection) query.execute();
    if (result.size() != 1) {
      fail(query.getQueryString());
    }
  }

  @Ignore("TODO: test is disabled")
  @Test
  public void testMiscQueries() throws Exception {
    String[] testData = {"NULL", "UNDEFINED"};
    for (final String testDatum : testData) {
      Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM " + testDatum);
      Object result = query.execute();
      if (!result.equals(QueryService.UNDEFINED)) {
        fail(query.getQueryString());
      }
    }
  }

  @Ignore("TODO: test is disabled")
  @Test
  public void testBug32763() throws Exception {
    Region region = CacheUtils.createRegion("pos", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    QueryService qs = CacheUtils.getQueryService();
    String qStr =
        "SELECT DISTINCT key: key, iD: entry.value.iD, secId: posnVal.secId  FROM " + SEPARATOR
            + "pos.entries entry, entry.value.positions.values posnVal  WHERE entry.value.\"type\" = 'type0' AND posnVal.secId = 'YHOO'";
    Query q = qs.newQuery(qStr);
    SelectResults result = (SelectResults) q.execute();
    StructType type = (StructType) result.getCollectionType().getElementType();
    String[] names = type.getFieldNames();
    List list = result.asList();
    if (list.size() < 1) {
      fail("Test failed as the resultset's size is zero");
    }
    for (Object o : list) {
      Struct stc = (Struct) o;
      if (!stc.get(names[2]).equals("YHOO")) {
        fail("Test failed as the SecID value is not YHOO");
      }
    }
  }

  @Test
  public void testBug() throws Exception {
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    QueryService qs = CacheUtils.getQueryService();

    String qStr =
        "Select distinct * from " + SEPARATOR
            + "portfolios pf, pf.positions.values where status = 'active' and secId = 'IBM'";
    qs.createIndex("index1", IndexType.FUNCTIONAL, "status", SEPARATOR + "portfolios pf");

    qs.createIndex("index4", IndexType.FUNCTIONAL, "itr",
        SEPARATOR + "portfolios pf, pf.collectionHolderMap chm, chm.value.arr itr");
    qs.createIndex("index2", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios pf, positions.values pos");
    qs.createIndex("index3", IndexType.FUNCTIONAL, "secId",
        SEPARATOR + "portfolios pf, positions.values pos");
    qs.createIndex("index5", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR
            + "portfolios pf, pf.collectionHolderMap chm, chm.value.arr, pf.positions.values pos");
    qs.createIndex("index6", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios pf, pf.collectionHolderMap chm");
    qs.createIndex("index7", IndexType.FUNCTIONAL, "itr",
        SEPARATOR
            + "portfolios pf, positions.values, pf.collectionHolderMap chm, chm.value.arr itr");
    Query q = qs.newQuery(qStr);
    SelectResults result = (SelectResults) q.execute();
    if (result.size() == 0) {
      fail("Test failed as size is zero");
    }
  }

  @Test
  public void testBug37723() throws Exception {
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    QueryService qs = CacheUtils.getQueryService();
    String qry =
        "select distinct getID, status from " + SEPARATOR
            + "portfolios pf where getID < 10 order by getID desc";
    Query q = qs.newQuery(qry);
    SelectResults result = (SelectResults) q.execute();
    Iterator itr = result.iterator();
    int j = 3;
    while (itr.hasNext()) {
      Struct struct = (Struct) itr.next();
      assertEquals(j--, ((Integer) struct.get("getID")).intValue());
    }
    qry = "select distinct getID, status from " + SEPARATOR
        + "portfolios pf where getID < 10 order by getID asc";
    q = qs.newQuery(qry);
    result = (SelectResults) q.execute();
    itr = result.iterator();
    j = 0;
    while (itr.hasNext()) {
      Struct struct = (Struct) itr.next();
      assertEquals(j++, ((Integer) struct.get("getID")).intValue());
    }
  }

  @Test
  public void testBug40428_1() throws Exception {
    HasShort shortData1 = new HasShort((short) 4);
    HasShort shortData2 = new HasShort((short) 5);
    Region region = CacheUtils.createRegion("shortFieldTest", Object.class);
    region.put("0", shortData1);
    QueryService qs = CacheUtils.getQueryService();
    String qry = "select * from " + SEPARATOR + "shortFieldTest sf where sf.shortField < 10 ";
    qs.createIndex("shortIndex", IndexType.FUNCTIONAL, "shortField", SEPARATOR + "shortFieldTest");
    region.put("1", shortData2);
    Query query = null;
    Object result = null;

    query = qs.newQuery(qry);

    SelectResults rs = (SelectResults) query.execute();
    assertEquals(rs.size(), 2);
  }

  @Test
  public void testBug40428_2() throws Exception {
    HasShort shortData1 = new HasShort((short) 4);
    HasShort shortData2 = new HasShort((short) 5);

    Region region = CacheUtils.createRegion("shortFieldTest", Object.class);
    region.put("0", shortData1);
    QueryService qs = CacheUtils.getQueryService();
    String qry =
        "select * from " + SEPARATOR + "shortFieldTest.entries sf where sf.value.shortField < 10 ";
    qs.createIndex("shortIndex", IndexType.FUNCTIONAL, "value.shortField",
        SEPARATOR + "shortFieldTest.entries");
    region.put("1", shortData2);
    Query query = null;
    Object result = null;

    query = qs.newQuery(qry);

    SelectResults rs = (SelectResults) query.execute();
    assertEquals(rs.size(), 2);
  }

  @Test
  public void testMultipleOrderByClauses() throws Exception {
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    region.put("6", new Portfolio(6));
    region.put("7", new Portfolio(7));
    QueryService qs = CacheUtils.getQueryService();
    String qry =
        "select distinct status, getID from " + SEPARATOR
            + "portfolios pf where getID < 10 order by status asc, getID desc";
    Query q = qs.newQuery(qry);
    SelectResults result = (SelectResults) q.execute();
    Iterator itr = result.iterator();
    int j = 6;
    while (itr.hasNext() && j > 0) {
      Struct struct = (Struct) itr.next();
      assertEquals("active", struct.get("status"));
      assertEquals(j, ((Integer) struct.get("getID")).intValue());

      j -= 2;
    }
    j = 7;
    while (itr.hasNext()) {
      Struct struct = (Struct) itr.next();
      assertEquals(j, ((Integer) struct.get("getID")).intValue());
      assertEquals("inactive", struct.get("status"));
      j -= 2;
    }
  }

  /**
   * Tests the where clause formed with CompiledComparison nesting
   */
  @Test
  public void testBug40333_InLocalRegion_1() throws Exception {
    CacheUtils.startCache();
    final Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes ra = attributesFactory.create();
    final Region region = cache.createRegion("new_pos", ra);
    String queryStr = " select distinct r.name, pVal, r.\"type\"  "
        + " from " + SEPARATOR + "new_pos r , r.positions.values pVal where "
        + "  (r.name='name_11' OR r.name='name_12') AND pVal.mktValue >=1.00";
    bug40333Simulation(region, queryStr);
  }

  /**
   * Commented the test as it is for some reason causing OOM when run in the suite. It is due to
   * presence of PR Tests the where clause formed with CompiledComparison nesting
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testBug40333_InPartitionedRegion_1() throws Exception {
    CacheUtils.startCache();
    final Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(10);
    PartitionAttributes pa = paf.create();
    attributesFactory.setPartitionAttributes(pa);
    RegionAttributes ra = attributesFactory.create();
    final Region region = cache.createRegion("new_pos", ra);
    String queryStr = " select distinct r.name, pVal, r.\"type\"  "
        + " from " + SEPARATOR + "new_pos r , r.positions.values pVal where "
        + "  (r.name='name_11' OR r.name='name_12') AND pVal.mktValue < 1.00";
    bug40333Simulation(region, queryStr);
  }

  /**
   * Tests the where clause formed with CompiledComparison nesting with CompiledIN
   */
  @Test
  public void testBug40333_InLocalRegion_2() throws Exception {
    CacheUtils.startCache();
    final Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes ra = attributesFactory.create();
    final Region region = cache.createRegion("new_pos", ra);
    String queryStr = " select distinct r.name, pVal, r.\"type\"  "
        + " from " + SEPARATOR + "new_pos r , r.positions.values pVal where "
        + " ( r.name IN Set('name_11' , 'name_12') OR false ) AND pVal.mktValue = 1.00";
    bug40333Simulation(region, queryStr);
  }

  /**
   * Commented the test as it is for some reason causing OOM when run in the suite. It is due to
   * presence of PR Tests the where clause formed with CompiledComparison nesting with CompiledIN
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testBug40333_InPartitionedRegion_2() throws Exception {
    CacheUtils.startCache();
    final Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(10);
    PartitionAttributes pa = paf.create();
    attributesFactory.setPartitionAttributes(pa);
    RegionAttributes ra = attributesFactory.create();
    final Region region = cache.createRegion("new_pos", ra);
    String queryStr = " select distinct r.name, pVal, r.\"type\"  "
        + " from " + SEPARATOR + "new_pos r , r.positions.values pVal where "
        + " ( r.name IN Set('name_11' , 'name_12') OR false ) AND pVal.mktValue < 1.00";
    bug40333Simulation(region, queryStr);
  }

  private void bug40333Simulation(final Region region, final String queryStr) throws Exception {
    final QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion(SEPARATOR + "new_pos");
    for (int i = 1; i < 100; ++i) {
      NewPortfolio pf = new NewPortfolio("name" + i, i);
      rgn.put("name" + i, pf);
    }
    final Object lock = new Object();
    final boolean[] expectionOccurred = new boolean[] {false};
    final boolean[] keepGoing = new boolean[] {true};

    Thread indexCreatorDestroyer = new Thread(() -> {
      boolean continueRunning = true;
      do {
        synchronized (lock) {
          continueRunning = keepGoing[0];
        }
        try {
          Index indx1 = qs.createIndex("MarketValues", IndexType.FUNCTIONAL, "itr2.mktValue",
              SEPARATOR + "new_pos itr1, itr1.positions.values itr2");
          Index indx2 =
              qs.createIndex("Name", IndexType.FUNCTIONAL, "itr1.name",
                  SEPARATOR + "new_pos itr1");
          Index indx3 =
              qs.createIndex("nameIndex", IndexType.PRIMARY_KEY, "name", SEPARATOR + "new_pos");
          Index indx4 =
              qs.createIndex("idIndex", IndexType.FUNCTIONAL, "id", SEPARATOR + "new_pos");
          Index indx5 = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
              SEPARATOR + "new_pos");
          Index indx6 = qs.createIndex("undefinedFieldIndex", IndexType.FUNCTIONAL,
              "undefinedTestField.toString", SEPARATOR + "new_pos");
          Thread.sleep(800);
          qs.removeIndex(indx1);
          qs.removeIndex(indx2);
          qs.removeIndex(indx3);
          qs.removeIndex(indx4);
          qs.removeIndex(indx5);
          qs.removeIndex(indx6);
        } catch (Throwable e) {
          region.getCache().getLogger().error(e);
          e.printStackTrace();
          synchronized (lock) {
            expectionOccurred[0] = true;
            keepGoing[0] = false;
            continueRunning = false;
          }

        }
      } while (continueRunning);
    });

    indexCreatorDestroyer.start();
    final Query q = qs.newQuery(queryStr);
    final int THREAD_COUNT = 10;
    Thread[] queryThreads = new Thread[THREAD_COUNT];
    final int numTimesToRun = 75;
    for (int i = 0; i < THREAD_COUNT; ++i) {
      queryThreads[i] = new Thread(() -> {
        boolean continueRunning = true;
        for (int i1 = 0; i1 < numTimesToRun && continueRunning; ++i1) {
          synchronized (lock) {
            continueRunning = keepGoing[0];
          }
          try {
            SelectResults sr = (SelectResults) q.execute();
          } catch (Throwable e) {
            e.printStackTrace();
            region.getCache().getLogger().error(e);
            synchronized (lock) {
              expectionOccurred[0] = true;
              keepGoing[0] = false;
              continueRunning = false;
            }
            break;
          }
        }
      });
    }
    synchronized (lock) {
      assertFalse(expectionOccurred[0]);
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
      queryThreads[i].start();
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
      queryThreads[i].join();
    }
    synchronized (lock) {
      keepGoing[0] = false;
    }

    indexCreatorDestroyer.join();
    synchronized (lock) {
      assertFalse(expectionOccurred[0]);
    }
  }

  @Test
  public void testBug40441() throws Exception {
    CacheUtils.startCache();
    final Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes ra = attributesFactory.create();
    final Region region = cache.createRegion("new_pos", ra);
    String queryStr1 = " select distinct r.name, pVal, r.\"type\"  "
        + " from " + SEPARATOR + "new_pos r , r.positions.values pVal where "
        + " ( r.undefinedTestField.toString = UNDEFINED  OR false ) ";// AND pVal.mktValue = 1.00";
    String queryStr2 = " select distinct r.name, pVal, r.\"type\"  "
        + " from " + SEPARATOR + "new_pos r , r.positions.values pVal where "
        + " ( r.undefinedTestField.toString = UNDEFINED  AND true ) AND pVal.mktValue = 1.00";
    final QueryService qs = CacheUtils.getQueryService();
    for (int i = 1; i < 100; ++i) {
      NewPortfolio pf = new NewPortfolio("name" + i, i);
      region.put("name" + i, pf);
    }

    Index indx1 = qs.createIndex("MarketValues", IndexType.FUNCTIONAL, "itr2.mktValue",
        SEPARATOR + "new_pos itr1, itr1.positions.values itr2");
    Index indx2 =
        qs.createIndex("Name", IndexType.FUNCTIONAL, "itr1.name", SEPARATOR + "new_pos itr1");
    Index indx3 = qs.createIndex("nameIndex", IndexType.PRIMARY_KEY, "name", SEPARATOR + "new_pos");
    Index indx4 = qs.createIndex("idIndex", IndexType.FUNCTIONAL, "id", SEPARATOR + "new_pos");
    Index indx5 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", SEPARATOR + "new_pos");
    Index indx6 = qs.createIndex("undefinedFieldIndex", IndexType.FUNCTIONAL,
        "undefinedTestField.toString", SEPARATOR + "new_pos");
    final Query q1 = qs.newQuery(queryStr1);
    final Query q2 = qs.newQuery(queryStr2);
    SelectResults sr1 = (SelectResults) q1.execute();
    SelectResults sr2 = (SelectResults) q2.execute();
  }

  @Test
  public void testBug37119() throws Exception {
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    region.put(Integer.MIN_VALUE + "", new Portfolio(Integer.MIN_VALUE));
    region.put("-1", new Portfolio(-1));
    QueryService qs = CacheUtils.getQueryService();

    String qStr = "Select distinct * from " + SEPARATOR + "portfolios pf where pf.getID() = "
        + Integer.MIN_VALUE;
    Query q = qs.newQuery(qStr);
    SelectResults result = (SelectResults) q.execute();
    assertEquals(result.size(), 1);
    Portfolio pf = (Portfolio) result.iterator().next();
    assertEquals(pf.getID(), Integer.MIN_VALUE);
    qStr = "Select distinct * from " + SEPARATOR + "portfolios pf where pf.getID() = -1";
    q = qs.newQuery(qStr);
    result = (SelectResults) q.execute();
    assertEquals(result.size(), 1);
    pf = (Portfolio) result.iterator().next();
    assertEquals(pf.getID(), -1);

    qStr = "Select distinct * from " + SEPARATOR
        + "portfolios pf where pf.getID() = 3 and pf.getLongMinValue() = "
        + Long.MIN_VALUE + 'l';
    q = qs.newQuery(qStr);
    result = (SelectResults) q.execute();
    assertEquals(result.size(), 1);
    pf = (Portfolio) result.iterator().next();
    assertEquals(pf.getID(), 3);

    qStr = "Select distinct * from " + SEPARATOR
        + "portfolios pf where pf.getID() = 3 and pf.getFloatMinValue() = "
        + Float.MIN_VALUE + 'f';
    q = qs.newQuery(qStr);
    result = (SelectResults) q.execute();
    assertEquals(result.size(), 1);
    pf = (Portfolio) result.iterator().next();
    assertEquals(pf.getID(), 3);

    qStr =
        "Select distinct * from " + SEPARATOR
            + "portfolios pf where pf.getID() = 3 and pf.getDoubleMinValue() = "
            + Double.MIN_VALUE;
    q = qs.newQuery(qStr);
    result = (SelectResults) q.execute();
    assertEquals(result.size(), 1);
    pf = (Portfolio) result.iterator().next();
    assertEquals(pf.getID(), 3);
  }

  private static class HasShort implements Serializable {
    public final short shortField;

    HasShort(short shortField) {
      this.shortField = shortField;
    }
  }
}
