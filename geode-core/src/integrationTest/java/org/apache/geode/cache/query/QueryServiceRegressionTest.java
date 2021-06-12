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
package org.apache.geode.cache.query;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.query.data.TestData.createAndPopulateSet;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.data.TestData.MyValue;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.QueryUtils;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * Verifies bug fixes against QueryService
 */
@Category({OQLQueryTest.class})
public class QueryServiceRegressionTest {

  private Region region;
  private Region region1;
  private QueryService qs;
  private Cache cache;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    // leave the region untyped
    // attributesFactory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = attributesFactory.create();

    region = cache.createRegion("pos", regionAttributes);
    region1 = cache.createRegion("pos1", regionAttributes);
    for (int i = 0; i < 4; i++) {
      Portfolio p = new Portfolio(i);
      region.put("" + i, p);
      region1.put("" + i, p);
    }
    qs = cache.getQueryService();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  /**
   * TRAC #41509: ClassCastException running an OQL query
   */
  @Test
  public void oqlQueryShouldNotThrowClassCastException() throws Exception {
    // Create Index.
    try {
      this.qs.createIndex("pos1_secIdIndex", IndexType.FUNCTIONAL, "p1.position1.secId",
          SEPARATOR + "pos1 p1");
      this.qs.createIndex("pos1_IdIndex", IndexType.FUNCTIONAL, "p1.position1.Id",
          SEPARATOR + "pos1 p1");
      this.qs.createIndex("pos_IdIndex", IndexType.FUNCTIONAL, "p.position1.Id",
          SEPARATOR + "pos p");
    } catch (Exception ex) {
      fail("Failed to create Index. " + ex);
    }
    // Execute Query.
    try {
      String queryStr =
          "select distinct * from " + SEPARATOR + "pos p, " + SEPARATOR + "pos1 p1 where "
              + "p.position1.Id = p1.position1.Id and p1.position1.secId in set('MSFT')";
      Query q = qs.newQuery(queryStr);
      CacheUtils.getLogger().fine("Executing:" + queryStr);
      q.execute();
    } catch (Exception ex) {
      fail("Query should have executed successfully. " + ex);
    }
  }

  /**
   * TRAC #32429: An issue with nested queries :Iterator for the region in inner query is not
   * getting resolved.
   */
  @Test
  public void iteratingNestedQueriesShouldWork() throws Exception {
    String[] queries = new String[] {
        "SELECT DISTINCT * FROM " + SEPARATOR + "pos where NOT(SELECT DISTINCT * FROM " + SEPARATOR
            + "pos p where p.ID = 0).isEmpty",
        "-- AMBIGUOUS\n" + "import org.apache.geode.cache.\"query\".data.Portfolio; "
            + "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos TYPE Portfolio where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "pos p TYPE Portfolio where ID = 0).status",
        "SELECT DISTINCT * FROM " + SEPARATOR + "pos where status = ELEMENT(SELECT DISTINCT * FROM "
            + SEPARATOR + "pos p where p.ID = 0).status",
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "pos p where p.ID = x.ID).status",
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "pos p where p.ID = 0).status",};

    for (int i = 0; i < queries.length; i++) {
      Object r = null;
      String queryStr = queries[i];
      Query q = qs.newQuery(queryStr);
      CacheUtils.getLogger().fine("Executing:" + queryStr);
      try {
        r = q.execute();
        if (i == 1) {
          fail("should have thrown an AmbiguousNameException");
        }
      } catch (AmbiguousNameException e) {
        if (i != 1) {
          throw e; // if it's 1 then pass
        }
      }
      if (r != null) {
        CacheUtils.getLogger().fine(Utils.printResult(r));
      }
    }
  }

  /**
   * TRAC #32375: Issue with Nested Query
   */
  @Test
  public void nestedQueriesShouldNotThrowTypeMismatchException() throws Exception {
    String queryStr;
    Query q;
    Object r;

    queryStr = "import org.apache.geode.cache.\"query\".data.Portfolio; "
        + "select distinct * from " + SEPARATOR + "pos, (select distinct * from " + SEPARATOR
        + "pos p TYPE Portfolio, p.positions where value!=null)";
    q = qs.newQuery(queryStr);
    assertThatCode(() -> q.execute()).doesNotThrowAnyException();
  }

  /**
   * Confirms that #32251 still exists (it was never fixed).
   *
   * <p>
   * TRAC #32251: Error: The attribute or method name 'value' could not be resolved
   *
   * <p>
   * The problem is that the query processor currently is limited on what kind of "dependent"
   * iterator expressions it is able to determine the type of. It is only able to determine the
   * type of a path expression that is dependent on a previous iterator.
   *
   * <p>
   * The reason why this is limited is that it doesn't fully evaluate the expression to determine
   * its element type, since this is done before the select statement itself is evaluated.
   *
   * <p>
   * This is exactly the same issue that we have with the inability to determine which untyped
   * iterator an implicit attribute name goes to.
   *
   * <p>
   * The workaround is to either type the iterator or use an explicit iterator variable.
   *
   * <p>
   * The following query passes because the query processor is smart enough to determine the type
   * of a simple path iterator:
   *
   * <pre>
   * Select distinct value.secId from /pos , positions
   * </pre>
   *
   * <p>
   * The following query, however, fails because the query processor is not smart enough yet to
   * fully type expressions:
   *
   * <pre>
   * Select distinct value.secId from /pos , getPositions(23)
   * </pre>
   *
   * <p>
   * The following queries, however, succeed because the iterator is either explicitly named with
   * a variable or it is typed:
   *
   * <pre>
   * Select distinct e.value.secId from /pos , getPositions(23) e
   *
   * import com.gemstone.gemfire.cache.$1"query$1".data.Position;
   * select distinct value.secId
   * from /pos, (map<string, Position>)getPositions(23)
   *
   * import java.util.Map$Entry as Entry;
   * select distinct value.secId from /pos, getPositions(23) type Entry
   * </pre>
   */
  @Test
  public void dependentIteratorExpressionsAreLimited() throws Exception {
    String queryStr;
    Query q;
    Object r;

    // partial fix for Bug 32251 was checked in so that if there is only
    // one untyped iterator, we assume names will associate with it.

    // the following used to fail due to inability to determine type of a dependent
    // iterator def, but now succeeds because there is only one untyped iterator
    queryStr = "Select distinct ID from " + SEPARATOR + "pos";
    q = qs.newQuery(queryStr);
    r = q.execute();
    Set expectedSet = createAndPopulateSet(4);
    assertEquals(expectedSet, ((SelectResults) r).asSet());

    // the following queries still fail because there is more than one
    // untyped iterator:
    queryStr = "Select distinct value.secId from " + SEPARATOR + "pos , positions";
    q = qs.newQuery(queryStr);
    try {
      r = q.execute();
      fail("Expected a TypeMismatchException due to bug 32251");
      CacheUtils.getLogger().fine(queryStr);
      CacheUtils.getLogger().fine(Utils.printResult(r));
    } catch (TypeMismatchException e) {
      // expected due to bug 32251
    }

    queryStr = "Select distinct value.secId from " + SEPARATOR + "pos , getPositions(23)";
    q = qs.newQuery(queryStr);
    try {
      r = q.execute();
      fail("Expected a TypeMismatchException due to bug 32251");
      CacheUtils.getLogger().fine(queryStr);
      CacheUtils.getLogger().fine(Utils.printResult(r));
    } catch (TypeMismatchException e) {
      // expected due to bug 32251
    }

    queryStr = "Select distinct value.secId from " + SEPARATOR + "pos , getPositions($1)";
    q = qs.newQuery(queryStr);
    try {
      r = q.execute(new Object[] {new Integer(23)});
      fail("Expected a TypeMismatchException due to bug 32251");
      CacheUtils.getLogger().fine(queryStr);
      CacheUtils.getLogger().fine(Utils.printResult(r));
    } catch (TypeMismatchException e) {
      // expected due to bug 32251
    }

    // the following queries, however, should work:
    queryStr = "Select distinct e.value.secId from " + SEPARATOR + "pos, getPositions(23) e";
    q = qs.newQuery(queryStr);
    r = q.execute();
    CacheUtils.getLogger().fine(queryStr);
    CacheUtils.getLogger().fine(Utils.printResult(r));

    queryStr = "import org.apache.geode.cache.\"query\".data.Position;"
        + "select distinct value.secId from " + SEPARATOR
        + "pos, (map<string, Position>)getPositions(23)";
    q = qs.newQuery(queryStr);
    r = q.execute();
    CacheUtils.getLogger().fine(queryStr);
    CacheUtils.getLogger().fine(Utils.printResult(r));

    queryStr = "import java.util.Map$Entry as Entry;"
        + "select distinct value.secId from " + SEPARATOR + "pos, getPositions(23) type Entry";
    q = qs.newQuery(queryStr);
    r = q.execute();
    CacheUtils.getLogger().fine(queryStr);
    CacheUtils.getLogger().fine(Utils.printResult(r));

  }

  /**
   * TRAC #32624: NameNotFoundException occurs while index maintenance.
   */
  @Test
  public void indexMaintenanceShouldNotThrowNameNotFoundException() throws Exception {
    this.qs.createIndex("iIndex", IndexType.FUNCTIONAL, "e.value.status",
        SEPARATOR + "pos.entries e");
    this.region.put("0", new Portfolio(0));
  }

  /**
   * This bug was occurring in simulation of Outer Join query
   */
  @Test
  public void testBugResultMismatch() throws Exception {
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    QueryService qs = CacheUtils.getQueryService();
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    qs.createIndex("index1", IndexType.FUNCTIONAL, "status", SEPARATOR + "portfolios pf");
    String query1 =
        "SELECT   DISTINCT iD as portfolio_id, pos.secId as sec_id from " + SEPARATOR
            + "portfolios p , p.positions.values pos  where p.status= 'active'";
    String query2 = "select  DISTINCT * from  "
        + "( SELECT   DISTINCT iD as portfolio_id, pos.secId as sec_id from " + SEPARATOR
        + "portfolios p , p.positions.values pos where p.status= 'active')";

    Query q1 = CacheUtils.getQueryService().newQuery(query1);
    Query q2 = CacheUtils.getQueryService().newQuery(query2);
    SelectResults rs1 = (SelectResults) q1.execute();
    SelectResults rs2 = (SelectResults) q2.execute();

    assertThatCode(() -> QueryUtils.union(rs1, rs2, null)).doesNotThrowAnyException();
  }

  /**
   * TRAC #36659: Nested query failure due to incorrect scope resolution
   */
  @Test
  public void multipleScopesShouldBeAllowedAtSameLevelOfNesting() throws Exception {
    // Task ID: NQIU 9
    CacheUtils.getQueryService();
    String queries =
        "select distinct p.x from (select distinct x, pos from " + SEPARATOR
            + "pos x, x.positions.values pos) p, (select distinct * from " + SEPARATOR + "pos"
            + SEPARATOR + "positions rtPos where rtPos.secId = p.pos.secId)";
    Region r = CacheUtils.getRegion(SEPARATOR + "pos");
    Region r1 = r.createSubregion("positions", new AttributesFactory().createRegionAttributes());


    r1.put("1", new Position("SUN", 2.272));
    r1.put("2", new Position("IBM", 2.272));
    r1.put("3", new Position("YHOO", 2.272));
    r1.put("4", new Position("GOOG", 2.272));
    r1.put("5", new Position("MSFT", 2.272));
    Query q = CacheUtils.getQueryService().newQuery(queries);
    CacheUtils.getLogger().info("Executing query: " + queries);
    SelectResults rs = (SelectResults) q.execute();
    assertTrue("Resultset size should be > 0", rs.size() > 0);
  }

  /**
   * Tests the Bug 38422 where the index results intersection results in incorrect size
   *
   * <p>
   * TRAC #38422: Non distinct select query may return a ResultsBag with incorrect size depending on
   * the where clause
   */
  @Test
  public void testBug38422() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion(SEPARATOR + "pos");
    // The region already contains 3 Portfolio object. The 4th Portfolio object
    // has its status field explictly made null. Thus the query below will result
    // in intersection of two non empty sets. One which contains 4th Portfolio
    // object containing null status & the second condition does not contain the
    // 4th portfolio object
    Portfolio pf = new Portfolio(4);
    pf.status = null;
    rgn.put(new Integer(4), pf);
    String queryStr =
        "select  * from " + SEPARATOR + "pos pf where pf.status != 'active' and pf.status != null";

    SelectResults r[][] = new SelectResults[1][2];
    Query qry = qs.newQuery(queryStr);
    SelectResults sr = null;
    sr = (SelectResults) qry.execute();
    r[0][0] = sr;
    qs.createIndex("statusIndx", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "pos pf");
    sr = null;
    sr = (SelectResults) qry.execute();
    r[0][1] = sr;
    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);
    assertEquals(2, (r[0][0]).size());
    assertEquals(2, (r[0][1]).size());
  }

  @Test
  public void testEquijoinPRColocatedQuery_1() throws Exception {

    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(1)
        .setTotalNumBuckets(40).setPartitionResolver(new PartitionResolver() {

          @Override
          public String getName() {
            return "blah";
          }

          @Override
          public Serializable getRoutingObject(EntryOperation opDetails) {
            return (Serializable) opDetails.getKey();
          }

          @Override
          public void close() {

        }

        }).create());
    PartitionedRegion pr1 =
        (PartitionedRegion) CacheUtils.getCache().createRegion("pr1", factory.create());
    factory = new AttributesFactory();
    factory.setPartitionAttributes(new PartitionAttributesFactory()

        .setRedundantCopies(1).setTotalNumBuckets(40).setPartitionResolver(new PartitionResolver() {

          @Override
          public String getName() {
            return "blah";
          }

          @Override
          public Serializable getRoutingObject(EntryOperation opDetails) {
            return (Serializable) opDetails.getKey();
          }

          @Override
          public void close() {

        }

        }).setColocatedWith(pr1.getName()).create());

    final PartitionedRegion pr2 =
        (PartitionedRegion) CacheUtils.getCache().createRegion("pr2", factory.create());

    createAllNumPRAndEvenNumPR(pr1, pr2, 80);
    Set<Integer> set = createAndPopulateSet(15);
    LocalDataSet lds = new LocalDataSet(pr1, set);

    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    QueryService qs = pr1.getCache().getQueryService();

    qs.createIndex("valueIndex", IndexType.FUNCTIONAL, "e.value", SEPARATOR + "pr1 e");
    qs.createIndex("valueIndex", IndexType.FUNCTIONAL, "e.value", SEPARATOR + "pr2 e");
    String query =
        "select distinct e1.value from " + SEPARATOR + "pr1 e1, " + SEPARATOR + "pr2  e2"
            + " where e1.value=e2.value";
    DefaultQuery cury = (DefaultQuery) CacheUtils.getQueryService().newQuery(query);
    final ExecutionContext executionContext =
        new QueryExecutionContext(null, (InternalCache) cache, cury);
    SelectResults r = (SelectResults) lds.executeQuery(cury, executionContext, null, set);

    if (!observer.isIndexesUsed) {
      fail("Indexes should have been used");
    }

  }

  @Test
  public void testEquijoinPRColocatedQuery_2() throws Exception {

    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(1)
        .setTotalNumBuckets(40).setPartitionResolver(new PartitionResolver() {

          @Override
          public String getName() {
            return "blah";
          }

          @Override
          public Serializable getRoutingObject(EntryOperation opDetails) {
            return (Serializable) opDetails.getKey();
          }

          @Override
          public void close() {

        }

        }).create());
    PartitionedRegion pr1 =
        (PartitionedRegion) CacheUtils.getCache().createRegion("pr1", factory.create());
    factory = new AttributesFactory();
    factory.setPartitionAttributes(new PartitionAttributesFactory()

        .setRedundantCopies(1).setTotalNumBuckets(40).setPartitionResolver(new PartitionResolver() {

          @Override
          public String getName() {
            return "blah";
          }

          @Override
          public Serializable getRoutingObject(EntryOperation opDetails) {
            return (Serializable) opDetails.getKey();
          }

          @Override
          public void close() {

        }

        }).setColocatedWith(pr1.getName()).create());

    final PartitionedRegion pr2 =
        (PartitionedRegion) CacheUtils.getCache().createRegion("pr2", factory.create());

    createAllNumPRAndEvenNumPR(pr1, pr2, 80);
    Set<Integer> set = createAndPopulateSet(15);
    LocalDataSet lds = new LocalDataSet(pr1, set);

    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    QueryService qs = pr1.getCache().getQueryService();

    qs.createIndex("valueIndex", IndexType.FUNCTIONAL, "e.value", SEPARATOR + "pr1.entries e");
    qs.createIndex("valueIndex", IndexType.FUNCTIONAL, "e.value", SEPARATOR + "pr2.entries e");
    String query =
        "select distinct e1.key from " + SEPARATOR + "pr1.entries e1," + SEPARATOR
            + "pr2.entries  e2" + " where e1.value=e2.value";
    DefaultQuery cury = (DefaultQuery) CacheUtils.getQueryService().newQuery(query);
    final ExecutionContext executionContext =
        new QueryExecutionContext(null, (InternalCache) cache, cury);
    SelectResults r = (SelectResults) lds.executeQuery(cury, executionContext, null, set);

    if (!observer.isIndexesUsed) {
      fail("Indexes should have been used");
    }
  }

  private void createAllNumPRAndEvenNumPR(final PartitionedRegion pr1, final PartitionedRegion pr2,
      final int range) {
    IntStream.rangeClosed(1, range).forEach(i -> {
      pr1.put(i, new MyValue(i));
      if (i % 2 == 0) {
        pr2.put(i, new MyValue(i));
      }
    });
  }

  private static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;

    ArrayList indexesUsed = new ArrayList();

    String IndexName;

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {

      IndexName = index.getName();
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }
}
