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
/*
 * IndexCreationJUnitTest.java
 *
 * Created on April 13, 2005, 4:16 PM Added a Test Case for testing the Task, IUM10 : May 16, 2005,
 * 2:45 PM
 */


package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.ComparableWrapper;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.CompactMapRangeIndex;
import org.apache.geode.cache.query.internal.index.CompactRangeIndex;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.query.internal.index.RangeIndex;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexCreationJUnitTest {

  private ObjectType resType1 = null;
  private ObjectType resType2 = null;

  private int resSize1 = 0;
  private int resSize2 = 0;

  private final Iterator itert1 = null;
  private final Iterator itert2 = null;

  private Set set1 = null;
  private Set set2 = null;

  private String s1;
  private String s2;

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testIndexCreation() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();

    Index i1 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
            SEPARATOR + "portfolios, positions");
    // TASK ICM1
    Index i2 = qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "portfolios pf, pf.positions.values b");
    // TASK ICM2
    Index i5 = qs.createIndex("intFunctionIndex", IndexType.FUNCTIONAL, "intFunction(pf.getID)",
        SEPARATOR + "portfolios pf, pf.positions b");
    Index i6 = qs.createIndex("statusIndex6", IndexType.FUNCTIONAL, "a.status",
        SEPARATOR + "portfolios.values.toArray a, positions");
    Index i7 = qs.createIndex("statusIndex7", IndexType.FUNCTIONAL, "a.status",
        SEPARATOR + "portfolios.getValues().asList() a, positions");
    Index i8 = qs.createIndex("statusIndex8", IndexType.FUNCTIONAL, "a.status",
        SEPARATOR + "portfolios.values.asSet a, positions");
    // TASK ICM6
    Object[] indices = {i1, i2, i5, i6, i7, i8}; // remove any commented Index
    // from Array

    for (int j = 0; j < indices.length; j++) {
      CacheUtils.log(((IndexProtocol) indices[j]).isValid());
      boolean r = ((IndexProtocol) indices[j]).isValid();
      assertTrue("Test: testIndexCreation FAILED", r);
      CacheUtils.log(((IndexProtocol) indices[j]).getName());
      CacheUtils.log("Test: testIndexCreation PASS");
    }
  }

  @Test
  public void testIndexCreationWithImports() throws Exception {
    // Task ID ICM 16
    QueryService qs;
    qs = CacheUtils.getQueryService();

    Index idx;

    try {
      idx = qs.createIndex("importsIndex", IndexType.FUNCTIONAL, "status",
          SEPARATOR + "portfolios, (map<string,Position>)positions");
      fail("Should have thrown a QueryInvalidException"); // can't find type
      // Position
    } catch (QueryInvalidException e) {
      // pass
    }

    idx = qs.createIndex("importsIndex", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios, (map<string,Position>)positions",
        "import org.apache.geode.cache.\"query\".data.Position");
    qs.removeIndex(idx);

    idx = qs.createIndex("importsIndex2", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios, positions TYPE Position",
        "import org.apache.geode.cache.\"query\".data.Position");
  }

  @Test
  public void testSimilarIndexCreation() throws Exception {
    // Task ID: ICM17
    QueryService qs;
    qs = CacheUtils.getQueryService();
    // boolean exceptionoccurred = true;
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios, positions");
    qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "portfolios pf, pf.positions.values b");
    try {
      qs.createIndex("secIdIndexDuplicate", IndexType.FUNCTIONAL, "b.secId",
          SEPARATOR + "portfolios pf, pf.positions.values b");
      fail("testSimilarIndexCreation: Allowed duplicate index creation");
    } catch (Exception e) {
      // testSimilarIndexCreation: Exception if duplicate index is
      // created with diffrenet name but same from clause & expression
    }

    try {
      qs.createIndex("secIdIndexDuplicate", IndexType.FUNCTIONAL, "b1.secId",
          SEPARATOR + "portfolios pf1, pf1.positions.values b1");
      fail("testSimilarIndexCreation: Allowed duplicate index creation");
    } catch (Exception e) {
      // testSimilarIndexCreation: Exception if duplicate index is
      // created with diffrenet name but same from clause & expression
    }
    // org.apache.geode.cache.query.IndexExistsException: Similar Index
    // Exists
    try {
      qs.createIndex("statusIndexDuplicate", IndexType.FUNCTIONAL, "b.status",
          SEPARATOR + "portfolios b, positions");
      fail("testSimilarIndexCreation: Allowed duplicate index creation");
    } catch (Exception e) {
      // testSimilarIndexCreation: Exception if duplicate index is
      // created with diffrenet name but same from clause & expression
    }
  }

  @Test
  public void testInvalidImportsIndexCreation() throws Exception {
    // Task ID: Invalid Indexes: ICM15
    QueryService qs;
    qs = CacheUtils.getQueryService();
    try {
      qs.createIndex("typeIndex", IndexType.FUNCTIONAL, "\"type\"",
          SEPARATOR + "portfolios pf, pf.positions b", "pf.position1");
      // projection attributes are not yet implemented
      // last parameter is the imports statement, so this is a syntax
      // error
      fail("Should have thrown an exception since imports are invalid");
      // TASK ICM7
    } catch (QueryInvalidException e) {
      // pass
    }
  }

  @Ignore("TODO: disabled and has no assertions")
  @Test
  public void testElementIndexCreation() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    qs.createIndex("funcReturnSecIdIndex", IndexType.FUNCTIONAL,
        "pf.funcReturnSecId(element(select distinct pos from " + SEPARATOR
            + "portfolios pf, pf.positions.values as pos where pos.sharesOutstanding = 5000))",
        SEPARATOR + "portfolios pf, pf.positions b");
    // TASK ICM8: InvalidIndexCreation
    // Query q = qs.newQuery("(element(select distinct pos from
    // /portfolios pf, pf.positions.values as pos where
    // pos.sharesOutstanding = 5000))");
    // Object r=q.execute();
    // CacheUtils.log(Utils.printResult(r));
  }

  @Test
  public void testIndexCreationOnNVLFunction() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Query query = null;
    qs.createIndex("NVLIndex1", IndexType.FUNCTIONAL, "nvl(pf.position2, pf.position1).secId",
        SEPARATOR + "portfolios pf");

    query = CacheUtils.getQueryService().newQuery(
        "select distinct * from " + SEPARATOR
            + "portfolios pf where nvl(pf.position2, pf.position1).secId = 'SUN'");
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    query.execute();

    if (!observer.isIndexesUsed) {
      fail("NO INDEX USED");
    }


    query = CacheUtils.getQueryService().newQuery(
        "select distinct nvl(pf.position2, 'inProjection') from " + SEPARATOR
            + "portfolios pf where nvl(pf.position2, pf.position1).secId = 'SUN'");
    observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    query.execute();

    if (!observer.isIndexesUsed && observer.indexesUsed.size() != 1) {
      fail("NO INDEX USED");
    }
  }

  @Test
  public void testIndexCreationWithImport() throws Exception {
    // Task ID: ICM16
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index i3 = qs.createIndex("typeIndex", IndexType.FUNCTIONAL, "\"type\"",
        SEPARATOR + "portfolios type Portfolio, positions b",
        "IMPORT org.apache.geode.cache.\"query\".data.Portfolio");
    // TASK ICM3 Region 'IMPORT' not found:....[BUG : Verified Fixed ]
    // Index i4=(Index)qs.createIndex("boolFunctionIndex",
    // IndexType.FUNCTIONAL,"boolFunction(pf.status)","/portfolios pf,
    // pf.positions.values b");
    // TASK ICM5 org.apache.geode.cache.query.IndexInvalidException

    Object[] indices = {i3}; // remove any commented Index from Array

    for (int j = 0; j < indices.length; j++) {
      CacheUtils.log(((IndexProtocol) indices[j]).isValid());
      boolean r = ((IndexProtocol) indices[j]).isValid();
      if (r == true) {
        CacheUtils.log(((IndexProtocol) indices[j]).getName());
        CacheUtils.log("Test: testIndexCreation PASS");
      } else {
        fail("Test: testIndexCreation FAILED");
      }
    }
  }

  @Test
  public void testComparisonBetnWithAndWithoutIndexCreationComparableObject() throws Exception {
    // Task ID IUM10
    SelectResults[][] r = new SelectResults[4][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries =
        {"select distinct * from " + SEPARATOR + "portfolios pf where pf.getCW(pf.ID) = $1",
            "select distinct * from " + SEPARATOR + "portfolios pf where pf.getCW(pf.ID) > $1",
            "select distinct * from " + SEPARATOR + "portfolios pf where pf.getCW(pf.ID) < $1",
            "select distinct * from " + SEPARATOR + "portfolios pf where pf.getCW(pf.ID) != $1"
        // TASK IUM 10
        };
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      q = CacheUtils.getQueryService().newQuery(queries[i]);
      Object[] params = new Object[1];
      params[0] = new ComparableWrapper(1);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][0] = (SelectResults) q.execute(params);

      resType1 = (r[i][0]).getCollectionType().getElementType();
      resSize1 = ((r[i][0]).size());
      set1 = ((r[i][0]).asSet());
      // Iterator iter=set1.iterator();
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    qs.createIndex("cIndex", IndexType.FUNCTIONAL, "pf.getCW(pf.ID)", SEPARATOR + "portfolios pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      q = CacheUtils.getQueryService().newQuery(queries[i]);
      Object[] params = new Object[1];
      params[0] = new ComparableWrapper(1);
      QueryObserverImpl observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      r[i][1] = (SelectResults) q.execute(params);
      if (!observer2.isIndexesUsed) {
        fail("FAILED: Index NOT Used");
      }
      resType2 = (r[i][1]).getCollectionType().getElementType();
      resSize2 = ((r[i][1]).size());
      set2 = ((r[i][1]).asSet());
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);
  }

  @Test
  public void testIndexCreationWithIndexOperatorUsage() throws Exception {
    // Task ID : ICM 18
    QueryService qs;
    qs = CacheUtils.getQueryService();

    String[] queries = {
        "select distinct * from " + SEPARATOR
            + "portfolios pf where pf.collectionHolderMap[(pf.ID).toString()].arr[pf.ID] != -1"};

    Object[][] r = new Object[queries.length][2];
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      q = qs.newQuery(queries[i]);
      CacheUtils.getLogger().info("Executing query: " + queries[i]);
      r[i][0] = q.execute();
      CacheUtils.log("Executed query:" + queries[i]);
    }
    Index i1 = qs.createIndex("fIndex", IndexType.FUNCTIONAL, "sIter",
        SEPARATOR + "portfolios pf, pf.collectionHolderMap[(pf.ID).toString()].arr sIter");
    Index i2 = qs.createIndex("cIndex", IndexType.FUNCTIONAL,
        "pf.collectionHolderMap[(pf.ID).toString()].arr[pf.ID]", SEPARATOR + "portfolios pf");
    // BUG # 32498
    // Index i3 = qs.createIndex("nIndex", IndexType.FUNCTIONAL,
    // "pf.collectionHolderMap[((pf.ID%2)).toString()].arr[pf.ID]","/portfolios
    // pf");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      q = qs.newQuery(queries[i]);
      CacheUtils.getLogger().info("Executing query: " + queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      SelectResults results = (SelectResults) r[i][1];
      assertTrue(results.size() > 0);
      CacheUtils.log("Executing query: " + queries[i] + " with index created");
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertTrue(itr.hasNext());
      String temp = itr.next().toString();
      assertEquals(temp, "cIndex");
    }

    CacheUtils.log(((RangeIndex) i1).dump());
    CacheUtils.log(((CompactRangeIndex) i2).dump());

    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
    // CacheUtils.log(((RangeIndex)i3).dump());
    // Index i3 =
    // qs.createIndex("Task6Index",IndexType.FUNCTIONAL,"pos.secId","/portfolios
    // pf, pf.positions.values pos");
  }

  @Test
  public void testIndexCreationOnKeys() throws Exception {
    // Task ID : ICM 9
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index i1 =
        qs.createIndex("kIndex", IndexType.FUNCTIONAL, "pf", SEPARATOR + "portfolios.keys pf");
    Index i2 =
        qs.createIndex("k1Index", IndexType.FUNCTIONAL, "key", SEPARATOR + "portfolios.entries");
    Index i3 = qs.createIndex("k2Index", IndexType.FUNCTIONAL, "pf",
        SEPARATOR + "portfolios.keys.toArray pf");
    // Index i4 = qs.createIndex("k3Index", IndexType.FUNCTIONAL,
    // "pf","/portfolios.keys().toArray() pf");
    Index i5 =
        qs.createIndex("k4Index", IndexType.FUNCTIONAL, "pf",
            SEPARATOR + "portfolios.getKeys.asList pf");
    // Index i5 = qs.createIndex("k5Index", IndexType.FUNCTIONAL,
    // "pf","/portfolios.getKeys.asList() pf");
    Index i6 =
        qs.createIndex("k5Index", IndexType.FUNCTIONAL, "pf",
            SEPARATOR + "portfolios.getKeys.asSet() pf");
    // Index i5 = qs.createIndex("k5Index", IndexType.FUNCTIONAL,
    // "pf","/portfolios.getKeys.asSet pf");
    CacheUtils.log(((CompactRangeIndex) i1).dump());
    CacheUtils.log(((CompactRangeIndex) i2).dump());
    CacheUtils.log(((CompactRangeIndex) i3).dump());
    CacheUtils.log(((CompactRangeIndex) i5).dump());
    CacheUtils.log(((CompactRangeIndex) i6).dump());
  }

  @Test
  public void testIndexCreationOnRegionEntry() throws Exception {
    // Task ID : ICM11
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index i1 = qs.createIndex("r1Index", IndexType.FUNCTIONAL, "secId",
        SEPARATOR + "portfolios.values['1'].positions.values");
    qs.createIndex("r12Index", IndexType.FUNCTIONAL, "secId",
        SEPARATOR + "portfolios['1'].positions.values");
    CacheUtils.log(((CompactRangeIndex) i1).dump());
    // CacheUtils.log(((RangeIndex)i2).dump());
  }


  /**
   * Creation of index on a path derived from Region.Entry object obtained via entrySet , fails as
   * that function was not supported in the QRegion & DummyQRegion
   */
  @Test
  public void testBug36823() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    qs.createIndex("entryIndex", IndexType.FUNCTIONAL, "value.getID()",
        SEPARATOR + "portfolios.entrySet pf");
    Region rgn = CacheUtils.getRegion(SEPARATOR + "portfolios");
    rgn.put("4", new Portfolio(4));
    rgn.put("5", new Portfolio(5));
    Query qr =
        qs.newQuery("Select distinct * from " + SEPARATOR
            + "portfolios.entrySet pf where pf.value.getID() = 4");
    SelectResults sr = (SelectResults) qr.execute();
    assertEquals(sr.size(), 1);
  }

  /**
   * Creation of index on key path derived from Region.Entry object obtained via keySet , fails as
   * that function was not supported in the QRegion & DummyQRegion
   */
  @Test
  public void testBug36590() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();

    qs.createIndex("keyIndex", IndexType.FUNCTIONAL, "keys", SEPARATOR + "portfolios.keySet keys");
    Region rgn = CacheUtils.getRegion(SEPARATOR + "portfolios");
    rgn.put("4", new Portfolio(4));
    rgn.put("5", new Portfolio(5));
    Query qr = qs.newQuery(
        "Select distinct  * from " + SEPARATOR + "portfolios.keySet keys where keys = '4'");
    SelectResults sr = (SelectResults) qr.execute();
    assertEquals(sr.size(), 1);
  }

  /**
   * The Index maintenance has a bug as it does not re-evaluate the index maintenance collection in
   * the IMQEvaluator when an entry gets modified & so the index resultset is messed up
   */
  @Test
  public void testBug36591() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index i1 =
        qs.createIndex("keyIndex", IndexType.FUNCTIONAL, "ks.hashCode",
            SEPARATOR + "portfolios.keys ks");
    Region rgn = CacheUtils.getRegion(SEPARATOR + "portfolios");
    rgn.put("4", new Portfolio(4));
    rgn.put("5", new Portfolio(5));
    CacheUtils.log(((CompactRangeIndex) i1).dump());

    Query qr =
        qs.newQuery("Select distinct * from " + SEPARATOR
            + "portfolios.keys keys where keys.hashCode >= $1");
    SelectResults sr = (SelectResults) qr.execute(new Object[] {new Integer(-1)});
    assertEquals(6, sr.size());
  }

  /**
   * Creation of index on a path derived from Region.Entry object obtained via entrySet , fails as
   * that function was not supported in the QRegion & DummyQRegion
   */
  @Test
  public void testBug43519() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index index =
        qs.createIndex("shortIndex", IndexType.FUNCTIONAL, "p.shortID", SEPARATOR + "portfolios p");
    Region rgn = CacheUtils.getRegion(SEPARATOR + "portfolios");
    for (int i = 1; i <= 10; i++) {
      String key = "" + i;
      Portfolio p = new Portfolio(i);
      p.shortID = new Short(key);
      // addToIndex
      rgn.put(key, p);
      // updateIndex
      rgn.put(key, p);
      if (i % 2 == 0) {
        // destroy from index.
        rgn.destroy(key);
      }
    }
    Query qr =
        qs.newQuery("Select p.shortID from " + SEPARATOR + "portfolios p where p.shortID < 5");
    SelectResults sr = (SelectResults) qr.execute();
    assertEquals(sr.size(), 2);
  }

  /**
   * Test the Index maiantenance as it may use the method keys() of QRegion instead of DummyQRegion
   * while running an IndexMaintenanceQuery
   */
  @Test
  public void testIMQFailureAsMethodKeysNAInDummyQRegion() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index i1 =
        qs.createIndex("keyIndex", IndexType.FUNCTIONAL, "ks.hashCode",
            SEPARATOR + "portfolios.keys() ks");
    Region rgn = CacheUtils.getRegion(SEPARATOR + "portfolios");
    rgn.put("4", new Portfolio(4));
    rgn.put("5", new Portfolio(5));
    CacheUtils.log(((CompactRangeIndex) i1).dump());

    Query qr = qs.newQuery(
        "Select distinct keys.hashCode  from " + SEPARATOR
            + "portfolios.keys() keys where keys.hashCode >= $1");
    SelectResults sr = (SelectResults) qr.execute(new Object[] {new Integer(-1)});
    assertEquals(6, sr.size());
  }

  @Test
  public void testIndexCreationWithFunctions() throws Exception {
    // Task ID : ICM14
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index i1 = qs.createIndex("SetSecIDIndex1", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "portfolios.asSet pf, pf.positions.values b");
    Index i2 = qs.createIndex("ListSecIDIndex2", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "portfolios.asList pf, pf.positions.values b");
    Index i3 = qs.createIndex("ArraySecIDIndex3", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "portfolios.toArray pf, pf.positions.values b");
    CacheUtils.log(((RangeIndex) i1).dump());
    CacheUtils.log(((RangeIndex) i2).dump());
    CacheUtils.log(((RangeIndex) i3).dump());
  }

  @Test
  public void testInvalidIndexes() throws Exception {
    // Task ID: ICM15
    QueryService qs;
    qs = CacheUtils.getQueryService();
    try {
      Index i1 = qs.createIndex("r1Index", IndexType.FUNCTIONAL, "secId",
          SEPARATOR + "portfolios.toArray[1].positions.values");
      CacheUtils.log(((RangeIndex) i1).dump());
      fail("Index creation should have failed");
    } catch (Exception e) {
    }
    try {
      Index i2 = qs.createIndex("r12Index", IndexType.FUNCTIONAL, "secId",
          SEPARATOR + "portfolios.asList[1].positions.values");
      CacheUtils.log(((RangeIndex) i2).dump());
      fail("Index creation should have failed");
    } catch (Exception e) {
    }
  }

  @Test
  public void testIndexCreationWithFunctionsinFromClause() throws Exception {
    // Task ID: ICM13
    QueryService qs;
    qs = CacheUtils.getQueryService();
    // BUG #32586 : FIXED
    Index i1 =
        qs.createIndex("Index11", IndexType.FUNCTIONAL, "status",
            SEPARATOR + "portfolios.values.toArray()");
    Index i2 = qs.createIndex("Index12", IndexType.FUNCTIONAL, "ID",
        SEPARATOR + "portfolios.values.asSet");
    Index i3 = qs.createIndex("Index13", IndexType.FUNCTIONAL, "ID",
        SEPARATOR + "portfolios.values.asList");

    qs.createIndex("Index14", IndexType.FUNCTIONAL, "value.ID",
        SEPARATOR + "portfolios.entries.toArray()");
    qs.createIndex("Index15", IndexType.FUNCTIONAL, "value.ID",
        SEPARATOR + "portfolios.entries.asSet");
    qs.createIndex("Index16", IndexType.FUNCTIONAL, "value.ID",
        SEPARATOR + "portfolios.entries.asList");

    // BUG #32586 : FIXED
    qs.createIndex("Index17", IndexType.FUNCTIONAL, "kIter",
        SEPARATOR + "portfolios.keys.toArray() kIter");
    qs.createIndex("Index18", IndexType.FUNCTIONAL, "kIter",
        SEPARATOR + "portfolios.keys.asSet kIter");
    qs.createIndex("Index19", IndexType.FUNCTIONAL, "kIter",
        SEPARATOR + "portfolios.keys.asList kIter");

    CacheUtils.log(((CompactRangeIndex) i1).dump());
    CacheUtils.log(((CompactRangeIndex) i2).dump());
    CacheUtils.log(((CompactRangeIndex) i3).dump());
  }

  @Test
  public void testIndexObjectTypeWithRegionConstraint() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "portfolios pf, pf.positions.values b");
    ObjectType type = ((IndexProtocol) i1).getResultSetType();
    String[] fieldNames = {"index_iter1", "index_iter2"};
    ObjectType[] fieldTypes =
        {new ObjectTypeImpl(Portfolio.class), new ObjectTypeImpl(Object.class)};
    // ObjectType expectedType = new StructTypeImpl( fieldNames,fieldTypes);
    ObjectType expectedType = new StructTypeImpl(fieldNames, fieldTypes);
    if (!(type instanceof StructType && type.equals(expectedType))) {
      fail(
          "The ObjectType obtained from index is not of the expected type. Type obtained from index="
              + type);
    }

    Index i2 =
        qs.createIndex("Index2", IndexType.FUNCTIONAL, "pf.ID", SEPARATOR + "portfolios.values pf");
    type = ((IndexProtocol) i2).getResultSetType();

    expectedType = new ObjectTypeImpl(Portfolio.class);
    if (!type.equals(expectedType)) {
      fail(
          "The ObjectType obtained from index is not of the expected type. Type obtained from index="
              + type);
    }

    Index i3 = qs.createIndex("Index3", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "portfolios['0'].positions.values pos");
    type = ((IndexProtocol) i3).getResultSetType();

    expectedType = new ObjectTypeImpl(Object.class);
    if (!type.equals(expectedType)) {
      fail(
          "The ObjectType obtained from index is not of the expected type. Type obtained from index="
              + type);
    }

    Index i4 = qs.createIndex("Index4", IndexType.PRIMARY_KEY, "ID", SEPARATOR + "portfolios");
    type = ((IndexProtocol) i4).getResultSetType();

    expectedType = new ObjectTypeImpl(Portfolio.class);
    if (!type.equals(expectedType)) {
      fail(
          "The ObjectType obtained from index is not of the expected type. Type obtained from index="
              + type);
    }
  }

  @Test
  public void testIndexOnOverflowRegion() throws Exception {
    String regionName = "portfolios_overflow";

    // overflow region.
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setValueConstraint(Portfolio.class);
    attributesFactory.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));

    Region region = CacheUtils.createRegion(regionName, attributesFactory.create(), true);

    for (int i = 0; i < 4; i++) {
      region.put(new Portfolio(i), new Portfolio(i));
    }

    QueryService qs = CacheUtils.getQueryService();
    // Currently supported with compact range-index.
    Index i1 = qs.createIndex("idIndex", IndexType.FUNCTIONAL, "pf.ID",
        SEPARATOR + "portfolios_overflow pf");
    Index i2 = qs.createIndex("keyIdIndex", IndexType.FUNCTIONAL, "key.ID",
        SEPARATOR + "portfolios_overflow.keys key");

    // Not yet supported with range-index.
    try {
      Index i3 = qs.createIndex("idIndex2", IndexType.FUNCTIONAL, "pf.ID",
          SEPARATOR + "portfolios_overflow pf, pf.positions pos");
      fail("Range index not supported on overflow region.");
    } catch (UnsupportedOperationException ex) {
      // Expected.
    }

    // Execute query.
    String[] queryStr =
        new String[] {"Select * from " + SEPARATOR + "portfolios_overflow pf where pf.ID = 2",
            "Select * from " + SEPARATOR + "portfolios_overflow.keys key where key.ID = 2",
            "Select * from " + SEPARATOR + "portfolios_overflow pf where pf.ID > 1",
            "Select * from " + SEPARATOR + "portfolios_overflow pf where pf.ID < 2",};

    int[] resultSize = new int[] {1, 1, 2, 2};

    for (int i = 0; i < queryStr.length; i++) {
      Query q = qs.newQuery(queryStr[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      SelectResults results = (SelectResults) q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index not used for query. " + queryStr[i]);
      }
      assertEquals(results.size(), resultSize[i]);
    }

    for (int i = 0; i < 10; i++) {
      region.put(new Portfolio(i), new Portfolio(i));
    }

    // Persistent overflow region.

  }

  @Test
  public void testMapKeyIndexCreation_1_NonCompactType() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "pf.positions[*]",
        SEPARATOR + "portfolios pf");
    assertEquals(i1.getCanonicalizedIndexedExpression(), "index_iter1.positions[*]");
    assertTrue(i1 instanceof CompactMapRangeIndex);
  }

  @Test
  public void testMapKeyIndexCreation_2_NonCompactType() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "pf.positions['key1','key2','key3']",
        SEPARATOR + "portfolios pf");
    assertEquals(i1.getCanonicalizedIndexedExpression(),
        "index_iter1.positions['key1','key2','key3']");
    assertTrue(i1 instanceof CompactMapRangeIndex);
    CompactMapRangeIndex mri = (CompactMapRangeIndex) i1;
    Object[] mapKeys = mri.getMapKeysForTesting();
    assertEquals(mapKeys.length, 3);
    Set<String> keys = new HashSet<>();
    keys.add("key1");
    keys.add("key2");
    keys.add("key3");
    for (Object key : mapKeys) {
      keys.remove(key);
    }
    assertTrue(keys.isEmpty());
    String[] patterns = mri.getPatternsForTesting();
    assertEquals(patterns.length, 3);
    Set<String> patternsSet = new HashSet<>();
    patternsSet.add("index_iter1.positions['key1']");
    patternsSet.add("index_iter1.positions['key2']");
    patternsSet.add("index_iter1.positions['key3']");
    for (String ptrn : patterns) {
      patternsSet.remove(ptrn);
    }
    assertTrue(patternsSet.isEmpty());
    assertEquals(mri.getIndexedExpression(), "pf.positions['key1','key2','key3']");
  }

  /**
   * Test for bug 46872, make sure we recover the index correctly if the cache.xml changes for a
   * persistent region.
   */
  @Test
  public void testIndexCreationFromXML() throws Exception {
    InternalDistributedSystem.getAnyInstance().disconnect();
    File file = new File("persistData0");
    file.mkdir();

    {
      Properties props = new Properties();
      props.setProperty(NAME, "test");
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(CACHE_XML_FILE,
          getClass().getResource("index-creation-with-eviction.xml").toURI().getPath());
      DistributedSystem ds = DistributedSystem.connect(props);

      // Create the cache which causes the cache-xml-file to be parsed
      Cache cache = CacheFactory.create(ds);
      QueryService qs = cache.getQueryService();
      Region region = cache.getRegion("mainReportRegion");
      for (int i = 0; i < 100; i++) {
        Portfolio pf = new Portfolio(i);
        pf.setCreateTime(i);
        region.put("" + i, pf);
      }

      // verify that a query on the creation time works as expected
      SelectResults results = (SelectResults) qs
          .newQuery(
              "<trace>SELECT * FROM " + SEPARATOR
                  + "mainReportRegion.entrySet mr Where mr.value.createTime > 1L and mr.value.createTime < 3L")
          .execute();
      assertEquals("OQL index results did not match", 1, results.size());
      cache.close();
      ds.disconnect();
    }

    {
      Properties props = new Properties();
      props.setProperty(NAME, "test");
      props.setProperty(MCAST_PORT, "0");
      // Using a different cache.xml that changes some region properties
      // That will force the disk code to copy the region entries.
      props.setProperty(CACHE_XML_FILE,
          getClass().getResource("index-creation-without-eviction.xml").toURI().getPath());
      DistributedSystem ds = DistributedSystem.connect(props);
      Cache cache = CacheFactory.create(ds);
      QueryService qs = cache.getQueryService();
      Region region = cache.getRegion("mainReportRegion");

      // verify that a query on the creation time works as expected
      SelectResults results = (SelectResults) qs
          .newQuery(
              "<trace>SELECT * FROM " + SEPARATOR
                  + "mainReportRegion.entrySet mr Where mr.value.createTime > 1L and mr.value.createTime < 3L")
          .execute();
      assertEquals("OQL index results did not match", 1, results.size());
      ds.disconnect();
      FileUtils.deleteDirectory(file);
    }
  }

  @Test
  public void testIndexCreationFromXMLForLocalScope() throws Exception {
    InternalDistributedSystem.getAnyInstance().disconnect();
    File file = new File("persistData0");
    file.mkdir();

    Properties props = new Properties();
    props.setProperty(NAME, "test");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(CACHE_XML_FILE,
        getClass().getResource("index-creation-without-eviction.xml").toURI().getPath());
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = CacheFactory.create(ds);
    Region localRegion = cache.getRegion("localRegion");
    for (int i = 0; i < 100; i++) {
      Portfolio pf = new Portfolio(i);
      localRegion.put("" + i, pf);
    }
    QueryService qs = cache.getQueryService();
    Index ind = qs.getIndex(localRegion, "localIndex");
    assertNotNull("Index localIndex should have been created ", ind);
    // verify that a query on the creation time works as expected
    SelectResults results = (SelectResults) qs
        .newQuery("<trace>SELECT * FROM " + localRegion.getFullPath() + " Where ID > 0").execute();
    assertEquals("OQL index results did not match", 99, results.size());
    ds.disconnect();
    FileUtils.deleteDirectory(file);
  }

  @Test
  public void testIndexCreationFromXMLForDiskLocalScope() throws Exception {
    InternalDistributedSystem.getAnyInstance().disconnect();
    File file = new File("persistData0"); // TODO: use TemporaryFolder
    file.mkdir();

    Properties props = new Properties();
    props.setProperty(NAME, "test");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(CACHE_XML_FILE,
        getClass().getResource("index-creation-without-eviction.xml").toURI().getPath());
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = CacheFactory.create(ds);
    Region localDiskRegion = cache.getRegion("localDiskRegion");
    for (int i = 0; i < 100; i++) {
      Portfolio pf = new Portfolio(i);
      localDiskRegion.put("" + i, pf);
    }
    QueryService qs = cache.getQueryService();
    Index ind = qs.getIndex(localDiskRegion, "localDiskIndex");
    assertNotNull("Index localIndex should have been created ", ind);
    // verify that a query on the creation time works as expected
    SelectResults results = (SelectResults) qs
        .newQuery(
            "<trace>SELECT * FROM " + localDiskRegion.getFullPath() + " Where status = 'active'")
        .execute();
    assertEquals("OQL index results did not match", 50, results.size());
    ds.disconnect();
    FileUtils.deleteDirectory(file);
  }

  @Test
  public void testIndexInitializationForOverFlowRegions() throws Exception {
    InternalDistributedSystem.getAnyInstance().disconnect();
    File file = new File("persistData0");
    file.mkdir();

    {
      Properties props = new Properties();
      props.setProperty(NAME, "test");
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
      props.setProperty(ENABLE_TIME_STATISTICS, "true");
      props.setProperty(CACHE_XML_FILE,
          getClass().getResource("index-recovery-overflow.xml").toURI().getPath());
      DistributedSystem ds = DistributedSystem.connect(props);

      // Create the cache which causes the cache-xml-file to be parsed
      Cache cache = CacheFactory.create(ds);
      QueryService qs = cache.getQueryService();
      Region region = cache.getRegion("mainReportRegion");
      for (int i = 0; i < 100; i++) {
        Portfolio pf = new Portfolio(i);
        pf.setCreateTime(i);
        region.put("" + i, pf);
      }

      IndexStatistics is1 = qs.getIndex(region, "status").getStatistics();
      assertEquals(2, is1.getNumberOfKeys());
      assertEquals(100, is1.getNumberOfValues());

      IndexStatistics is2 = qs.getIndex(region, "ID").getStatistics();
      assertEquals(100, is2.getNumberOfKeys());
      assertEquals(100, is2.getNumberOfValues());

      // verify that a query on the creation time works as expected
      SelectResults results = (SelectResults) qs
          .newQuery(
              "<trace>SELECT * FROM " + SEPARATOR
                  + "mainReportRegion.entrySet mr Where mr.value.createTime > 1L and mr.value.createTime < 3L")
          .execute();
      assertEquals("OQL index results did not match", 1, results.size());
      cache.close();
      ds.disconnect();
    }

    {
      Properties props = new Properties();
      props.setProperty(NAME, "test");
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
      props.setProperty(ENABLE_TIME_STATISTICS, "true");
      props.setProperty(CACHE_XML_FILE,
          getClass().getResource("index-recovery-overflow.xml").toURI().getPath());
      DistributedSystem ds = DistributedSystem.connect(props);
      Cache cache = CacheFactory.create(ds);
      QueryService qs = cache.getQueryService();
      Region region = cache.getRegion("mainReportRegion");

      assertTrue("Index initialization time should not be 0.",
          ((LocalRegion) region).getCachePerfStats().getIndexInitializationTime() > 0);

      IndexStatistics is1 = qs.getIndex(region, "status").getStatistics();
      assertEquals(2, is1.getNumberOfKeys());
      assertEquals(100, is1.getNumberOfValues());

      IndexStatistics is2 = qs.getIndex(region, "ID").getStatistics();
      assertEquals(100, is2.getNumberOfKeys());
      assertEquals(100, is2.getNumberOfValues());

      // verify that a query on the creation time works as expected
      SelectResults results = (SelectResults) qs
          .newQuery(
              "<trace>SELECT * FROM " + SEPARATOR
                  + "mainReportRegion.entrySet mr Where mr.value.createTime > 1L and mr.value.createTime < 3L")
          .execute();
      assertEquals("OQL index results did not match", 1, results.size());
      ds.disconnect();
      FileUtils.deleteDirectory(file);
    }
  }

  @Test
  public void testIndexCreationWithoutLoadingData() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();

    Index i1 = ((DefaultQueryService) qs).createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios", null, false);
    Index i2 = ((DefaultQueryService) qs).createIndex("secIndex", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "portfolios p, p.positions.values pos", null, false);
    Index i3 = ((DefaultQueryService) qs).createIndex("statusHashIndex", IndexType.HASH, "status",
        SEPARATOR + "portfolios", null, false);

    assertEquals("Index should have been empty ", 0, i1.getStatistics().getNumberOfKeys());
    assertEquals("Index should have been empty ", 0, i1.getStatistics().getNumberOfValues());
    assertEquals("Index should have been empty ", 0, i2.getStatistics().getNumberOfKeys());
    assertEquals("Index should have been empty ", 0, i2.getStatistics().getNumberOfValues());
    assertEquals("Index should have been empty ", 0, i3.getStatistics().getNumberOfKeys());
    assertEquals("Index should have been empty ", 0, i3.getStatistics().getNumberOfValues());

    qs.removeIndexes();

    i1 = ((DefaultQueryService) qs).createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios", null, true);
    i2 = ((DefaultQueryService) qs).createIndex("secIndex", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "portfolios p, p.positions.values pos", null, true);
    i3 = ((DefaultQueryService) qs).createIndex("statusHashIndex", IndexType.HASH, "status",
        SEPARATOR + "portfolios", null, true);

    assertEquals("Index should not have been empty ", 2, i1.getStatistics().getNumberOfKeys());
    assertEquals("Index should not have been empty ", 4, i1.getStatistics().getNumberOfValues());
    assertEquals("Index should not have been empty ", 8, i2.getStatistics().getNumberOfKeys());
    assertEquals("Index should not have been empty ", 8, i2.getStatistics().getNumberOfValues());
    assertEquals("Index should not have been empty ", 0, i3.getStatistics().getNumberOfKeys()); // hash
                                                                                                // index
                                                                                                // does
                                                                                                // not
                                                                                                // have
                                                                                                // keys
    assertEquals("Index should not have been empty ", 4, i3.getStatistics().getNumberOfValues());
  }


  @Test
  public void failedIndexCreationCorrectlyRemovesItself() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Cache cache = CacheUtils.getCache();
    cache.createRegionFactory(RegionShortcut.PARTITION).create("portfoliosInPartitionedRegion");
    Region region = CacheUtils.getCache().getRegion(SEPARATOR + "portfoliosInPartitionedRegion");
    IntStream.range(0, 3).forEach((i) -> {
      region.put(i, new Portfolio(i));
    });

    Index i1 = qs.createIndex("statusIndex", "secId",
        SEPARATOR + "portfoliosInPartitionedRegion p, p.positions pos, pos.secId secId");
    try {
      Index i2 =
          qs.createIndex("anotherIndex", "secId",
              SEPARATOR + "portfoliosInPartitionedRegion p, p.positions");
      // index should fail to create
      fail();
    } catch (IndexInvalidException e) {
    }
    qs.removeIndex(i1);
    // This test should not throw an exception if i2 was properly cleaned up.
    Index i3 = qs.createIndex("anotherIndex", "secType",
        SEPARATOR + "portfoliosInPartitionedRegion p, p.positions pos, pos.secType secType");
    assertNotNull(i3);
  }


  private static class QueryObserverImpl extends QueryObserverAdapter {

    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
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
