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
 * IndexWithSngleFrmAndMultCondQryJUnitTest.java
 *
 * Created on April 26, 2005, 6:03 PM
 */


package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
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
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexWithSngleFrmAndMultCondQryJUnitTest {
  StructType resType1 = null;
  StructType resType2 = null;

  String[] strg1 = null;
  String[] strg2 = null;

  int resSize1 = 0;
  int resSize2 = 0;

  Object valPf1 = null;
  Object valPos1 = null;

  Object valPf2 = null;
  Object valPos2 = null;

  Iterator itert1 = null;
  Iterator itert2 = null;

  Set set1 = null;
  Set set2 = null;

  boolean isActive1 = false;
  boolean isActive2 = false;

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testComparisonBetnWithAndWithoutIndexCreation() throws Exception {

    Region region = CacheUtils.createRegion("pos", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries = {
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos pf,  positions.values pos where pf.status='active' and pos.secId= 'IBM' and ID = 0"};
    SelectResults[][] sr = new SelectResults[queries.length][2];
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i][0] = (SelectResults) q.execute();
        if (!observer.isIndexesUsed) {
          CacheUtils.log("NO INDEX USED");
        } else {
          fail("How could index be present when not created!?");
        }
        // CacheUtils.log(Utils.printResult(r));
        resType1 = (StructType) sr[i][0].getCollectionType().getElementType();
        resSize1 = (sr[i][0].size());
        CacheUtils.log(resType1);
        strg1 = resType1.getFieldNames();

        set1 = (sr[i][0].asSet());
        for (final Object o : set1) {
          Struct stc1 = (Struct) o;
          valPf1 = stc1.get(strg1[0]);
          valPos1 = stc1.get(strg1[1]);
          isActive1 = ((Portfolio) stc1.get(strg1[0])).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    Index index1 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "pos pf");
    // Index index2 = (Index)qs.createIndex("secIdIndex", IndexType.FUNCTIONAL,"pos.secId","/pos pf,
    // pf.positions.values pos");
    Index index3 = qs.createIndex("IDIndex", IndexType.FUNCTIONAL, "pf.ID", SEPARATOR + "pos pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        sr[i][1] = (SelectResults) q.execute();
        if (!observer2.isIndexesUsed) {
          fail("FAILED: Index NOT Used");
        }
        resType2 = (StructType) sr[i][1].getCollectionType().getElementType();
        resSize2 = (sr[i][1].size());
        strg2 = resType2.getFieldNames();

        set2 = (sr[i][1].asSet());
        for (final Object o : set2) {
          Struct stc2 = (Struct) o;
          valPf2 = stc2.get(strg2[0]);
          valPos2 = stc2.get(strg2[1]);
          isActive2 = ((Portfolio) stc2.get(strg2[0])).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    if ((resType1).equals(resType2)) {
      CacheUtils.log("Both Search Results are of the same Type i.e.--> " + resType1);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }
    if (resSize1 == resSize2 || resSize1 != 0) {
      CacheUtils
          .log("Search Results size is Non Zero and equal in both cases i.e.  Size= " + resSize1);
    } else {
      fail("FAILED:Search result size is different in both the cases");
    }
    itert2 = set2.iterator();
    itert1 = set1.iterator();
    while (itert1.hasNext()) {
      Struct stc2 = (Struct) itert2.next();
      Struct stc1 = (Struct) itert1.next();
      if (stc2.get(strg2[0]) != stc1.get(strg1[0])) {
        fail(
            "FAILED: In both the Cases the first member of StructSet i.e. Portfolio are different. ");
      }
      if (stc2.get(strg2[1]) != stc1.get(strg1[1])) {
        fail("FAILED: In both the cases Positions are different");
      }
      if (!StringUtils.equals(((Position) stc2.get(strg2[1])).secId,
          ((Position) stc1.get(strg1[1])).secId)) {
        fail("FAILED: In both the cases Positions secIds are different");
      }
      if (((Portfolio) stc2.get(strg2[0])).isActive() != ((Portfolio) stc1.get(strg1[0]))
          .isActive()) {
        fail("FAILED: Status of the Portfolios found are different");
      }
      if (((Portfolio) stc2.get(strg2[0])).getID() != ((Portfolio) stc1.get(strg1[0])).getID()) {
        fail("FAILED: IDs of the Portfolios found are different");
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }

  @Test
  public void testIndexSkipping() throws Exception {
    Region region = CacheUtils.createRegion("pos", Portfolio.class);
    for (int i = 0; i < 10; i++) {
      region.put("" + i, new Portfolio(i));
    }
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries = {
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos pf,  positions.values pos where pf.ID > 0 and pf.ID < 3  and pf.status='active' and  pos.secId != null "};
    SelectResults[][] sr = new SelectResults[queries.length][2];
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i][0] = (SelectResults) q.execute();
        if (!observer.isIndexesUsed) {
          CacheUtils.log("NO INDEX USED");
        } else {
          fail("How could index be present when not created!?");
        }
        // CacheUtils.log(Utils.printResult(r));
        resType1 = (StructType) sr[i][0].getCollectionType().getElementType();
        resSize1 = (sr[i][0].size());
        CacheUtils.log(resType1);
        strg1 = resType1.getFieldNames();

        set1 = (sr[i][0].asSet());
        for (final Object o : set1) {
          Struct stc1 = (Struct) o;
          valPf1 = stc1.get(strg1[0]);
          valPos1 = stc1.get(strg1[1]);
          isActive1 = ((Portfolio) stc1.get(strg1[0])).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    Index index1 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "pos pf");
    Index index2 = qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "pos pf, pf.positions.values pos");
    Index index3 = qs.createIndex("IDIndex", IndexType.FUNCTIONAL, "pf.ID", SEPARATOR + "pos pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        sr[i][1] = (SelectResults) q.execute();
        if (!observer2.isIndexesUsed) {
          fail("FAILED: Index NOT Used");
        }
        assertTrue(observer2.indexesUsed.size() < 2);

        resType2 = (StructType) sr[i][1].getCollectionType().getElementType();
        resSize2 = (sr[i][1].size());
        strg2 = resType2.getFieldNames();

        set2 = (sr[i][1].asSet());
        for (final Object o : set2) {
          Struct stc2 = (Struct) o;
          valPf2 = stc2.get(strg2[0]);
          valPos2 = stc2.get(strg2[1]);
          isActive2 = ((Portfolio) stc2.get(strg2[0])).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    if ((resType1).equals(resType2)) {
      CacheUtils.log("Both Search Results are of the same Type i.e.--> " + resType1);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }
    if (resSize1 == resSize2 || resSize1 != 0) {
      CacheUtils
          .log("Search Results size is Non Zero and equal in both cases i.e.  Size= " + resSize1);
    } else {
      fail("FAILED:Search result size is different in both the cases");
    }
    itert2 = set2.iterator();
    itert1 = set1.iterator();
    while (itert1.hasNext()) {
      Struct stc2 = (Struct) itert2.next();
      Struct stc1 = (Struct) itert1.next();
      if (stc2.get(strg2[0]) != stc1.get(strg1[0])) {
        fail(
            "FAILED: In both the Cases the first member of StructSet i.e. Portfolio are different. ");
      }
      if (stc2.get(strg2[1]) != stc1.get(strg1[1])) {
        fail("FAILED: In both the cases Positions are different");
      }
      if (!StringUtils.equals(((Position) stc2.get(strg2[1])).secId,
          ((Position) stc1.get(strg1[1])).secId)) {
        fail("FAILED: In both the cases Positions secIds are different");
      }
      if (((Portfolio) stc2.get(strg2[0])).isActive() != ((Portfolio) stc1.get(strg1[0]))
          .isActive()) {
        fail("FAILED: Status of the Portfolios found are different");
      }
      if (((Portfolio) stc2.get(strg2[0])).getID() != ((Portfolio) stc1.get(strg1[0])).getID()) {
        fail("FAILED: IDs of the Portfolios found are different");
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }

  @Test
  public void testNoIndexSkipping() throws Exception {

    Region region = CacheUtils.createRegion("pos", Portfolio.class);
    for (int i = 0; i < 400; i++) {
      region.put("" + i, new Portfolio(i));
    }
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries = {
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos pf,  positions.values pos where pf.ID > 0 and pf.ID < 250  and pf.status='active' and  pos.secId != null "};
    SelectResults[][] sr = new SelectResults[queries.length][2];
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i][0] = (SelectResults) q.execute();
        if (!observer.isIndexesUsed) {
          CacheUtils.log("NO INDEX USED");
        } else {
          fail("How could index be present when not created!?");
        }
        // CacheUtils.log(Utils.printResult(r));
        resType1 = (StructType) sr[i][0].getCollectionType().getElementType();
        resSize1 = (sr[i][0].size());
        CacheUtils.log(resType1);
        strg1 = resType1.getFieldNames();

        set1 = (sr[i][0].asSet());
        for (final Object o : set1) {
          Struct stc1 = (Struct) o;
          valPf1 = stc1.get(strg1[0]);
          valPos1 = stc1.get(strg1[1]);
          isActive1 = ((Portfolio) stc1.get(strg1[0])).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    Index index1 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "pos pf");
    Index index2 = qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "pos pf, pf.positions.values pos");
    Index index3 = qs.createIndex("IDIndex", IndexType.FUNCTIONAL, "pf.ID", SEPARATOR + "pos pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        sr[i][1] = (SelectResults) q.execute();
        if (!observer2.isIndexesUsed) {
          fail("FAILED: Index NOT Used");
        }
        assertEquals(observer2.indexesUsed.size(), 1);

        resType2 = (StructType) sr[i][1].getCollectionType().getElementType();
        resSize2 = (sr[i][1].size());
        CacheUtils.log(resType2);
        strg2 = resType2.getFieldNames();

        set2 = (sr[i][1].asSet());
        for (final Object o : set2) {
          Struct stc2 = (Struct) o;
          valPf2 = stc2.get(strg2[0]);
          valPos2 = stc2.get(strg2[1]);
          isActive2 = ((Portfolio) stc2.get(strg2[0])).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    if ((resType1).equals(resType2)) {
      CacheUtils.log("Both Search Results are of the same Type i.e.--> " + resType1);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }
    if (resSize1 == resSize2 || resSize1 != 0) {
      CacheUtils
          .log("Search Results size is Non Zero and equal in both cases i.e.  Size= " + resSize1);
    } else {
      fail("FAILED:Search result size is different in both the cases");
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }

  class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    @Override
    public void beforeIndexLookup(Index index, int lowerBoundOperator, Object lowerBoundKey,
        int upperBoundOperator, Object upperBoundKey, Set NotEqualKeys) {
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }

  /**
   * Test index usage on PR region & Local region if the total number of indexes created are more
   * than the fields used in the where clause of the query and the number of from clause iterators
   * are two
   */
  @Test
  public void testIndexUsageIfIndexesGreaterThanFieldsInQueryWhereClauseWithTwoIterators()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setValueConstraint(Portfolio.class);
    RegionAttributes ra = af.createRegionAttributes();
    Region region = CacheUtils.getCache().createRegion("pos", ra);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
    executeQuery(region, true /* chcek referential integrity */);
    region.destroyRegion();
    CacheUtils.closeCache();
    CacheUtils.restartCache();
    af = new AttributesFactory();
    af.setValueConstraint(Portfolio.class);

    PartitionAttributesFactory pfa = new PartitionAttributesFactory();

    pfa.setRedundantCopies(0);
    pfa.setTotalNumBuckets(1);
    af.setPartitionAttributes(pfa.create());
    ra = af.createRegionAttributes();
    region = CacheUtils.getCache().createRegion("pos", ra);

    for (int i = 0; i < 4; i++) {
      Portfolio x = new Portfolio(i);
      region.put("" + i, x);
    }
    executeQuery(region, false/* chcek referential integrity */);
    region.destroyRegion();

  }


  private void executeQuery(Region region, boolean checkReferentialIntegrity) throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries = {
        "SELECT DISTINCT pf FROM " + SEPARATOR
            + "pos pf,  positions.values pos where pf.description = 'XXXX'  and pos.secId= 'IBM' "};
    SelectResults[][] sr = new SelectResults[queries.length][2];

    ObjectType resType1 = null, resType2 = null;
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i][0] = (SelectResults) q.execute();
        if (!observer.isIndexesUsed) {
          CacheUtils.log("NO INDEX USED");
        } else {
          fail("How did Index get created?");
        }
        // CacheUtils.log(Utils.printResult(r));
        resType1 = sr[i][0].getCollectionType().getElementType();
        resSize1 = (sr[i][0].size());
        CacheUtils.log(resType1);
        set1 = (sr[i][0].asSet());
        for (final Object o : set1) {
          valPf1 = o;
          isActive1 = ((Portfolio) valPf1).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    Index index1 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "pos pf");
    Index index2 = qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "pos pf, pf.positions.values pos");
    Index index3 =
        qs.createIndex("descriptionIndex", IndexType.FUNCTIONAL, "pf.description",
            SEPARATOR + "pos pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        sr[i][1] = (SelectResults) q.execute();
        if (observer2.isIndexesUsed) {
          assertEquals(1, observer2.indexesUsed.size());
        } else {
          fail("FAILED: Index NOT Used");
        }
        resType2 = sr[i][1].getCollectionType().getElementType();
        resSize2 = (sr[i][1].size());
        CacheUtils.log(resType2);
        // strg2=resType2.getFieldNames();

        set2 = (sr[i][1].asSet());
        for (final Object o : set2) {
          valPf2 = o;
          // valPf2=stc2.get(strg2[0]);
          // valPos2=stc2.get(strg2[1]);
          isActive2 = ((Portfolio) valPf2).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    if ((resType1).equals(resType2)) {
      CacheUtils.log("Both Search Results are of the same Type i.e.--> " + resType1);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }
    if (resSize1 == resSize2 || resSize1 != 0) {
      CacheUtils
          .log("Search Results size is Non Zero and equal in both cases i.e.  Size= " + resSize1);
    } else {
      fail("FAILED:Search result size is different in both the cases");
    }
    itert2 = set2.iterator();
    itert1 = set1.iterator();
    while (itert1.hasNext()) {
      Object pos2 = itert2.next();
      Object pos1 = itert1.next();
      Object posFromRegion = region.get(((Portfolio) pos1).getPk());
      if (!pos1.equals(pos2)) {
        fail(
            "FAILED: In both the Cases the first member of StructSet i.e. Portfolio are different. ");
      }
      if (checkReferentialIntegrity) {
        assertTrue(pos2 == pos1);
        assertTrue(pos2 == posFromRegion);
      }
      if (((Portfolio) pos2).isActive() != ((Portfolio) pos1).isActive()) {
        fail("FAILED: Status of the Portfolios found are different");
      }
      if (((Portfolio) pos2).getID() != ((Portfolio) pos1).getID()) {
        fail("FAILED: IDs of the Portfolios found are different");
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);

  }

  /**
   * Test index usage on PR region & Local region if the total number of indexes created are more
   * than the fields used in the where clause of the query and the number of from clause iterators
   * is One
   */
  @Test
  public void testIndexUsageIfIndexesGreaterThanFieldsInQueryWhereClauseWithOneIterator()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setValueConstraint(Portfolio.class);
    RegionAttributes ra = af.createRegionAttributes();
    Region region = CacheUtils.getCache().createRegion("pos", ra);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
    executeQuery_1(region, true /* chcek referential integrity */);
    region.destroyRegion();
    CacheUtils.closeCache();
    CacheUtils.restartCache();
    af = new AttributesFactory();
    af.setValueConstraint(Portfolio.class);

    PartitionAttributesFactory pfa = new PartitionAttributesFactory();

    pfa.setRedundantCopies(0);
    pfa.setTotalNumBuckets(1);
    af.setPartitionAttributes(pfa.create());
    ra = af.createRegionAttributes();
    region = CacheUtils.getCache().createRegion("pos", ra);

    for (int i = 0; i < 4; i++) {
      Portfolio x = new Portfolio(i);
      region.put("" + i, x);
    }
    executeQuery_1(region, false/* chcek referential integrity */);
    region.destroyRegion();

  }


  private void executeQuery_1(Region region, boolean checkReferentialIntegrity) throws Exception {
    // Region region = CacheUtils.createRegion("pos", Portfolio.class);

    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries =
        {"SELECT DISTINCT * FROM " + SEPARATOR
            + "pos pf where pf.description = 'XXXX'  and pf.status='active' "};
    SelectResults[][] sr = new SelectResults[queries.length][2];

    ObjectType resType1 = null, resType2 = null;
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i][0] = (SelectResults) q.execute();

        if (!observer.isIndexesUsed) {
          CacheUtils.log("NO INDEX USED");
        } else {
          fail("How did Index get created?");
        }
        // CacheUtils.log(Utils.printResult(r));
        resType1 = sr[i][0].getCollectionType().getElementType();
        resSize1 = (sr[i][0].size());
        CacheUtils.log(resType1);
        // strg1=resType1.getFieldNames();

        set1 = (sr[i][0].asSet());
        for (final Object o : set1) {
          valPf1 = o;
          // valPf1 = stc1.get(strg1[0]);
          // valPos1 = stc1.get(strg1[1]);
          isActive1 = ((Portfolio) valPf1).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    Index index1 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "pos pf");
    Index index2 =
        qs.createIndex("IdIndex", IndexType.FUNCTIONAL, "pf.iD", SEPARATOR + "pos pf");
    Index index3 =
        qs.createIndex("descriptionIndex", IndexType.FUNCTIONAL, "pf.description",
            SEPARATOR + "pos pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        sr[i][1] = (SelectResults) q.execute();
        if (observer2.isIndexesUsed) {
          assertEquals(1, observer2.indexesUsed.size());
        } else {
          fail("FAILED: Index NOT Used");
        }
        resType2 = sr[i][1].getCollectionType().getElementType();
        resSize2 = (sr[i][1].size());
        CacheUtils.log(resType2);
        // strg2=resType2.getFieldNames();

        set2 = (sr[i][1].asSet());
        for (final Object o : set2) {
          valPf2 = o;
          // valPf2=stc2.get(strg2[0]);
          // valPos2=stc2.get(strg2[1]);
          isActive2 = ((Portfolio) valPf2).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    if ((resType1).equals(resType2)) {
      CacheUtils.log("Both Search Results are of the same Type i.e.--> " + resType1);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }
    if (resSize1 == resSize2 || resSize1 != 0) {
      CacheUtils
          .log("Search Results size is Non Zero and equal in both cases i.e.  Size= " + resSize1);
    } else {
      fail("FAILED:Search result size is different in both the cases");
    }
    itert2 = set2.iterator();
    itert1 = set1.iterator();
    while (itert1.hasNext()) {
      Object pos2 = itert2.next();
      Object pos1 = itert1.next();
      Object posFromRegion = region.get(((Portfolio) pos1).getPk());
      if (!pos1.equals(pos2)) {
        fail(
            "FAILED: In both the Cases the first member of StructSet i.e. Portfolio are different. ");
      }
      if (checkReferentialIntegrity) {
        assertTrue(pos2 == pos1);
        assertTrue(pos2 == posFromRegion);
      }
      /*
       * if( pos1 != pos2 )
       * fail("FAILED: In both the Cases the first member of StructSet i.e. Portfolio are different. "
       * );
       */

      /*
       * if(stc2.get(strg2[1]) != stc1.get(strg1[1]))
       * fail("FAILED: In both the cases Positions are different");
       * if(((Position)stc2.get(strg2[1])).secId != ((Position)stc1.get(strg1[1])).secId)
       * fail("FAILED: In both the cases Positions secIds are different");
       */
      if (((Portfolio) pos2).isActive() != ((Portfolio) pos1).isActive()) {
        fail("FAILED: Status of the Portfolios found are different");
      }
      if (((Portfolio) pos2).getID() != ((Portfolio) pos1).getID()) {
        fail("FAILED: IDs of the Portfolios found are different");
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);

  }

  /**
   * BugTest for Bug # 38032 Test index usage on PR region & Local region if the where clause
   * contains three conditions but only one condition is indexed & the total number of from clause
   * iterators is 1.
   *
   */
  @Test
  public void testIndexUsageIfOneFieldIndexedAndMoreThanOneUnindexed_Bug38032() throws Exception {

    AttributesFactory af = new AttributesFactory();
    af.setValueConstraint(Portfolio.class);
    RegionAttributes ra = af.createRegionAttributes();
    Region region = CacheUtils.getCache().createRegion("pos", ra);
    for (int i = 0; i < 5; i++) {
      region.put("" + i, new Portfolio(i));
    }
    // As default generation of Portfolio objects
    // The status is active for key =0 & key =2 & key =4
    // The description is XXXX for key =1, key =3
    // Let us make explitly
    // Set createTime as 5 for three Portfolio objects of key =0 & key =1 & key=2;
    // Description as XXXX for key =2 only.

    // Thus out of 4 objects created only 1 object ( key =2 ) will have
    // description = XXXX , status = active & time = 5

    ((Portfolio) region.get("0")).setCreateTime(5);
    ((Portfolio) region.get("1")).setCreateTime(5);
    ((Portfolio) region.get("2")).setCreateTime(5);
    ((Portfolio) region.get("2")).description = "XXXX";
    int numSatisfyingOurCond = 0;
    for (int i = 0; i < 5; i++) {
      Portfolio pf = (Portfolio) region.get("" + i);
      if (pf.description != null && pf.description.equals("XXXX") && pf.getCreateTime() == 5
          && pf.isActive()) {
        ++numSatisfyingOurCond;
      }
    }

    assertEquals(1, numSatisfyingOurCond);
    executeQuery_2(region, true /* chcek referential integrity */);
    region.destroyRegion();
    CacheUtils.closeCache();
    CacheUtils.restartCache();
    af = new AttributesFactory();
    af.setValueConstraint(Portfolio.class);

    PartitionAttributesFactory pfa = new PartitionAttributesFactory();

    pfa.setRedundantCopies(0);
    pfa.setTotalNumBuckets(1);
    af.setPartitionAttributes(pfa.create());
    ra = af.createRegionAttributes();
    region = CacheUtils.getCache().createRegion("pos", ra);


    for (int i = 0; i < 5; i++) {
      region.put("" + i, new Portfolio(i));
    }
    // As default generation of Portfolio objects
    // The status is active for key =0 & key =2 & key =4
    // The description is XXXX for key =1, key =3
    // Let us make explitly
    // Set createTime as 5 for three Portfolio objects of key =0 & key =1 & key=2;
    // Description as XXXX for key =2 only.

    // Thus out of 4 objects created only 1 object ( key =2 ) will have
    // description = XXXX , status = active & time = 5

    Portfolio x = (Portfolio) region.get("0");
    x.setCreateTime(5);
    region.put("0", x);
    x = (Portfolio) region.get("1");
    x.setCreateTime(5);
    region.put("1", x);
    x = (Portfolio) region.get("2");
    x.setCreateTime(5);
    x.description = "XXXX";
    region.put("2", x);
    numSatisfyingOurCond = 0;
    for (int i = 0; i < 5; i++) {
      Portfolio pf = (Portfolio) region.get("" + i);
      if (pf.description != null && pf.description.equals("XXXX") && pf.getCreateTime() == 5
          && pf.isActive()) {
        ++numSatisfyingOurCond;
      }
    }

    assertEquals(1, numSatisfyingOurCond);
    executeQuery_2(region, false/* chcek referential integrity */);
    region.destroyRegion();

  }

  private void executeQuery_2(Region region, boolean checkReferentialIntegrity) throws Exception {
    // Region region = CacheUtils.createRegion("pos", Portfolio.class);

    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries = {
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos pf where pf.description = 'XXXX'  and pf.status='active' and pf.createTime = 5 "};
    SelectResults[][] sr = new SelectResults[queries.length][2];
    ObjectType resType1 = null, resType2 = null;
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i][0] = (SelectResults) q.execute();
        if (!observer.isIndexesUsed) {
          CacheUtils.log("NO INDEX USED");
        } else {
          fail("How did Index get created!!1?");
        }
        // CacheUtils.log(Utils.printResult(r));
        resType1 = sr[i][0].getCollectionType().getElementType();
        resSize1 = (sr[i][0].size());
        CacheUtils.log(resType1);
        assertEquals(1, resSize1);
        set1 = (sr[i][0].asSet());
        for (final Object o : set1) {
          valPf1 = o;
          isActive1 = ((Portfolio) valPf1).isActive();
          assertTrue(isActive1);
          assertEquals("XXXX", ((Portfolio) valPf1).description);
          assertEquals(5, ((Portfolio) valPf1).getCreateTime());

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    Index index1 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "pos pf");
    // Index index2 = (Index)qs.createIndex("IdIndex", IndexType.FUNCTIONAL,"pf.iD","/pos pf");
    // Index index3 = qs.createIndex("descriptionIndex", IndexType.FUNCTIONAL,"pf.description","/pos
    // pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        sr[i][1] = (SelectResults) q.execute();
        if (observer2.isIndexesUsed) {
          assertEquals(1, observer2.indexesUsed.size());
        } else {
          fail("FAILED: Index NOT Used");
        }
        resType2 = sr[i][1].getCollectionType().getElementType();
        resSize2 = (sr[i][1].size());
        CacheUtils.log(resType2);
        // strg2=resType2.getFieldNames();

        set2 = (sr[i][1].asSet());
        for (final Object o : set2) {
          valPf2 = o;
          // valPf2=stc2.get(strg2[0]);
          // valPos2=stc2.get(strg2[1]);
          isActive2 = ((Portfolio) valPf2).isActive();
          assertTrue(isActive2);
          assertEquals("XXXX", ((Portfolio) valPf2).description);
          assertEquals(5, ((Portfolio) valPf2).getCreateTime());


        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    if ((resType1).equals(resType2)) {
      CacheUtils.log("Both Search Results are of the same Type i.e.--> " + resType1);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }
    if (resSize1 == resSize2 || resSize1 != 0) {
      CacheUtils
          .log("Search Results size is Non Zero and equal in both cases i.e.  Size= " + resSize1);
    } else {
      fail("FAILED:Search result size is different in both the cases");
    }
    itert2 = set2.iterator();
    itert1 = set1.iterator();
    while (itert1.hasNext()) {
      Object pos2 = itert2.next();
      Object pos1 = itert1.next();
      Object posFromRegion = region.get(((Portfolio) pos1).getPk());
      if (!pos1.equals(pos2)) {
        fail(
            "FAILED: In both the Cases the first member of StructSet i.e. Portfolio are different. ");
      }
      if (checkReferentialIntegrity) {
        assertTrue(pos2 == pos1);
        assertTrue(pos2 == posFromRegion);
      }
      if (((Portfolio) pos2).isActive() != ((Portfolio) pos1).isActive()) {
        fail("FAILED: Status of the Portfolios found are different");
      }
      if (((Portfolio) pos2).getID() != ((Portfolio) pos1).getID()) {
        fail("FAILED: IDs of the Portfolios found are different");
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);

  }


  /**
   * Test index usage on PR region & Local region if the where clause contains three conditions with
   * two conditions indexed & the total number of from clause iterators is 1.
   */
  @Test
  public void testIndexUsageIfTwoFieldsIndexedAndOneUnindexed() throws Exception {

    AttributesFactory af = new AttributesFactory();
    af.setValueConstraint(Portfolio.class);
    RegionAttributes ra = af.createRegionAttributes();
    Region region = CacheUtils.getCache().createRegion("pos", ra);
    for (int i = 0; i < 5; i++) {
      region.put("" + i, new Portfolio(i));
    }
    // As default generation of Portfolio objects
    // The status is active for key =0 & key =2 & key =4
    // The description is XXXX for key =1, key =3
    // Let us make explitly
    // Set createTime as 5 for three Portfolio objects of key =0 & key =1 & key=2;
    // Description as XXXX for key =2 only.

    // Thus out of 4 objects created only 1 object ( key =2 ) will have
    // description = XXXX , status = active & time = 5

    ((Portfolio) region.get("0")).setCreateTime(5);
    ((Portfolio) region.get("1")).setCreateTime(5);
    ((Portfolio) region.get("2")).setCreateTime(5);
    ((Portfolio) region.get("2")).description = "XXXX";
    int numSatisfyingOurCond = 0;
    for (int i = 0; i < 5; i++) {
      Portfolio pf = (Portfolio) region.get("" + i);
      if (pf.description != null && pf.description.equals("XXXX") && pf.getCreateTime() == 5
          && pf.isActive()) {
        ++numSatisfyingOurCond;
      }
    }

    assertEquals(1, numSatisfyingOurCond);
    executeQuery_3(region, true /* chcek referential integrity */);
    region.destroyRegion();
    CacheUtils.closeCache();
    CacheUtils.restartCache();
    af = new AttributesFactory();
    af.setValueConstraint(Portfolio.class);

    PartitionAttributesFactory pfa = new PartitionAttributesFactory();

    pfa.setRedundantCopies(0);
    pfa.setTotalNumBuckets(1);
    af.setPartitionAttributes(pfa.create());
    ra = af.createRegionAttributes();
    region = CacheUtils.getCache().createRegion("pos", ra);


    for (int i = 0; i < 5; i++) {
      region.put("" + i, new Portfolio(i));
    }
    // As default generation of Portfolio objects
    // The status is active for key =0 & key =2 & key =4
    // The description is XXXX for key =1, key =3
    // Let us make explitly
    // Set createTime as 5 for three Portfolio objects of key =0 & key =1 & key=2;
    // Description as XXXX for key =2 only.

    // Thus out of 4 objects created only 1 object ( key =2 ) will have
    // description = XXXX , status = active & time = 5

    Portfolio x = (Portfolio) region.get("0");
    x.setCreateTime(5);
    region.put("0", x);
    x = (Portfolio) region.get("1");
    x.setCreateTime(5);
    region.put("1", x);
    x = (Portfolio) region.get("2");
    x.setCreateTime(5);
    x.description = "XXXX";
    region.put("2", x);
    numSatisfyingOurCond = 0;
    for (int i = 0; i < 5; i++) {
      Portfolio pf = (Portfolio) region.get("" + i);
      if (pf.description != null && pf.description.equals("XXXX") && pf.getCreateTime() == 5
          && pf.isActive()) {
        ++numSatisfyingOurCond;
      }
    }

    assertEquals(1, numSatisfyingOurCond);
    executeQuery_3(region, false/* chcek referential integrity */);
    region.destroyRegion();

  }

  private void executeQuery_3(Region region, boolean checkReferentialIntegrity) throws Exception {
    // Region region = CacheUtils.createRegion("pos", Portfolio.class);

    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries = {
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos pf where pf.description = 'XXXX'  and pf.status='active' and pf.createTime = 5 "};
    ObjectType resType1 = null, resType2 = null;
    SelectResults[][] sr = new SelectResults[queries.length][2];
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i][0] = (SelectResults) q.execute();
        if (!observer.isIndexesUsed) {
          CacheUtils.log("NO INDEX USED");
        }
        // CacheUtils.log(Utils.printResult(r));
        resType1 = sr[i][0].getCollectionType().getElementType();
        resSize1 = (sr[i][0].size());
        CacheUtils.log(resType1);
        assertEquals(1, resSize1);
        set1 = (sr[i][0].asSet());
        for (final Object o : set1) {
          valPf1 = o;
          isActive1 = ((Portfolio) valPf1).isActive();
          assertTrue(isActive1);
          assertEquals("XXXX", ((Portfolio) valPf1).description);
          assertEquals(5, ((Portfolio) valPf1).getCreateTime());

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    Index index1 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "pos pf");
    Index index2 =
        qs.createIndex("IdIndex", IndexType.FUNCTIONAL, "pf.iD", SEPARATOR + "pos pf");
    Index index3 =
        qs.createIndex("descriptionIndex", IndexType.FUNCTIONAL, "pf.description",
            SEPARATOR + "pos pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        sr[i][1] = (SelectResults) q.execute();
        if (observer2.isIndexesUsed) {
          assertEquals(1, observer2.indexesUsed.size());
        } else {
          fail("FAILED: Index NOT Used");
        }
        resType2 = sr[i][1].getCollectionType().getElementType();
        resSize2 = (sr[i][1].size());
        // strg2=resType2.getFieldNames();

        set2 = (sr[i][1].asSet());
        for (final Object o : set2) {
          valPf2 = o;
          // valPf2=stc2.get(strg2[0]);
          // valPos2=stc2.get(strg2[1]);
          isActive2 = ((Portfolio) valPf2).isActive();
          assertTrue(isActive2);
          assertEquals("XXXX", ((Portfolio) valPf2).description);
          assertEquals(5, ((Portfolio) valPf2).getCreateTime());


        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    if ((resType1).equals(resType2)) {
      CacheUtils.log("Both Search Results are of the same Type i.e.--> " + resType1);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }
    if (resSize1 == resSize2 || resSize1 != 0) {
      CacheUtils
          .log("Search Results size is Non Zero and equal in both cases i.e.  Size= " + resSize1);
    } else {
      fail("FAILED:Search result size is different in both the cases");
    }
    itert2 = set2.iterator();
    itert1 = set1.iterator();
    while (itert1.hasNext()) {
      Object pos2 = itert2.next();
      Object pos1 = itert1.next();
      Object posFromRegion = region.get(((Portfolio) pos1).getPk());
      if (!pos1.equals(pos2)) {
        fail(
            "FAILED: In both the Cases the first member of StructSet i.e. Portfolio are different. ");
      }
      if (checkReferentialIntegrity) {
        assertTrue(pos2 == pos1);
        assertTrue(pos2 == posFromRegion);
      }
      if (((Portfolio) pos2).isActive() != ((Portfolio) pos1).isActive()) {
        fail("FAILED: Status of the Portfolios found are different");
      }
      if (((Portfolio) pos2).getID() != ((Portfolio) pos1).getID()) {
        fail("FAILED: IDs of the Portfolios found are different");
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);

  }

  @Test
  public void testNonDistinctOrCondResults() throws Exception {


    Region region = CacheUtils.createRegion("pos", Portfolio.class);
    for (int i = 0; i < 10; i++) {
      region.put("" + i, new Portfolio(i));
    }

    for (int i = 10; i < 20; i++) {
      Portfolio p = new Portfolio(i);
      if (i % 2 == 0) {
        p.status = null;
      }
      region.put("" + i, p);
    }
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries =
        {"SELECT * FROM " + SEPARATOR
            + "pos pf,  positions.values pos where pf.ID > 0 OR pf.status='active' OR  pos.secId != 'IBM'",
            "SELECT * FROM " + SEPARATOR + "pos pf where pf.ID > 0 OR pf.status='active'",
            "SELECT * FROM " + SEPARATOR + "pos pf where pf.ID > 0 OR pf.status LIKE 'act%'",
            "SELECT * FROM " + SEPARATOR
                + "pos pf where pf.ID > 0 OR pf.status IN SET('active', 'inactive')",};

    SelectResults[] sr = new SelectResults[queries.length];
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i] = (SelectResults) q.execute();
        if (observer.isIndexesUsed) {
          fail("How could index be present when not created!?");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Verify Results
    for (int i = 0; i < sr.length; i++) {
      Set resultSet = sr[i].asSet();

      // Check Element Type
      if (queries[i].contains("values")) {
        assertTrue("Checking Element type for Query: [" + queries[i] + "] results",
            sr[i].getCollectionType().getElementType().toString().equals(
                "struct<pf:org.apache.geode.cache.query.data.Portfolio,pos:java.lang.Object>"));
        // Check Size of results
        assertEquals("Checking Element type for Query: [" + queries[i] + "] results", 40,
            resultSet.size());
      } else {
        assertTrue("Checking Element type for Query: [" + queries[i] + "] results",
            sr[i].getCollectionType().getElementType().toString()
                .equals("org.apache.geode.cache.query.data.Portfolio"));
        // Check Size of results
        assertEquals("Checking Element type for Query: [" + queries[i] + "] results", 20,
            resultSet.size());
      }

      for (final Object obj : resultSet) {
        if (sr[i].getCollectionType().getElementType().toString().equals(
            "struct<pf:org.apache.geode.cache.query.data.Portfolio,pos:java.lang.Object>")) {
          Object[] values = ((Struct) obj).getFieldValues();
          Portfolio port = (Portfolio) values[0];
          Position pos = (Position) values[1];
          if (!(port.getID() > 0 || port.status.equals("active") || pos.secId.equals("IBM"))) {
            fail("Result object" + obj
                + " failed to satisfy all OR conditions of where clause of query " + queries[i]);
          }
        } else {
          Portfolio port = (Portfolio) obj;
          if (!(port.getID() > 0 || port.status.equals("active"))) {
            fail("Result object" + port
                + " failed to satisfy all OR conditions of where clause of query " + queries[i]);
          }
        }
      }
    }
  }

  /**
   * Test verifies the result of query with 'OR' conditions when separate indexes are used for two
   * comparisons of OR clause. Like, "select * from /portfolio where ID > 0 OR status = 'active'"
   * and we have indexes on ID and status both. Both indexes must be used.
   */
  @Test
  public void testCorrectOrCondResultWithMultiIndexes() throws Exception {

    Region region = CacheUtils.createRegion("pos", Portfolio.class);
    for (int i = 0; i < 10; i++) {
      region.put("" + i, new Portfolio(i));
    }

    for (int i = 10; i < 20; i++) {
      Portfolio p = new Portfolio(i);
      if (i % 2 == 0) {
        p.status = null;
      }
      region.put("" + i, p);
    }
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries =
        {"SELECT * FROM " + SEPARATOR
            + "pos pf,  positions.values pos where pf.ID > 0 OR pf.status='active' OR  pos.secId != 'IBM'",
            "SELECT * FROM " + SEPARATOR + "pos pf where pf.ID > 0 OR pf.status='active'",
            "SELECT * FROM " + SEPARATOR + "pos pf where pf.ID > 0 OR pf.status LIKE 'act%'",
            "SELECT DISTINCT * FROM " + SEPARATOR
                + "pos pf where pf.ID > 0 OR pf.status='active' ORDER BY pf.status desc LIMIT 10",
            "SELECT * FROM " + SEPARATOR
                + "pos pf where pf.ID > 0 OR pf.status IN SET('active', 'inactive')",};

    SelectResults[][] sr = new SelectResults[queries.length][2];
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i][0] = (SelectResults) q.execute();
        if (observer.isIndexesUsed) {
          fail("How could index be present when not created!?");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    Index index1 =
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "pos pf");
    Index index2 = qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "pos pf, pf.positions.values pos");
    Index index3 = qs.createIndex("IDIndex", IndexType.FUNCTIONAL, "pf.ID", SEPARATOR + "pos pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        sr[i][1] = (SelectResults) q.execute();
        if (!observer2.isIndexesUsed && !queries[i].contains("LIKE")) {
          fail("FAILED: Index NOT Used for query : " + q.getQueryString());
        } else if (!queries[i].contains("LIKE")) {
          assertTrue(observer2.indexesUsed.size() >= 2);
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
}
