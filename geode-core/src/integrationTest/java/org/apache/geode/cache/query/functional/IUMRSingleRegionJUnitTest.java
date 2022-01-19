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
 * IUMRSingleRegionJUnitTest.java
 *
 * Created on October 3, 2005, 1:20 PM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.City;
import org.apache.geode.cache.query.data.Country;
import org.apache.geode.cache.query.data.District;
import org.apache.geode.cache.query.data.State;
import org.apache.geode.cache.query.data.Village;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IUMRSingleRegionJUnitTest {

  private static final String indexName = "queryTest";

  private Cache cache;
  private Region region;
  private Index index;
  private DistributedSystem ds;
  private QueryService qs;

  private final StructType resType1 = null;
  private final StructType resType2 = null;

  private final int resSize1 = 0;
  private final int resSize2 = 0;

  private final Iterator itert1 = null;
  private final Iterator itert2 = null;

  private final Set set1 = null;
  private final Set set2 = null;

  // ////////////// queries ////////////////
  private final String[] queries = {
      // Query 1
      "SELECT DISTINCT * FROM " + SEPARATOR + "Countries c, c.states s, s.districts d,"
          + " d.villages v, d.cities ct WHERE v.name = 'MAHARASHTRA_VILLAGE1'",
      // Query 2
      "SELECT DISTINCT * FROM " + SEPARATOR
          + "Countries c, c.states s, s.districts d, d.villages v,"
          + " d.cities ct WHERE v.name='MAHARASHTRA_VILLAGE1' AND ct.name = 'PUNE'",
      // Query 3
      "SELECT DISTINCT * FROM " + SEPARATOR
          + "Countries c, c.states s, s.districts d, d.villages v, "
          + "d.cities ct WHERE ct.name = 'PUNE' AND s.name = 'MAHARASHTRA'",
      // Query 4a & 4b
      "SELECT DISTINCT * FROM " + SEPARATOR + "Countries c WHERE c.name = 'INDIA'",
      "SELECT DISTINCT * FROM " + SEPARATOR
          + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v WHERE c.name = 'INDIA'",
      // Query 5
      "SELECT DISTINCT * FROM " + SEPARATOR
          + "Countries c, c.states s, s.districts d WHERE d.name = 'PUNEDIST' AND s.name = 'GUJARAT'",
      // Query 6
      "SELECT DISTINCT * FROM " + SEPARATOR
          + "Countries c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI'",
      // Query 7
      "SELECT DISTINCT c.name, s.name, d.name, ct.name FROM " + SEPARATOR
          + "Countries c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI' OR ct.name = 'CHENNAI'",
      // Query 8
      "SELECT DISTINCT c.name, s.name FROM " + SEPARATOR
          + "Countries c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI' OR s.name = 'GUJARAT'",
      // Query 9a & 9b
      "SELECT DISTINCT c.name, s.name, ct.name FROM " + SEPARATOR
          + "Countries c, c.states s, (SELECT DISTINCT * FROM "
          + SEPARATOR
          + "Countries c, c.states s, s.districts d, d.cities ct WHERE s.name = 'PUNJAB') itr1, "
          + "s.districts d, d.cities ct WHERE ct.name = 'CHANDIGARH'",
      "SELECT DISTINCT c.name, s.name, ct.name FROM " + SEPARATOR
          + "Countries c, c.states s, s.districts d,"
          + " d.cities ct WHERE ct.name = (SELECT DISTINCT ct.name FROM " + SEPARATOR
          + "Countries c, c.states s, "
          + "s.districts d, d.cities ct WHERE s.name = 'MAHARASHTRA' AND ct.name = 'PUNE')",
      // Query 10
      "SELECT DISTINCT c.name, s.name, ct.name FROM " + SEPARATOR
          + "Countries c, c.states s, s.districts d, "
          + "d.cities ct, d.getVillages() v WHERE v.getName() = 'PUNJAB_VILLAGE1'",
      // Query 11
      "SELECT DISTINCT s.name, s.getDistricts(), ct.getName() FROM " + SEPARATOR
          + "Countries c, c.getStates() s, "
          + "s.getDistricts() d, d.getCities() ct WHERE ct.getName() = 'PUNE' OR ct.name = 'CHANDIGARH' "
          + "OR s.getName() = 'GUJARAT'",
      // Query 12
      "SELECT DISTINCT d.getName(), d.getCities(), d.getVillages() FROM " + SEPARATOR
          + "Countries c, "
          + "c.states s, s.districts d WHERE d.name = 'MUMBAIDIST'",

  };

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
    /* create region containing Country objects */
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Country.class);
    region = cache.createRegion("Countries", factory.create());

    populateData();
  }// end of setUp

  @After
  public void tearDown() throws Exception {
    if (ds != null) {
      ds.disconnect();
    }
  }// end of tearDown

  @Test
  public void testChangedFormClauseOrder1() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR003
    String sqlStr =
        "SELECT DISTINCT * FROM " + SEPARATOR + "Countries c, c.states s, s.districts d,"
            + " d.villages v, d.cities ct WHERE v.name = 'MAHARASHTRA_VILLAGE1'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("villageName", itr.next().toString());

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testChangedFormClauseOrder2() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR008
    String sqlStr = "SELECT DISTINCT * FROM " + SEPARATOR
        + "Countries c, c.states s, s.districts d, d.villages v,"
        + " d.cities ct WHERE v.name='MAHARASHTRA_VILLAGE1' AND ct.name = 'PUNE'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }
      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("villageName")) {
          break;
        } else if (temp.equals("cityName")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<villageName> and <cityName> were expected but found " + itr.next());
        }
      }

      areResultsMatching(rs, queries);

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testChangedFormClauseOrder3() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR009
    String sqlStr = "SELECT DISTINCT * FROM " + SEPARATOR
        + "Countries c, c.states s, s.districts d, d.villages v, "
        + "d.cities ct WHERE ct.name = 'PUNE' AND s.name = 'MAHARASHTRA'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }
      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("cityName")) {
          break;
        } else if (temp.equals("stateName")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<cityName> and <stateName> were expected but found " + itr.next());
        }
      }

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testSelectBestIndex1() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR010
    String sqlStr = "SELECT DISTINCT * FROM " + SEPARATOR + "Countries c WHERE c.name = 'INDIA'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("countryName2", itr.next().toString());

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testSelectBestIndex2() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR011
    String sqlStr =
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v WHERE c.name = 'INDIA'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("countryName1", itr.next().toString());

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testProjectionAttr1() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR012
    String sqlStr =
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Countries c, c.states s, s.districts d WHERE d.name = 'PUNEDIST' AND s.name = 'GUJARAT'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("districtName")) {
          break;
        } else if (temp.equals("stateName")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<districtName> and <stateName> were expected but found " + itr.next());
        }
      }

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testCutDown1() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR013
    String sqlStr =
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Countries c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("cityName", itr.next().toString());

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testCutDown2() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR014
    String sqlStr =
        "SELECT DISTINCT c.name, s.name, d.name, ct.name FROM " + SEPARATOR
            + "Countries c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI' OR ct.name = 'CHENNAI'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("cityName", itr.next().toString());

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testCutDown3() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR015
    String sqlStr =
        "SELECT DISTINCT c.name, s.name FROM " + SEPARATOR
            + "Countries c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI' OR s.name = 'GUJARAT'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("cityName")) {
          break;
        } else if (temp.equals("stateName")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<cityName> and <stateName> were expected but found " + itr.next());
        }
      }

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testSelectAsFromClause() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR016
    String sqlStr =
        "SELECT DISTINCT c.name, s.name, ct.name FROM " + SEPARATOR
            + "Countries c, c.states s, (SELECT DISTINCT * FROM "
            + SEPARATOR
            + "Countries c, c.states s, s.districts d, d.cities ct WHERE s.name = 'PUNJAB') itr1, "
            + "s.districts d, d.cities ct WHERE ct.name = 'CHANDIGARH'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("stateName")) {
          break;
        } else if (temp.equals("cityName")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<stateName> and <cityName> were expected but found " + itr.next());
        }
      }

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testSelectAsWhereClause() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR017
    String sqlStr =
        "SELECT DISTINCT c.name, s.name, ct.name FROM " + SEPARATOR
            + "Countries c, c.states s, s.districts d,"
            + " d.cities ct WHERE ct.name = element (SELECT DISTINCT ct.name FROM " + SEPARATOR
            + "Countries c, c.states s, "
            + "s.districts d, d.cities ct WHERE s.name = 'MAHARASHTRA' AND ct.name = 'PUNE')";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("stateName")) {
          break;
        } else if (temp.equals("cityName")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<stateName> and <cityName> were expected but found " + itr.next());
        }
      }

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testFunctionUse1() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR018
    String sqlStr =
        "SELECT DISTINCT c.name, s.name, ct.name FROM " + SEPARATOR
            + "Countries c, c.states s, s.districts d, "
            + "d.cities ct, d.getVillages() v WHERE v.getName() = 'PUNJAB_VILLAGE1'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("villageName", itr.next().toString());

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testFunctionUse2() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR019
    String sqlStr =
        "SELECT DISTINCT s.name, s.getDistricts(), ct.getName() FROM " + SEPARATOR
            + "Countries c, c.getStates() s, "
            + "s.getDistricts() d, d.getCities() ct WHERE ct.getName() = 'PUNE' OR ct.name = 'CHANDIGARH' "
            + "OR s.getName() = 'GUJARAT'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("cityName")) {
          break;
        } else if (temp.equals("stateName")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<cityName> and <stateName> were expected but found " + itr.next());
        }
      }

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  @Test
  public void testFunctionUse3() throws Exception {
    SelectResults[][] rs = new SelectResults[1][2];
    // Test Case No. IUMR020
    String sqlStr =
        "SELECT DISTINCT d.getName(), d.getCities(), d.getVillages() FROM " + SEPARATOR
            + "Countries c, "
            + "c.states s, s.districts d WHERE d.name = 'MUMBAIDIST'";

    // query execution without Index.
    Query q = null;
    try {
      QueryService qs1 = cache.getQueryService();
      q = qs1.newQuery(sqlStr);
      rs[0][0] = (SelectResults) q.execute();

      createIndex();
      QueryService qs2 = cache.getQueryService();// ????
      q = qs2.newQuery(sqlStr);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs[0][1] = (SelectResults) q.execute();

      if (!observer.isIndexesUsed) {
        fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q.getQueryString());
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("districtName", itr.next().toString());

      areResultsMatching(rs, new String[] {sqlStr});

    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }// end of test

  private static void areResultsMatching(SelectResults[][] rs, String[] queries) {
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(rs, 1, queries);

  }// end of areResultsMatching

  // ////////////// function to pupualte data in single region //////////////
  private void populateData() throws Exception {
    /* create villages */
    Village v1 = new Village("MAHARASHTRA_VILLAGE1", 123456);
    Village v2 = new Village("PUNJAB_VILLAGE1", 123789);
    Village v3 = new Village("KERALA_VILLAGE1", 456789);
    Village v4 = new Village("GUJARAT_VILLAGE1", 123478);
    Village v5 = new Village("AASAM_VILLAGE1", 783456);
    Set villages = new HashSet();
    villages.add(v1);
    villages.add(v2);
    villages.add(v3);
    villages.add(v4);
    villages.add(v5);

    /* create cities */
    City ct1 = new City("MUMBAI", 123456);
    City ct2 = new City("PUNE", 123789);
    City ct3 = new City("GANDHINAGAR", 456789);
    City ct4 = new City("CHANDIGARH", 123478);
    Set cities = new HashSet();
    cities.add(ct1);
    cities.add(ct2);
    cities.add(ct3);
    cities.add(ct4);

    /* create districts */
    District d1 = new District("MUMBAIDIST", cities, villages);
    District d2 = new District("PUNEDIST", cities, villages);
    District d3 = new District("GANDHINAGARDIST", cities, villages);
    District d4 = new District("CHANDIGARHDIST", cities, villages);
    Set districts = new HashSet();
    districts.add(d1);
    districts.add(d2);
    districts.add(d3);
    districts.add(d4);

    /* create states */
    State s1 = new State("MAHARASHTRA", "west", districts);
    State s2 = new State("PUNJAB", "north", districts);
    State s3 = new State("GUJARAT", "west", districts);
    State s4 = new State("KERALA", "south", districts);
    State s5 = new State("AASAM", "east", districts);
    Set states = new HashSet();
    states.add(s1);
    states.add(s2);
    states.add(s3);
    states.add(s4);
    states.add(s5);

    /* create countries */
    Country c1 = new Country("INDIA", "asia", states);
    Country c2 = new Country("ISRAEL", "africa", states);
    Country c3 = new Country("CANADA", "america", states);
    Country c4 = new Country("AUSTRALIA", "australia", states);
    Country c5 = new Country("MALAYSIA", "asia", states);

    for (int i = 0; i < 25; i++) {
      int temp;
      temp = i % 5;
      switch (temp) {
        case 1:
          region.put(i, c1);
          break;

        case 2:
          region.put(i, c2);
          break;

        case 3:
          region.put(i, c3);
          break;

        case 4:
          region.put(i, c4);
          break;

        case 0:
          region.put(i, c5);
          break;

        default:
          CacheUtils.log("Nothing to add in region for: " + temp);
          break;

      }// end of switch
    } // end of for

  }// end of populateData

  // //////////////// function to create index ///////////////////
  private void createIndex() throws Exception {
    QueryService qs;
    qs = cache.getQueryService();
    qs.createIndex("villageName", IndexType.FUNCTIONAL, "v.name",
        SEPARATOR + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("cityName", IndexType.FUNCTIONAL, "ct.name",
        SEPARATOR + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("districtName", IndexType.FUNCTIONAL, "d.name",
        SEPARATOR + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("stateName", IndexType.FUNCTIONAL, "s.name",
        SEPARATOR + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("countryName1", IndexType.FUNCTIONAL, "c.name",
        SEPARATOR + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("countryName2", IndexType.FUNCTIONAL, "c.name", SEPARATOR + "Countries c");

  }// end of createIndex

  private static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();
    String indexName;

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexName = index.getName();
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }// end of QueryObserverImpl

}// end of the class
