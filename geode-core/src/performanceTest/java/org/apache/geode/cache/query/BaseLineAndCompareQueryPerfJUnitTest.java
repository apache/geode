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
 * BaseLineAndCompareQueryPerfJUnitTest.java
 *
 * Created on October 13, 2005, 11:28 AM
 */
package org.apache.geode.cache.query;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.data.Country;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.PerformanceTest;

/**
 * This test is to baseline and compare the performance figures for index usage benchmarks.
 */
@Category({PerformanceTest.class, OQLQueryTest.class})
@Ignore("Performance tests should not be run as part of precheckin")
public class BaseLineAndCompareQueryPerfJUnitTest {

  /** Creates a new instance of BaseLineAndCompareQueryPerfJUnitTest */
  public BaseLineAndCompareQueryPerfJUnitTest() {}// end of constructor1


  /////////////////////
  // static Cache cache;
  static Region region;
  static Region region1;
  static Region region2;
  static Region region3;
  static Index index;
  // static DistributedSystem ds;
  // static Properties props = new Properties();
  static QueryService qs;
  static Map queriesMap = new TreeMap();
  static Map withoutIndexTimeRegion = new TreeMap();
  static Map withIndexTimeRegion = new TreeMap();
  static Map indexNameRegion = new TreeMap();
  static Map withoutIndexResultSetSize = new TreeMap();
  static Map withIndexResultSetSize = new TreeMap();

  private static FileOutputStream file;
  private static BufferedWriter wr;
  private static int printCntr = 1;

  static final int MAX_OBJECTS = 10;
  static final int QUERY_EXECUTED = 5;

  ///////////// queries ///////////

  String[] queries = {
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
          + "Countries c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI' AND s.name = 'GUJARAT'",

      //// following are multiregion queries
      // Query 9
      "SELECT DISTINCT * FROM " + SEPARATOR
          + "Countries1 c1, c1.states s1, s1.districts d1, d1.villages v1, d1.cities ct1, "
          + SEPARATOR
          + "Countries2 c2, c2.states s2, s2.districts d2, d2.villages v2, d2.cities ct2 WHERE v1.name = 'MAHARASHTRA_VILLAGE1' AND ct2.name = 'MUMBAI' ",
      // Query 10
      // "SELECT DISTINCT * FROM /Countries1 c1, c1.states s1, s1.districts d1, d1.villages v1,
      // d1.cities ct1, /Countries2 c2, c2.states s2, s2.districts d2, d2.villages v2, d2.cities
      // ct2, /Countries3 c3, c3.states s3, s3.districts d3, d3.villages v3, d3.cities ct3 WHERE
      // v1.name='MAHARASHTRA_VILLAGE1' AND ct3.name = 'PUNE'",
      // Query 11
      "SELECT DISTINCT * FROM " + SEPARATOR + "Countries1 c1, " + SEPARATOR
          + "Countries2 c2 WHERE c1.name = 'INDIA' AND c2.name = 'ISRAEL'",
      // Query 12
      "SELECT DISTINCT * FROM " + SEPARATOR
          + "Countries1 c1, c1.states s1, s1.districts d1, d1.cities ct1, d1.villages v1, "
          + SEPARATOR
          + "Countries2 c2, c2.states s2, s2.districts d2, d2.cities ct2, d2.villages v2 WHERE c1.name = 'INDIA' AND c2.name = 'ISRAEL'",
      // Query 13
      "SELECT DISTINCT * FROM " + SEPARATOR + "Countries1 c1, c1.states s1, s1.districts d1, "
          + SEPARATOR + "Countries2 c2, c2.states s2, s2.districts d2, " + SEPARATOR
          + "Countries3 c3, c3.states s3, s3.districts d3 WHERE d3.name = 'PUNEDIST' AND s2.name = 'GUJARAT'",
      // Query 14
      "SELECT DISTINCT c1.name, s1.name, d2.name, ct2.name FROM " + SEPARATOR
          + "Countries1 c1, c1.states s1, s1.districts d1, d1.cities ct1, " + SEPARATOR
          + "Countries2 c2, c2.states s2, s2.districts d2, d2.cities ct2 WHERE ct1.name = 'MUMBAI' OR ct2.name = 'CHENNAI'",
      // Query 15
      "SELECT DISTINCT c1.name, s1.name, ct1.name FROM " + SEPARATOR
          + "Countries1 c1, c1.states s1, (SELECT DISTINCT * FROM " + SEPARATOR
          + "Countries2 c2, c2.states s2, s2.districts d2, d2.cities ct2 WHERE s2.name = 'PUNJAB') itr1, s1.districts d1, d1.cities ct1 WHERE ct1.name = 'CHANDIGARH'",
      // Query 16
      "SELECT DISTINCT c1.name, s1.name, ct1.name FROM " + SEPARATOR
          + "Countries1 c1, c1.states s1, s1.districts d1, d1.cities ct1 WHERE ct1.name = element (SELECT DISTINCT ct3.name FROM "
          + SEPARATOR
          + "Countries3 c3, c3.states s3, s3.districts d3, d3.cities ct3 WHERE s3.name = 'MAHARASHTRA' AND ct3.name = 'PUNE')",
      // Query 17
      "SELECT DISTINCT c1.name, s1.name, ct1.name FROM " + SEPARATOR
          + "Countries1 c1, c1.states s1, s1.districts d1, d1.cities ct1, d1.getVillages() v1 WHERE v1.getName() = 'PUNJAB_VILLAGE1'",
      // Query 18
      "SELECT DISTINCT s.name, s.getDistricts(), ct.getName() FROM " + SEPARATOR
          + "Countries1 c, c.getStates() s, s.getDistricts() d, d.getCities() ct WHERE ct.getName() = 'PUNE' OR ct.name = 'CHANDIGARH' OR s.getName() = 'GUJARAT'",
      // Query 19
      "SELECT DISTINCT d.getName(), d.getCities(), d.getVillages() FROM " + SEPARATOR
          + "Countries3 c, c.states s, s.districts d WHERE d.name = 'MUMBAIDIST'"

      // Query 9a & 9b
      // "SELECT DISTINCT c.name, s.name, ct.name FROM /Countries c, c.states s, (SELECT DISTINCT *
      // FROM " +
      // "/Countries c, c.states s, s.districts d, d.cities ct WHERE s.name = 'PUNJAB') itr1, " +
      // "s.districts d, d.cities ct WHERE ct.name = 'CHANDIGARH'",
      //
      // "SELECT DISTINCT c.name, s.name, ct.name FROM /Countries c, c.states s, s.districts d," +
      // " d.cities ct WHERE ct.name = (SELECT DISTINCT ct.name FROM /Countries c, c.states s, " +
      // "s.districts d, d.cities ct WHERE s.name = 'MAHARASHTRA' AND ct.name = 'PUNE')",
      // //Query 10
      // "SELECT DISTINCT c.name, s.name, ct.name FROM /Countries c, c.states s, s.districts d, " +
      // "d.cities ct, d.getVillages() v WHERE v.getName() = 'PUNJAB_VILLAGE1'",
      // //Query 11
      // "SELECT DISTINCT s.name, s.getDistricts(), ct.getName() FROM /Countries c, c.getStates() s,
      // " +
      // "s.getDistricts() d, d.getCities() ct WHERE ct.getName() = 'PUNE' OR ct.name = 'CHANDIGARH'
      // " +
      // "OR s.getName() = 'GUJARAT'",
      // //Query 12
      // "SELECT DISTINCT d.getName(), d.getCities(), d.getVillages() FROM /Countries c, " +
      // "c.states s, s.districts d WHERE d.name = 'MUMBAIDIST'"
  };


  ////////////////////

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }// end of setUp

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
    // ds.disconnect();
  }// end of tearDown


  //////////////// test methods ///////////////
  @Test
  public void testPerf() throws Exception {
    createRegion();
    populateData();

    String sqlStr;
    long startTime, endTime, totalTime = 0;
    SelectResults rs = null;
    Query q;

    ///// without index ////////
    for (int x = 0; x < queries.length; x++) {
      CacheUtils.log("Query No: " + (x + 1) + "...without index execution");
      sqlStr = queries[x];
      QueryService qs = CacheUtils.getQueryService();
      q = qs.newQuery(sqlStr);

      totalTime = 0;

      queriesMap.put(new Integer(x), q);

      for (int i = 0; i < QUERY_EXECUTED; i++) {
        startTime = System.currentTimeMillis();
        rs = (SelectResults) q.execute();
        endTime = System.currentTimeMillis();
        totalTime = totalTime + (endTime - startTime);
      }

      long withoutIndexTime = totalTime / QUERY_EXECUTED;

      withoutIndexTimeRegion.put(new Integer(x), new Long(withoutIndexTime));

      withoutIndexResultSetSize.put(new Integer(x), new Integer(rs.size()));
    }

    ////////// create index
    createIndex();

    ///////// measuring time with index
    for (int x = 0; x < queries.length; x++) {
      CacheUtils.log("Query No: " + (x + 1) + "...with index execution");
      sqlStr = queries[x];
      QueryService qs2 = CacheUtils.getQueryService();// ????
      q = qs2.newQuery(sqlStr);

      queriesMap.put(new Integer(x), q);

      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);

      totalTime = 0;

      for (int i = 0; i < QUERY_EXECUTED; i++) {
        startTime = System.currentTimeMillis();
        rs = (SelectResults) q.execute();
        endTime = System.currentTimeMillis();
        totalTime = totalTime + (endTime - startTime);

        if (i == 0) {
          ArrayList al = new ArrayList();
          Iterator itr = observer.indexesUsed.iterator();
          while (itr.hasNext()) {
            al.add(itr.next());
          }
          indexNameRegion.put(new Integer(x), al);
        }
      } // end of for loop

      long withIndexTime = totalTime / QUERY_EXECUTED;

      withIndexTimeRegion.put(new Integer(x), new Long(withIndexTime));

      withIndexResultSetSize.put(new Integer(x), new Integer(rs.size()));
    }

    printSummary();

  }// end of testPerf

  /**
   * Get the performance of Range query in an AND junction.
   */
  @Test
  public void testPerformanceForRangeQueries() {
    try {
      // ds = DistributedSystem.connect(props);
      // cache = CacheFactory.create(ds);
      /*
       * AttributesFactory factory = new AttributesFactory();
       * factory.setScope(Scope.DISTRIBUTED_ACK); factory.setValueConstraint(Portfolio.class);
       */
      region = CacheUtils.createRegion("Portfolio", Portfolio.class);
      for (int i = 0; i < 25000; ++i) {
        region.create(new Integer(i + 1), new Portfolio(i));
      }
      String queryStr = "select  * from " + SEPARATOR
          + "Portfolio pf where pf.getID  > 10000 and pf.getID < 12000";

      SelectResults[][] r = new SelectResults[1][2];
      QueryService qs = CacheUtils.getQueryService();
      Query qry = qs.newQuery(queryStr);
      for (int i = 0; i < 5; ++i) {
        qry.execute();
      }

      long time = 0l;
      long start = 0;
      long end = 0;
      int numTimesToRun = 10;
      SelectResults sr = null;
      float t1, t2 = 0;
      for (int i = 0; i < numTimesToRun; ++i) {
        start = System.currentTimeMillis();
        sr = (SelectResults) qry.execute();
        end = System.currentTimeMillis();
        time += (end - start);
      }
      r[0][0] = sr;
      t1 = ((float) (time)) / numTimesToRun;
      CacheUtils.log("AVG time taken without Index =" + t1);
      qs.createIndex("ID", IndexType.FUNCTIONAL, "pf.getID", SEPARATOR + "Portfolio pf");
      for (int i = 0; i < 5; ++i) {
        qry.execute();
      }

      time = 0l;
      start = 0;
      end = 0;
      sr = null;
      for (int i = 0; i < numTimesToRun; ++i) {
        start = System.currentTimeMillis();
        sr = (SelectResults) qry.execute();
        end = System.currentTimeMillis();
        time += (end - start);
      }
      r[0][1] = sr;
      t2 = ((float) (time)) / numTimesToRun;
      CacheUtils.log("AVG time taken with Index =" + t2);
      CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);
      assertTrue("Avg. Time taken to query with index should be less than time taken without index",
          t2 < t1);


    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception=" + e);
    }

  }



  // /////// supplementary methods /////////////
  public static void createRegion() {
    try {
      /*
       * ds = DistributedSystem.connect(props); cache = CacheFactory.create(ds); AttributesFactory
       * factory = new AttributesFactory(); factory.setScope(Scope.DISTRIBUTED_ACK);
       * factory.setValueConstraint(Country.class); region = cache.createRegion("Countries",
       * factory.create());
       */
      region = CacheUtils.createRegion("Countries", Country.class);
      region1 = CacheUtils.createRegion("Countries1", null);
      region2 = CacheUtils.createRegion("Countries2", null);
      region3 = CacheUtils.createRegion("Countries3", null);
      CacheUtils.log("Regions are created");

    } catch (Exception e) {
      e.printStackTrace();
    }

  }// end of createRegion

  public static void populateData() throws Exception {
    /*
     * Set villages = new HashSet(); for (int i=0; i<10; i++){ villages.add(new Village(i)); }
     *
     * Set cities = new HashSet(); for (int i=0; i<10; i++){ cities.add(new City(i)); }
     *
     * Set districts = new HashSet(); for (int i=0; i<10; i++){ districts.add(new District(i,
     * cities, villages)); }
     *
     * Set states = new HashSet(); for (int i=0; i<10; i++){ states.add(new State(i, districts)); }
     */

    /* Add the countries */
    for (int i = 0; i < MAX_OBJECTS; i++) {
      region.put(new Integer(i), new Country(i, 2, 3, 4, 4));
      region1.put(new Integer(i), new Country(i, 2, 3, 4, 4));
      region2.put(new Integer(i), new Country(i, 2, 3, 4, 4));
      region3.put(new Integer(i), new Country(i, 2, 3, 4, 4));
    }
    CacheUtils.log("Regions are populated");

  }// end of populateData

  public static void createIndex() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    /*
     * Indices share the following percentages: a. countryName: 20% objects b. stateName: 33.33%
     * objects c. districtName: 20% objects d. cityName: 50% objects e. villageName: No index
     */
    // queryService.createIndex("villageName", IndexType.FUNCTIONAL, "v.name", "/Countries c,
    // c.states s,
    // s.districts d, d.cities ct, d.villages v");
    qs.createIndex("cityName", IndexType.FUNCTIONAL, "ct.name",
        SEPARATOR + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v");//
    qs.createIndex("districtName", IndexType.FUNCTIONAL, "d.name",
        SEPARATOR + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("stateName", IndexType.FUNCTIONAL, "s.name",
        SEPARATOR + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("countryName", IndexType.FUNCTIONAL, "c.name",
        SEPARATOR + "Countries c, c.states s, s.districts d, d.cities ct, d.villages v");

    /* Indices on region1 */
    qs.createIndex("villageName1", IndexType.FUNCTIONAL, "v.name",
        SEPARATOR + "Countries1 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("cityName1", IndexType.FUNCTIONAL, "ct.name",
        SEPARATOR + "Countries1 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("countryNameA", IndexType.FUNCTIONAL, "c.name",
        SEPARATOR + "Countries1 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("countryNameB", IndexType.FUNCTIONAL, "c.name", SEPARATOR + "Countries1 c");

    /* Indices on region2 */
    qs.createIndex("stateName2", IndexType.FUNCTIONAL, "s.name",
        SEPARATOR + "Countries2 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("cityName2", IndexType.FUNCTIONAL, "ct.name",
        SEPARATOR + "Countries2 c, c.states s, s.districts d, d.cities ct, d.villages v");

    /* Indices on region3 */
    qs.createIndex("districtName3", IndexType.FUNCTIONAL, "d.name",
        SEPARATOR + "Countries3 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("villageName3", IndexType.FUNCTIONAL, "v.name",
        SEPARATOR + "Countries3 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("cityName3", IndexType.FUNCTIONAL, "ct.name",
        SEPARATOR + "Countries3 c, c.states s, s.districts d, d.cities ct, d.villages v");

    CacheUtils.log("Indices are created");

  }// end of createIndex

  public static void printSummary() throws Exception {
    CacheUtils.log("Printing summary");

    if (printCntr == 1) {
      // file = new FileOutputStream("c:\\QueryPerfLog.txt");
      Date date = new Date(System.currentTimeMillis());
      file = new FileOutputStream("./QueryPerfLog-" + (date.toGMTString()
          .substring(0, date.toGMTString().indexOf("200") + 4).replace(' ', '-')) + ".txt");
      wr = new BufferedWriter(new OutputStreamWriter(file));

      wr.write("===========================================================================");
      wr.newLine();
      wr.write("====================QUERY PERFORMANCE REPORT===============================");
      wr.newLine();
      wr.flush();
      wr.write("Percentages of indices are as follows: ");
      wr.newLine();
      wr.write("countryName: 20% objects");
      wr.newLine();
      wr.write("stateName: 33.33% objects");
      wr.newLine();
      wr.write("districtName: 20% objects");
      wr.newLine();
      wr.write("cityName: 50% objects");
      wr.newLine();
      wr.write("villageName: No index");
      wr.newLine();
      wr.newLine();
      wr.flush();

      wr.write("Timings are the average of times for execution of query " + QUERY_EXECUTED
          + " number of times");
      wr.newLine();
      wr.newLine();
      wr.write("===========================================================================");
    }



    wr.newLine();
    wr.newLine();
    wr.write("Printing details for query no: " + printCntr);
    wr.newLine();
    wr.newLine();


    Set set0 = queriesMap.keySet();
    Iterator itr0 = set0.iterator();

    /*
     * Set set1 = withoutIndexTimeRegion.keySet(); Iterator itr1 = set1.iterator();
     *
     * Set set2 = withIndexTimeRegion.keySet(); Iterator itr2 = set2.iterator();
     *
     * Set set3 = indexNameRegion.keySet(); Iterator itr3 = set3.iterator();
     *
     * Set set4 = withoutIndexResultSetSize.keySet(); Iterator itr4 = set4.iterator();
     *
     * Set set5 = withIndexResultSetSize.keySet(); Iterator itr5 = set5.iterator();
     */
    Integer it;
    while (itr0.hasNext()) {
      wr.write("Printing details for query no: " + printCntr);
      wr.newLine();
      wr.newLine();
      it = (Integer) itr0.next();
      Query q1 = (Query) queriesMap.get(it);
      wr.write("Query string is: ");
      wr.newLine();
      wr.write(q1.getQueryString());
      wr.newLine();
      wr.newLine();

      wr.write("Time taken without index is: " + withoutIndexTimeRegion.get(it));
      wr.newLine();
      wr.newLine();

      // Query q2 = (Query) itr2.next();
      wr.write("Time taken with index is: " + withIndexTimeRegion.get(it));
      wr.newLine();
      wr.newLine();

      wr.write("Size of result set without index is: " + withoutIndexResultSetSize.get(it));
      wr.newLine();
      wr.newLine();
      wr.write("Size of result set with index is: " + withIndexResultSetSize.get(it));
      wr.newLine();
      wr.newLine();

      wr.write("Indices used are: ");
      wr.newLine();
      wr.newLine();

      ArrayList al = (ArrayList) indexNameRegion.get(it);

      if (al.size() == 0) {
        wr.write("No indices are getting used in this query");
        wr.newLine();
        wr.newLine();
      } else {
        Iterator itr4 = al.iterator();
        while (itr4.hasNext()) {
          wr.write(itr4.next().toString());
          wr.newLine();
        }
      }

      printCntr++;
      wr.write("===========================================================================");
      wr.newLine();
      wr.newLine();
      wr.flush();
    }

    wr.write("===========================================================================");
    wr.flush();

  }// end of printSummary

  ////// query observer to get which indices are getting used /////
  class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }//////

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      } /////////
    }
  }// end of QueryObserverImpls


}// end of BaseLineAndCompareQueryPerfJUnitTest
