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
 * IUMRCompositeIteratorJUnitTest.java
 *
 * Created on October 20, 2005, 4:52 PM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Address;
import org.apache.geode.cache.query.data.Country;
import org.apache.geode.cache.query.data.Employee;
import org.apache.geode.cache.query.data.PhoneNo;
import org.apache.geode.cache.query.data.Street;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IUMRCompositeIteratorJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();

    Region r1 = CacheUtils.createRegion("countries", Country.class);
    Set CountrySet1 = new HashSet();
    CountrySet1.add("India");
    CountrySet1.add("China");
    CountrySet1.add("USA");
    CountrySet1.add("UK");
    for (int i = 0; i < 4; i++) {
      r1.put(i, new Country(i, CountrySet1));
    }

    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r2 = CacheUtils.createRegion("employees", Employee.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }

    Region r3 = CacheUtils.createRegion("address", Address.class);
    Set ph = new HashSet();
    Set str = new HashSet();
    ph.add(new PhoneNo(111, 222, 333, 444));
    str.add(new Street("DPRoad", "lane5"));
    str.add(new Street("DPStreet1", "lane5"));
    for (int i = 0; i < 4; i++) {
      r3.put(i, new Address("411001", "Pune", str, ph));
    }


  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testQueryWithCompositeIter1() throws Exception {

    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries = {
        // Test Case No. IUMR
        "Select distinct * from " + SEPARATOR + "employees e, " + SEPARATOR
            + "address a, e.getPhoneNo(a.zipCode) ea where e.name ='empName'",
        // "Select distinct * from /employees e, /address a, e.getPh(e.empId) where e.name
        // ='empName'",
    };
    SelectResults[][] r = new SelectResults[queries.length][2];
    // Execute Query without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = (SelectResults) q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes and Execute Queries
    qs.createIndex("nameIndex", IndexType.FUNCTIONAL, "e.name", SEPARATOR + "employees e");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = (SelectResults) q.execute();
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        int indxs = observer.indexesUsed.size();
        CacheUtils
            .log("***********Indexes Used :::: " + indxs + " IndexName::" + observer.IndexName);
        if (indxs != 1) {
          fail("FAILED: The Index should be used. Presently only " + indxs + " Index(es) is used");
        }

        Iterator itr = observer.indexesUsed.iterator();
        String temp;

        while (itr.hasNext()) {
          temp = itr.next().toString();

          if (temp.equals("nameIndex")) {
            break;
          } else {
            fail("indices used do not match with the which is expected to be used"
                + "<nameIndex> was expected but found " + itr.next());
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Verifying the query results
    // StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);

  }

  @Ignore
  @Test
  public void testQueryWithCompositeIter2() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries = {
        // Test Case No. IUMR
        "Select distinct * from " + SEPARATOR + "countries c, " + SEPARATOR
            + "employees e, c.citizens[e.empId].arr where e.name='empName'",};
    SelectResults[][] r = new SelectResults[queries.length][2];

    // Execute Query without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i][0] = (SelectResults) q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create Indexes and Execute Queries
    qs.createIndex("nameIndex", IndexType.FUNCTIONAL, "e.name", SEPARATOR + "employees e");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = (SelectResults) q.execute();
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        int indxs = observer.indexesUsed.size();
        CacheUtils
            .log("***********Indexes Used :::: " + indxs + " IndexName::" + observer.IndexName);
        if (indxs != 1) {
          fail("FAILED: The Index should be used. Presently only " + indxs + " Index(es) is used");
        }

        Iterator itr = observer.indexesUsed.iterator();
        String temp;

        while (itr.hasNext()) {
          temp = itr.next().toString();

          if (temp.equals("nameIndex")) {
            break;
          } else {
            fail("indices used do not match with the which is expected to be used"
                + "<nameIndex> was expected but found " + itr.next());
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Verifying the query results

    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);

  }

  class QueryObserverImpl extends QueryObserverAdapter {
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

}// end of the class
