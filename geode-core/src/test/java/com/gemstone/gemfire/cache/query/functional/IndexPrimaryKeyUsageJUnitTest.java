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
 * IndexPrimaryKeyUsageJUnitTest.java
 *
 * Created on June 7, 2005, 12:32 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.data.Address;
import com.gemstone.gemfire.cache.query.data.Employee;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class IndexPrimaryKeyUsageJUnitTest {


  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region r = CacheUtils.createRegion("portfolios", Portfolio.class);
    for(int i=0;i<4;i++)
      r.put(i+"", new Portfolio(i));
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  @Test
  public void testPrimaryKeyIndexUsage() throws Exception {
    // Task ID: PKI 1
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String queries[] = {
        "select distinct * from /portfolios x, x.positions.values where x.pk = '1'",
        "select distinct * from /portfolios x, x.positions.values where x.pkid = '1'",
        //          BUG # 32707:  FIXED       
        "select distinct * from /portfolios p, p.positions.values where p.pkid != '53'"        
    };
    SelectResults r[][]= new SelectResults[queries.length][2];

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        //                DebuggerSupport.waitForJavaDebugger(CacheUtils.getLogger());
        r[i][0] = (SelectResults)q.execute();
        if(!observer.isIndexesUsed){
          CacheUtils.log("NO INDEX USED");
        }
        CacheUtils.log(Utils.printResult(r[i][0]));
        if ((r[i][0]).size() != 0){
          CacheUtils.log("As Expected, Results Size is NON ZERO");
        }else {
          fail("FAILED:Search result Size is zero");
        } 
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.toString());
      }
    }

    //  Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    qs.createIndex("pkIndex", IndexType.PRIMARY_KEY, "pk", "/portfolios");
    qs.createIndex("pkidIndex", IndexType.PRIMARY_KEY, "pkid", "/portfolios");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i][1] =(SelectResults) q.execute();
        if(observer2.isIndexesUsed == true){
          CacheUtils.log("As expected, INDEX is USED!");

        }else {
          fail("FAILED: INDEX IS NOT USED!");
        }
        CacheUtils.log(Utils.printResult(r[i][1]));
        if ((r[i][1]).size() != 0){
          CacheUtils.log("As Expected, Results Size is NON ZERO");
        }else {
          fail("FAILED:Search result Size is zero");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(r,this);
  }

  @Test
  public void testPrimaryKeyIndexUsageNegativeTestA() throws Exception{
    // Task ID: PKI 2
    Object r[]= new Object[5];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String queries[] = {
        "select distinct * from /portfolios x, x.positions.values where x.pk = '1'",
        "select distinct * from /portfolios.entries x, x.value.positions.values where x.value.pkid = '1'",
        "select distinct * from /portfolios.entries x, x.value.positions.values where x.key = '1'",
    };
    qs = CacheUtils.getQueryService();
    qs.createIndex("pkidIndex", IndexType.PRIMARY_KEY, "pkid", "/portfolios");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i] = q.execute();
        if(!observer.isIndexesUsed == false){
          fail("FAILED: INDEX IS USED!");
        }else{
          CacheUtils.log("As Expected, INDEX is NOT USED");
        }
        CacheUtils.log(Utils.printResult(r[i]) );
        if (((SelectResults)r[i]).size() != 0){
          CacheUtils.log("As Expected, Results Size is NON ZERO");
        }else {
          fail("FAILED:Search result Size is zero");
        } 
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testPrimaryKeyIndexUsageNegativeTestB() throws Exception{
    //Task ID : PKI 3
    Object r[]= new Object[5];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String queries[] = {
        "select distinct * from /portfolios x, x.positions.values where x.pkid = '1'",
    };
    qs = CacheUtils.getQueryService();
    qs.createIndex("pkIndex", IndexType.PRIMARY_KEY, "pk", "/portfolios");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i] = q.execute();
        if(observer.isIndexesUsed){
          fail("INDEX IS USED!");
        }else{
          CacheUtils.log("As Expected, Index Is Not Used");
        }
        CacheUtils.log(Utils.printResult(r[i]) );
        if (((SelectResults)r[i]).size() != 0){
          CacheUtils.log("As Expected, Results Size is NON ZERO");
        }else {
          fail("FAILED:Search result Size is zero");
        } 
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

  }

  @Test
  public void testFunctionalAndPrimaryKey() throws Exception {
    // Task ID: PKI4
    Object r[] = new Object[7];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String queries[] = {
        "select distinct * from /portfolios p, p.positions.values where p.ID > 1 ",
        "select distinct * from /portfolios p, p.positions.values where p.ID < 3 ",
        "select distinct * from /portfolios p, p.positions.values where p.ID >= 1 ",
        "select distinct * from /portfolios p, p.positions.values where p.ID <= 1 ",

    };
    qs = CacheUtils.getQueryService();
    qs.createIndex("IDPRKIndex", IndexType.PRIMARY_KEY, "pkid", "/portfolios");
    qs.createIndex("IDFNLIndex", IndexType.FUNCTIONAL, "ID", "/portfolios");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i] = q.execute();

        if(observer.isIndexesUsed){
          CacheUtils.log("YES, INDEX IS USED!");
        }else{
          fail("ERROR:Index Is Not Used");
        }
        CacheUtils.log(Utils.printResult(r[i]) );
        if (observer.IndexTypeFunctional != 1){
          fail("IMPROPER INDEX USAGE: INDEX USED IS NOT OF TYPE FUNCTIONAL");
        }
        if (((SelectResults)r[i]).size() != 0){
          CacheUtils.log("As Expected, Results Size is NON ZERO");
        }else {
          fail("FAILED:Search result Size is zero");
        } 
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

  }

  @Test
  public void testPublicKeyUsageOnTwoRegions() throws Exception {
    // Task ID: PKI5
    Region rg2 = CacheUtils.createRegion("employees", Employee.class);
    Set add1 = new HashSet();
    Set add2 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add2.add(new Address("411046", "Aundh"));
    rg2.put("1", new Employee("aaa", 27, 270, "QA", 1800, add1));
    rg2.put("2", new Employee("bbb", 28, 280, "QA", 1900, add2));

    Object r[] = new Object[5];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String queries[] = {
        "select distinct * from /portfolios p, /employees e  where p.pkid = '1' ",
    };
    qs = CacheUtils.getQueryService();
    qs.createIndex("IDFNLIndex", IndexType.FUNCTIONAL, "pkid", "/portfolios");
    qs.createIndex("IDPRKIndex", IndexType.PRIMARY_KEY, "pkid", "/portfolios");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i] = q.execute();

        if(observer.isIndexesUsed){
          CacheUtils.log("YES, INDEX IS USED!");

        }else{
          fail("FAILED:Index Is Not Used");
        }
        CacheUtils.log(Utils.printResult(r[i]) );
        if (observer.IndexTypePrimKey != 2){
          fail("IMPROPER INDEX USAGE: INDEX USED IS NOT OF TYPE PRIMARY_KEY");
        }
        if (((SelectResults)r[i]).size() != 0){
          CacheUtils.log("As Expected, Results Size is NON ZERO");
        }else {
          fail("FAILED:Search result Size is zero");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  } 

  class QueryObserverImpl extends QueryObserverAdapter{
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();
    int IndexTypeFunctional = 0 ;
    int IndexTypePrimKey    = 0 ;

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
      if ((index.getType()).equals(IndexType.FUNCTIONAL)){
        IndexTypeFunctional = 1;
      }else {
        IndexTypePrimKey    = 2;
      }
    }

    public void afterIndexLookup(Collection results) {
      if(results != null){
        isIndexesUsed = true;
        //CacheUtils.log(Utils.printResult(results));
      }
    }
  }
}
