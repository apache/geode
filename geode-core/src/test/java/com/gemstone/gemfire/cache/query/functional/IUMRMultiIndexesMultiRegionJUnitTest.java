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
 * IUMRMultiIndexesMultiRegionJUnitTest.java
 *
 * Created on September 30, 2005, 2:54 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
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
import com.gemstone.gemfire.cache.query.data.Address;
import com.gemstone.gemfire.cache.query.data.Employee;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.query.data.Quote;
import com.gemstone.gemfire.cache.query.data.Restricted;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class IUMRMultiIndexesMultiRegionJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();

  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testMultiIteratorsMultiRegion1() throws Exception {
    Object r[][]= new Object[4][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    //Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for(int i=0;i<4;i++){
      r1.put(i+"", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for(int i=0;i<4;i++){
      r2.put(i+"", new Portfolio(i));
    }

    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    for(int i=0;i<4;i++){
      r3.put(i+"", new Employee("empName",(20+i),i,"Mr.",(5000+i),add1));
    }
    String queries[] = {
        // Test case No. IUMR021
        "SELECT DISTINCT * FROM /portfolio1 pf1, /portfolio2 pf2, /employees e WHERE pf1.status = 'active'",
        //"SELECT DISTINCT * FROM /portfolio1 pf1, pf1.positions.values posit1, /portfolio2 pf2, /employees e WHERE posit1.secId='IBM'"

    };
    //Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    //Create Indexes
    qs.createIndex("statusIndexPf1",IndexType.FUNCTIONAL,"status","/portfolio1");
    qs.createIndex("statusIndexPf2",IndexType.FUNCTIONAL,"status","/portfolio2");

    //Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if(!observer.isIndexesUsed){
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while(itr.hasNext()){
          String indexUsed = itr.next().toString(); 
          if( !(indexUsed).equals("statusIndexPf1")){
            fail("<statusIndexPf1> was expected but found "+indexUsed);
          }
          //assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        CacheUtils.log("**************************************************Indexes Used :::::: "+indxs+" Index Name: "+observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length,queries);
  }//end of test1

  @Test
  public void testMultiIteratorsMultiRegion2() throws Exception{
    Object r[][]= new Object[4][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    //Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for(int i=0;i<4;i++){
      r1.put(i+"", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for(int i=0;i<4;i++){
      r2.put(i+"", new Portfolio(i));
    }

    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    for(int i=0;i<4;i++){
      r3.put(i+"", new Employee("empName",(20+i),i,"Mr.",(5000+i),add1));
    }
    String queries[] = {
        //Test case No. IUMR022
        //Both the Indexes Must get used. Presently only one Index is being used.
        "SELECT DISTINCT * FROM /portfolio1 pf1, /portfolio2 pf2, /employees e1 WHERE pf1.status = 'active' AND e1.empId < 10"
    };
    //Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    //Create Indexes & Execute the queries
    qs.createIndex("statusIndexPf1",IndexType.FUNCTIONAL,"pf1.status","/portfolio1 pf1");
    qs.createIndex("empIdIndex",IndexType.FUNCTIONAL,"e.empId","/employees e");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if(!observer.isIndexesUsed){
          fail("Index is NOT uesd");
        }
        int indxs = observer.indexesUsed.size();
        CacheUtils.log("**************************************************Indexes Used :::::: "+indxs+" Index Name: "+observer.indexName);
        if(indxs!=2){
          fail("Both the idexes are not getting used.Only "+indxs+" index is getting used");
        }

        Iterator itr = observer.indexesUsed.iterator();
        String temp;

        while(itr.hasNext()){
          temp = itr.next().toString();

          if( temp.equals("statusIndexPf1")){
            break;
          }else if(temp.equals("empIdIndex")){
            break;
          }else{
            fail("indices used do not match with those which are expected to be used" +
                "<statusIndexPf1> and <empIdIndex> were expected but found " +itr.next());
          }
          //assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
  }//end of test2

  @Test
  public void testMultiIteratorsMultiRegion3() throws Exception{
    Object r[][]= new Object[9][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    //Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for(int i=0;i<4;i++){
      r1.put(i+"", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for(int i=0;i<4;i++){
      r2.put(i+"", new Portfolio(i));
    }
    String queries[] = {
        //Test Case No. IUMR004
        "SELECT DISTINCT * FROM /portfolio1 pf1, /portfolio2 pf2, pf1.positions.values posit1," +
        " pf2.positions.values posit2 WHERE posit1.secId='IBM' AND posit2.secId='IBM'",
        //Test Case No.IUMR023
        "SELECT DISTINCT * FROM /portfolio1 pf1,/portfolio2 pf2, pf1.positions.values posit1," +
        " pf2.positions.values posit2 WHERE posit1.secId='IBM' OR posit2.secId='IBM'",

        "SELECT DISTINCT * FROM /portfolio1 pf1, pf1.collectionHolderMap.values coll1," +
            " pf1.positions.values posit1, /portfolio2 pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values posit2 " +
            " WHERE posit1.secId='IBM' AND posit2.secId='IBM'",
            //Test Case No. IUMR005
            "SELECT DISTINCT * FROM /portfolio1 pf1,/portfolio2 pf2, pf1.positions.values posit1, pf2.positions.values posit2," +
            " pf1.collectionHolderMap.values coll1,pf2.collectionHolderMap.values coll2 " +
            " WHERE posit1.secId='IBM' OR posit2.secId='IBM'",
            //Test Case No. IUMR006
            "SELECT DISTINCT coll1 as collHldrMap1 , coll2 as CollHldrMap2 FROM /portfolio1 pf1, /portfolio2 pf2, pf1.positions.values posit1, pf2.positions.values posit2," +
            " pf1.collectionHolderMap.values coll1,pf2.collectionHolderMap.values coll2 " +
            " WHERE posit1.secId='IBM' OR posit2.secId='IBM'",

    };
    // Execute Queries Without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    //Create Indexes and Execute the Queries
    qs.createIndex("secIdIndexPf1",IndexType.FUNCTIONAL,"pos11.secId","/portfolio1 pf1, pf1.collectionHolderMap.values coll1, pf1.positions.values pos11");
    qs.createIndex("secIdIndexPf2",IndexType.FUNCTIONAL,"pos22.secId","/portfolio2 pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values pos22");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        //try {Thread.sleep(5000);} catch(Exception e){}
        r[i][1] = q.execute();
        if(!observer.isIndexesUsed){
          fail("Index is NOT uesd");
        }
        int indxs = observer.indexesUsed.size();
        if(indxs!=2){
          fail("Both the idexes are not getting used.Only "+indxs+" index is getting used");
        }

        CacheUtils.log("**************************************************Indexes Used :::::: "+indxs);

        Iterator itr = observer.indexesUsed.iterator();
        String temp;

        while(itr.hasNext()){
          temp = itr.next().toString();

          if(temp.equals("secIdIndexPf1")){
            break;
          }else if(temp.equals("secIdIndexPf2")){
            break;
          }else{
            fail("indices used do not match with those which are expected to be used" +
                "<secIdIndexPf1> and <secIdIndexPf2> were expected but found " +itr.next());
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    //Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
  }// end of test3

  @Test
  public void testMultiIteratorsMultiRegion4() throws Exception{
    Object r[][]= new Object[4][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    //Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for(int i=0;i<4;i++){
      r1.put(i+"", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for(int i=0;i<4;i++){
      r2.put(i+"", new Portfolio(i));
    }
    String queries[] = {
        //Test case No. IUMR024
        //Both the Indexes Must get used. Presently only one Index is being used.
        "SELECT DISTINCT * FROM /portfolio1 pf1, pf1.positions.values posit1, /portfolio2 pf2, pf2.positions.values posit2 WHERE pf2.status='active' AND posit1.secId='IBM'"
    };
    //Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    //Create Indexes
    qs.createIndex("secIdIndexPf1",IndexType.FUNCTIONAL,"pos11.secId","/portfolio1 pf1, pf1.positions.values pos11");
    qs.createIndex("statusIndexPf2",IndexType.FUNCTIONAL,"pf2.status","/portfolio2 pf2");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if(!observer.isIndexesUsed){
          fail("Index is NOT uesd");
        }
        int indxs = observer.indexesUsed.size();
        if(indxs!=2){
          fail("Both the idexes are not getting used.Only "+indxs+" index is getting used");
        }
        CacheUtils.log("**************************************************Indexes Used :::::: "+indxs);

        Iterator itr = observer.indexesUsed.iterator();
        String temp;                

        while(itr.hasNext()){
          temp = itr.next().toString();

          if(temp.equals("secIdIndexPf1")){
            break;
          }else if(temp.equals("statusIndexPf2")){
            break;
          }else{
            fail("indices used do not match with those which are expected to be used" +
                "<statusIndexPf1> and <statusIndexPf2> were expected but found " +itr.next());
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    //Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
  }//end of test4

  @Test
  public void testMultiIteratorsMultiRegion5() throws Exception{
    Object r[][]= new Object[4][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    //Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for(int i=0;i<4;i++){
      r1.put(i+"", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for(int i=0;i<4;i++){
      r2.put(i+"", new Portfolio(i));
    }

    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    for(int i=0;i<4;i++){
      r3.put(i+"", new Employee("empName",(20+i),i,"Mr.",(5000+i),add1));
    }
    String queries[] = {
        //Test case IUMR025
        //Three of the idexes must get used.. Presently only one Index is being used.
        "SELECT DISTINCT * FROM /portfolio1 pf1, /portfolio2 pf2, /employees e1 WHERE pf1.status = 'active' AND pf2.status = 'active' AND e1.empId < 10"
    };
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    //Create Indexes and Execute the Queries
    qs.createIndex("statusIndexPf1",IndexType.FUNCTIONAL,"pf1.status","/portfolio1 pf1");
    qs.createIndex("statusIndexPf2",IndexType.FUNCTIONAL,"pf2.status","/portfolio2 pf2");
    qs.createIndex("empIdIndex",IndexType.FUNCTIONAL,"empId","/employees");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if(!observer.isIndexesUsed){
          fail("Index is NOT uesd");
        }
        int indxs = observer.indexesUsed.size();
        if(indxs!=3){
          fail("Three of the idexes are not getting used. Only "+indxs+" index is getting used");
        }
        CacheUtils.log("**************************************************Indexes Used :::::: "+indxs);

        Iterator itr = observer.indexesUsed.iterator();
        String temp;

        while(itr.hasNext()){
          temp = itr.next().toString();

          if(temp.equals("statusIndexPf1")){
            break;
          }else if(temp.equals("statusIndexPf2")){
            break;
          }else if(temp.equals("empIdIndex")){
            break;
          }else{
            fail("indices used do not match with those which are expected to be used" +
                "<statusIndexPf1>, <statusIndexPf2> and <empIdIndex> were expected but found " +itr.next());
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    //Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
  }//end of test5

  @Test
  public void testMultiIteratorsMultiRegion6() throws Exception{
    Object r[][]= new Object[4][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    //Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for(int i=0;i<4;i++){
      r1.put(i+"", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for(int i=0;i<4;i++){
      r2.put(i+"", new Portfolio(i));
    }
    String queries[] = {
        //Both the Indexes Must get used. Presently only one Index is being used.
        " SELECT DISTINCT * FROM /portfolio1 pf1, /portfolio2 pf2, pf1.positions.values posit1," +
        " pf2.positions.values posit2 WHERE posit1.secId='IBM' AND posit2.secId='IBM' ",
        " SELECT DISTINCT * FROM /portfolio1 pf1, pf1.collectionHolderMap.values coll1," +
            " pf1.positions.values posit1, /portfolio2 pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values posit2 " +
            " WHERE posit1.secId='IBM' AND posit2.secId='IBM'",
    };
    // Execute Queries Without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    //Create Indexes and Execute the Queries
    qs.createIndex("secIdIndexPf1",IndexType.FUNCTIONAL,"pos11.secId","/portfolio1 pf1, pf1.positions.values pos11");
    qs.createIndex("secIdIndexPf2",IndexType.FUNCTIONAL,"pos22.secId","/portfolio2 pf2, pf2.positions.values pos22");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if(!observer.isIndexesUsed){
          fail("Index is NOT uesd");
        }
        int indxs = observer.indexesUsed.size();
        if(indxs!=2){
          fail("Both the idexes are not getting used.Only "+indxs+" index is getting used");
        }
        CacheUtils.log("**************************************************Indexes Used :::::: "+indxs);

        Iterator itr = observer.indexesUsed.iterator();
        String temp;

        while(itr.hasNext()){
          temp = itr.next().toString();

          if(temp.equals("secIdIndexPf1")){
            break;
          }else if(temp.equals("secIdIndexPf2")){
            break;
          }else{
            fail("indices used do not match with those which are expected to be used" +
                "<secIdIndexPf1> and <secIdIndexPf2> were expected but found " +itr.next());
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    //Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
  }// end of test6

  @Test
  public void testMultiIteratorsMultiRegion7() throws Exception{

    Object r[][]= new Object[4][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    //Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for(int i=0;i<4;i++){
      r1.put(i+"", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for(int i=0;i<4;i++){
      r2.put(i+"", new Portfolio(i));
    }
    String queries[] = {
        //Task IUMR007
        "SELECT DISTINCT coll1 as collHldrMap1 , coll2 as CollHldrMap2 FROM /portfolio1 pf1, /portfolio2 pf2, pf1.positions.values posit1,pf2.positions.values posit2," +
        "pf1.collectionHolderMap.values coll1, pf2.collectionHolderMap.values coll2 " +
        "WHERE posit1.secId='IBM' OR posit2.secId='IBM'",

    };
    // Execute Queries Without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    //Create Indexes and Execute the Queries
    qs.createIndex("secIdIndexPf1",IndexType.FUNCTIONAL,"pos11.secId","/portfolio1 pf1, pf1.positions.values pos11");
    qs.createIndex("secIdIndexPf2",IndexType.FUNCTIONAL,"pos22.secId","/portfolio2 pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values pos22");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if(!observer.isIndexesUsed){
          fail("Index is NOT uesd");
        }
        int indxs = observer.indexesUsed.size();
        if(indxs!=2){
          fail("Both the idexes are not getting used.Only "+indxs+" index is getting used");
        }
        CacheUtils.log("**************************************************Indexes Used :::::: "+indxs);

        Iterator itr = observer.indexesUsed.iterator();
        String temp;

        while(itr.hasNext()){
          temp = itr.next().toString();

          if(temp.equals("secIdIndexPf1")){
            break;
          }else if(temp.equals("secIdIndexPf2")){
            break;
          }else{
            fail("indices used do not match with those which are expected to be used" +
                "<secIdIndexPf1> and <secIdIndexPf2> were expected but found " +itr.next());
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    //Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
  }// end of test7

  @Test
  public void testMultiIteratorsMultiRegion8() throws Exception {
    Object r[][]= new Object[4][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    //Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for(int i=0;i<4;i++){
      r1.put(i+"", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for(int i=0;i<4;i++){
      r2.put(i+"", new Portfolio(i));
    }

    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    for(int i=0;i<4;i++){
      r3.put(i+"", new Employee("empName",(20+i),i,"Mr.",(5000+i),add1));
    }
    String queries[] = {
        "SELECT DISTINCT * FROM /portfolio1 pf1, pf1.positions.values posit1, /portfolio2 pf2, /employees e WHERE posit1.secId='IBM'"
    };
    //Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    //Create Indexes
    qs.createIndex("statusIndexPf1",IndexType.FUNCTIONAL,"status","/portfolio1");
    qs.createIndex("secIdIndexPf1",IndexType.FUNCTIONAL,"posit1.secId","/portfolio1 pf1, pf1.positions.values posit1");

    //Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if(!observer.isIndexesUsed){
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        assertEquals("secIdIndexPf1",itr.next().toString());

        int indxs = observer.indexesUsed.size();
        CacheUtils.log("**************************************************Indexes Used :::::: "+indxs+" Index Name: "+observer.indexName);
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length,queries);
  }//end of test8

  @Test
  public void testBasicCompositeIndexUsage() throws Exception {
    try {
      IndexManager.TEST_RANGEINDEX_ONLY = true;

      QueryService qs;
      qs = CacheUtils.getQueryService();
      Position.resetCounter();
      //Create Regions
      Region r1 = CacheUtils.createRegion("portfolio", Portfolio.class);
      for(int i=0;i<1000;i++){
        r1.put(i+"", new Portfolio(i));
      }
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));

      Region r2 = CacheUtils.createRegion("employee", Employee.class);
      for(int i=0;i<1000;i++){
        r2.put(i+"", new Employee("empName",(20+i), i /*empId*/,"Mr.",(5000+i),add1));
      }       



      String queriesWithResCount[][] = {
          // Test case No. IUMR021
          {"SELECT DISTINCT * FROM /portfolio pf, /employee emp WHERE pf.ID = emp.empId", 1000+""},
          {"SELECT * FROM /portfolio pf, /employee emp WHERE pf.ID = emp.empId", ""+1000},
          {"SELECT pf.status, emp.empId, pf.getType() FROM /portfolio pf, /employee emp WHERE pf.ID = emp.empId", ""+1000},
          /*
           * Following query returns more (999001) than expected (1000) results as pf.ID > 0 conditions is evalusted first for
           * all Portfolio and Employee objects (999 * 1000) and then other condition with AND is executed for pf.ID = 0 and 
           * pf.status = ''active' and pf.ID = emp.ID. So total results are 999001.
           * 
           * For expected results correct parenthesis must be used as in next query.
           * 
           */
          {"SELECT pf.status, emp.empId, pf.getType() FROM /portfolio pf, /employee emp WHERE pf.ID = emp.empId AND pf.status='active' OR pf.ID > 0", ""+999001},
          {"SELECT * FROM /portfolio pf, /employee emp WHERE pf.ID = emp.empId AND (pf.status='active' OR pf.ID > 499)", ""+750}, 
          {"SELECT pf.status, emp.empId, pf.getType() FROM /portfolio pf, /employee emp WHERE pf.ID = emp.empId AND (pf.status='active' OR pf.ID > 499)", ""+750},
          //"SELECT DISTINCT * FROM /portfolio1 pf1, pf1.positions.values posit1, /portfolio2 pf2, /employees e WHERE posit1.secId='IBM'"

      };

      String queries[] = new String[queriesWithResCount.length];
      Object r[][]= new Object[queries.length][2];

      //Execute Queries without Indexes
      long t1 = -1;
      long t2 = -1;
      for (int i = 0; i < queries.length; i++) {
        Query q = null;
        try {
          queries[i]=queriesWithResCount[i][0];
          q = CacheUtils.getQueryService().newQuery(queries[i]);
          CacheUtils.getLogger().info("Executing query: " + queries[i]);
          t1 = System.currentTimeMillis();
          r[i][0] = q.execute();
          assertTrue(r[i][0] instanceof SelectResults);
          assertEquals(Integer.parseInt(queriesWithResCount[i][1]), ((SelectResults)r[i][0]).size());
          t2 = System.currentTimeMillis();
          float x = (t2-t1)/1000f;
          CacheUtils.log("Executing query: " + queries[i] + " without index. Time taken is " +x + "seconds");
        } catch (Exception e) {
          e.printStackTrace();
          fail(q.getQueryString());
        }
      }
      //Create Indexes
      qs.createIndex("idIndexPf",IndexType.FUNCTIONAL,"ID","/portfolio");
      qs.createIndex("statusIndexPf",IndexType.FUNCTIONAL,"status","/portfolio");
      qs.createIndex("empIdIndexPf2",IndexType.FUNCTIONAL,"empId","/employee");

      //Execute Queries with Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = null;
        try {
          q = CacheUtils.getQueryService().newQuery(queries[i]);
          CacheUtils.getLogger().info("Executing query: " + queries[i]);
          QueryObserverImpl observer = new QueryObserverImpl();
          QueryObserverHolder.setInstance(observer);
          t1 = System.currentTimeMillis();
          r[i][1] = q.execute();
          assertTrue(r[i][0] instanceof SelectResults);
          assertEquals(Integer.parseInt(queriesWithResCount[i][1]), ((SelectResults)r[i][0]).size());
          t2 = System.currentTimeMillis();
          float x = (t2-t1)/1000f;
          CacheUtils.log("Executing query: " + queries[i] + " with index created. Time taken is " + x + "seconds");
          if(!observer.isIndexesUsed && i != 3 /* For join query without parenthesis */){
            fail("Index is NOT uesd for query" + queries[i]);
          }

          Iterator itr = observer.indexesUsed.iterator();
          while(itr.hasNext()){
            String temp = itr.next().toString();
            if(  !(temp.equals("idIndexPf") || temp.equals("empIdIndexPf2") || temp.equals("statusIndexPf"))){
              fail("<idIndexPf> or <empIdIndexPf2>    was expected but found "+temp.toString());
            }
            //assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
          }

          if (i != 3 /* For join query without parenthesis */) {
            int indxs = observer.indexesUsed.size();
            assertTrue("Indexes used is not of size >= 2",indxs >= 2);
            CacheUtils.log("**************************************************Indexes Used :::::: "+indxs+" Index Name: "+observer.indexName);
          }

        } catch (Exception e) {
          e.printStackTrace();
          fail(q.getQueryString());
        }
      }
      StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
      ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length, queries);
    }finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;

    }
  }//end of test1


  // !!!:ezoerner:20081031 disabled because it throws 
  // UnsupportedOperationException: Cannot do an equi-join between a region having a RangeIndex and a region having a CompactRangeIndex.
  @Test
  public void testBasicCompositeIndexUsageWithOneIndexExpansionAndTruncation() throws Exception {
    try {
      IndexManager.TEST_RANGEINDEX_ONLY = true;

      Object r[][]= new Object[1][2];
      QueryService qs;
      qs = CacheUtils.getQueryService();
      Position.resetCounter();
      //Create Regions
      Region r1 = CacheUtils.createRegion("portfolio", Portfolio.class);
      for(int i=0;i<1000;i++){
        r1.put(i+"", new Portfolio(i));
      }
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));

      Region r2 = CacheUtils.createRegion("employee", Employee.class);
      for(int i=0;i<1000;i++){
        r2.put(i+"", new Employee("empName",(20+i),i,"Mr.",(5000+i),add1));
      }       



      String queries[] = {
          // Test case No. IUMR021
          "SELECT DISTINCT * FROM /portfolio pf, pf.positions pos, /employee emp WHERE pf.iD = emp.empId",
          //"SELECT DISTINCT * FROM /portfolio1 pf1, pf1.positions.values posit1, /portfolio2 pf2, /employees e WHERE posit1.secId='IBM'"

      };
      //Execute Queries without Indexes
      long t1 = -1;
      long t2 = -1;
      for (int i = 0; i < queries.length; i++) {
        Query q = null;
        try {
          q = CacheUtils.getQueryService().newQuery(queries[i]);
          CacheUtils.getLogger().info("Executing query: " + queries[i]);
          t1 = System.currentTimeMillis();
          r[i][0] = q.execute();
          t2 = System.currentTimeMillis();
          float x = (t2-t1)/1000f;

          CacheUtils.log("Executing query: " + queries[i] + " without index. Time taken is " + x + "seconds");
        } catch (Exception e) {
          e.printStackTrace();
          fail(q.getQueryString());
        }
      }
      //Create Indexes
      qs.createIndex("idIndexPf",IndexType.FUNCTIONAL,"iD","/portfolio pf , pf.collectionHolderMap");
      qs.createIndex("empIdIndexPf2",IndexType.FUNCTIONAL,"empId","/employee");

      //Execute Queries with Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = null;
        try {
          q = CacheUtils.getQueryService().newQuery(queries[i]);
          CacheUtils.getLogger().info("Executing query: " + queries[i]);
          QueryObserverImpl observer = new QueryObserverImpl();
          QueryObserverHolder.setInstance(observer);
          t1 = System.currentTimeMillis();
          r[i][1] = q.execute();
          t2 = System.currentTimeMillis();
          float x = (t2-t1)/1000f;
          CacheUtils.log("Executing query: " + queries[i] + " with index created. Time taken is " +  x + " seconds");
          if(!observer.isIndexesUsed){
            fail("Index is NOT uesd");
          }

          Iterator itr = observer.indexesUsed.iterator();
          while(itr.hasNext()){
            String temp = itr.next().toString();
            if(  !(temp.equals("idIndexPf") || temp.equals("empIdIndexPf2"))){
              fail("<idIndexPf> or <empIdIndexPf2>    was expected but found "+temp.toString());
            }
            //assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
          }

          int indxs = observer.indexesUsed.size();
          assertTrue("Indexes used is not of size = 2",indxs == 2);
          CacheUtils.log("**************************************************Indexes Used :::::: "+indxs+" Index Name: "+observer.indexName);

        } catch (Exception e) {
          e.printStackTrace();
          fail(q.getQueryString());
        }
      }
      StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
      ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length,queries);
    }finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;

    }
  }//end of test1

  //Asif: Equijoin between range index & compact index is not supported so 
  // allowing the  test to run with Range index only
  @Test
  public void testBasicCompositeIndexUsageWithMultipleIndexes()
      throws Exception
  {
    try {
      IndexManager.TEST_RANGEINDEX_ONLY = true;
      Object r[][] = new Object[1][2];
      QueryService qs;
      qs = CacheUtils.getQueryService();
      Position.resetCounter();
      // Create Regions
      Region r1 = CacheUtils.createRegion("portfolio", Portfolio.class);
      for (int i = 0; i < 1000; i++) {
        r1.put(i + "", new Portfolio(i));
      }
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));

      Region r2 = CacheUtils.createRegion("employee", Employee.class);
      for (int i = 0; i < 1000; i++) {
        r2.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i),
            add1));
      }

      String queries[] = {
          // Test case No. IUMR021
          "SELECT DISTINCT * FROM /portfolio pf, pf.positions pos, /employee emp WHERE pf.iD = emp.empId and pf.status='active' and emp.age > 900",
          // "SELECT DISTINCT * FROM /portfolio1 pf1, pf1.positions.values posit1, /portfolio2 pf2, /employees e WHERE posit1.secId='IBM'"

      };
      // Execute Queries without Indexes
      long t1 = -1;
      long t2 = -1;
      for (int i = 0; i < queries.length; i++) {
        Query q = null;
        try {
          q = CacheUtils.getQueryService().newQuery(queries[i]);
          CacheUtils.getLogger().info("Executing query: " + queries[i]);
          t1 = System.currentTimeMillis();
          r[i][0] = q.execute();
          t2 = System.currentTimeMillis();
          float x = (t2 - t1) / 1000f;

          CacheUtils.log("Executing query: " + queries[i]
              + " without index. Time taken is " + x + "seconds");
        }
        catch (Exception e) {
          e.printStackTrace();
          fail(q.getQueryString());
        }
      }
      // Create Indexes
      qs.createIndex("idIndexPf", IndexType.FUNCTIONAL, "iD",
          "/portfolio pf , pf.collectionHolderMap");
      qs.createIndex("empIdIndexPf2", IndexType.FUNCTIONAL, "empId",
          "/employee");
      // qs.createIndex("statusIndexPf2",IndexType.FUNCTIONAL,"status","/portfolio pf , pf.positions");
      qs.createIndex("ageIndexemp", IndexType.FUNCTIONAL, "age",
          "/employee emp ");
      // Execute Queries with Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = null;
        try {
          q = CacheUtils.getQueryService().newQuery(queries[i]);
          CacheUtils.getLogger().info("Executing query: " + queries[i]);
          QueryObserverImpl observer = new QueryObserverImpl();
          QueryObserverHolder.setInstance(observer);
          t1 = System.currentTimeMillis();
          r[i][1] = q.execute();
          t2 = System.currentTimeMillis();
          float x = (t2 - t1) / 1000f;
          CacheUtils.log("Executing query: " + queries[i]
              + " with index created. Time taken is " + x + " seconds");
          if (!observer.isIndexesUsed) {
            fail("Index is NOT uesd");
          }

          Iterator itr = observer.indexesUsed.iterator();
          while (itr.hasNext()) {
            String temp = itr.next().toString();
            if (!(temp.equals("ageIndexemp") || temp.equals("idIndexPf")
                || temp.equals("empIdIndexPf2") || temp
                .equals("statusIndexPf2"))) {
              fail("<idIndexPf> or <empIdIndexPf2>    was expected but found "
                  + temp.toString());
            }
            // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
          }

          int indxs = observer.indexesUsed.size();
          assertTrue("Indexes used is not of size = 3", indxs == 3);
          System.out
          .println("**************************************************Indexes Used :::::: "
              + indxs + " Index Name: " + observer.indexName);

        }
        catch (Exception e) {
          e.printStackTrace();
          fail(q.getQueryString());
        }
      }
      StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
      ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
    }
    finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
    }
  }//end of test1

  // !!!:ezoerner:20081031 disabled because it throws 
  // UnsupportedOperationException: Cannot do an equi-join between a region having a RangeIndex and a region having a CompactRangeIndex.
  @Test
  public void testAssertionBug() {      
    try { 
      IndexManager.TEST_RANGEINDEX_ONLY = true;       
      Region region1 = CacheUtils.createRegion("Quotes1",Quote.class);
      Region region2 = CacheUtils.createRegion("Quotes2",Quote.class);
      Region region3 = CacheUtils.createRegion("Restricted1",Restricted.class);
      for (int i=0; i<10; i++){            
        region1.put(new Integer(i), new Quote(i));
        region2.put(new Integer(i), new Quote(i));
        region3.put(new Integer(i), new Restricted(i));
      }
      QueryService qs = CacheUtils.getQueryService();
      //////////creating indexes on region Quotes1
      qs.createIndex("Quotes1Region-quoteIdStrIndex", IndexType.PRIMARY_KEY, "q.quoteIdStr", "/Quotes1 q");

      //qs.createIndex("Quotes1Region-cusipIndex1", IndexType.FUNCTIONAL, "q.cusip", "/Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Region-quoteTypeIndex", IndexType.FUNCTIONAL, "q.quoteType", "/Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-dealerPortfolioIndex", IndexType.FUNCTIONAL, "q.dealerPortfolio", "/Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-channelNameIndex", IndexType.FUNCTIONAL, "q.channelName", "/Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-priceTypeIndex", IndexType.FUNCTIONAL, "q.priceType", "/Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-lowerQtyIndex", IndexType.FUNCTIONAL, "q.lowerQty", "/Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Region-upperQtyIndex", IndexType.FUNCTIONAL, "q.upperQty", "/Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Restricted-quoteTypeIndex", IndexType.FUNCTIONAL, "r.quoteType", "/Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Restricted-minQtyIndex", IndexType.FUNCTIONAL, "r.minQty", "/Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Restricted-maxQtyIndex", IndexType.FUNCTIONAL, "r.maxQty", "/Quotes1 q, q.restrict r");

      //////////creating indexes on region Quotes2
      qs.createIndex("Quotes2Region-quoteIdStrIndex", IndexType.PRIMARY_KEY, "q.quoteIdStr", "/Quotes2 q");

      //qs.createIndex("Quotes1Region-cusipIndex2", IndexType.FUNCTIONAL, "q.cusip", "/Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Region-quoteTypeIndex", IndexType.FUNCTIONAL, "q.quoteType", "/Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-dealerPortfolioIndex", IndexType.FUNCTIONAL, "q.dealerPortfolio", "/Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-channelNameIndex", IndexType.FUNCTIONAL, "q.channelName", "/Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-priceTypeIndex", IndexType.FUNCTIONAL, "q.priceType", "/Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-lowerQtyIndex", IndexType.FUNCTIONAL, "q.lowerQty", "/Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Region-upperQtyIndex", IndexType.FUNCTIONAL, "q.upperQty", "/Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Restricted-quoteTypeIndex", IndexType.FUNCTIONAL, "r.quoteType", "/Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Restricted-minQtyIndex", IndexType.FUNCTIONAL, "r.minQty", "/Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Restricted-maxQtyIndex", IndexType.FUNCTIONAL, "r.maxQty", "/Quotes2 q, q.restrict r");

      //////////creating indexes on region Restricted1
      //qs.createIndex("RestrictedRegion-cusip", IndexType.FUNCTIONAL, "r.cusip", "/Restricted1 r");

      qs.createIndex("RestrictedRegion-quoteTypeIndex", IndexType.FUNCTIONAL, "r.quoteType", "/Restricted1 r");
      qs.createIndex("RestrictedRegion-minQtyIndex", IndexType.FUNCTIONAL, "r.minQty", "/Restricted1 r");
      qs.createIndex("RestrictedRegion-maxQtyIndex-1", IndexType.FUNCTIONAL, "r.maxQty", "/Restricted1 r"); 
      Query q = qs.newQuery("SELECT DISTINCT  q.cusip, q.quoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q, /Restricted1 r WHERE q.cusip = r.cusip AND q.quoteType = r.quoteType");
      q.execute();
    }catch(Exception e) {
      e.printStackTrace();
      fail("Test failed bcoz of exception "+e);
    }finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
    }
  }


  // !!!:ezoerner:20081031 disabled because it throws 
  // UnsupportedOperationException: Cannot do an equi-join between a region having a RangeIndex and a region having a CompactRangeIndex.
  @Test
  public void testBasicCompositeIndexUsageInAllGroupJunction() throws Exception {
    try {
      IndexManager.TEST_RANGEINDEX_ONLY = true;

      Object r[][]= new Object[1][2];
      QueryService qs;
      qs = CacheUtils.getQueryService();
      Position.resetCounter();
      //Create Regions
      Region r1 = CacheUtils.createRegion("portfolio", Portfolio.class);
      for(int i=0;i<100;i++){
        r1.put(i+"", new Portfolio(i));
      }

      Region r3 = CacheUtils.createRegion("portfolio3", Portfolio.class);
      for(int i=0;i<10;i++){
        r3.put(i+"", new Portfolio(i));
      }
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));

      Region r2 = CacheUtils.createRegion("employee", Employee.class);
      for(int i=0;i<100;i++){
        r2.put(i+"", new Employee("empName",(20+i),i,"Mr.",(5000+i),add1));
      }       



      String queries[] = {
          // Test case No. IUMR021
          "SELECT DISTINCT * FROM /portfolio pf, pf.positions pos, /portfolio3 pf3, /employee emp WHERE pf.iD = emp.empId and pf.status='active' and emp.age > 50 and pf3.status='active'",
          //"SELECT DISTINCT * FROM /portfolio1 pf1, pf1.positions.values posit1, /portfolio2 pf2, /employees e WHERE posit1.secId='IBM'"

      };
      //Execute Queries without Indexes
      long t1 = -1;
      long t2 = -1;
      for (int i = 0; i < queries.length; i++) {
        Query q = null;
        try {
          q = CacheUtils.getQueryService().newQuery(queries[i]);
          CacheUtils.getLogger().info("Executing query: " + queries[i]);
          t1 = System.currentTimeMillis();
          r[i][0] = q.execute();
          t2 = System.currentTimeMillis();
          float x = (t2-t1)/1000f;

          CacheUtils.log("Executing query: " + queries[i] + " without index. Time taken is " + x + "seconds");
        } catch (Exception e) {
          e.printStackTrace();
          fail(q.getQueryString());
        }
      }
      //Create Indexes
      qs.createIndex("idIndexPf",IndexType.FUNCTIONAL,"iD","/portfolio pf , pf.collectionHolderMap");
      qs.createIndex("empIdIndexPf2",IndexType.FUNCTIONAL,"empId","/employee");
      qs.createIndex("statusIndexPf3",IndexType.FUNCTIONAL,"status","/portfolio3 pf3 ");
      qs.createIndex("ageIndexemp",IndexType.FUNCTIONAL,"age","/employee emp ");
      //Execute Queries with Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = null;
        try {
          q = CacheUtils.getQueryService().newQuery(queries[i]);
          CacheUtils.getLogger().info("Executing query: " + queries[i]);
          QueryObserverImpl observer = new QueryObserverImpl();
          QueryObserverHolder.setInstance(observer);
          t1 = System.currentTimeMillis();
          r[i][1] = q.execute();
          t2 = System.currentTimeMillis();
          float x = (t2-t1)/1000f;
          CacheUtils.log("Executing query: " + queries[i] + " with index created. Time taken is " +  x + " seconds");
          if(!observer.isIndexesUsed){
            fail("Index is NOT uesd");
          }

          Iterator itr = observer.indexesUsed.iterator();
          while(itr.hasNext()){
            String temp = itr.next().toString();
            if(  !(temp.equals("ageIndexemp")||temp.equals("idIndexPf") || temp.equals("empIdIndexPf2") || temp.equals("statusIndexPf3"))){
              fail("<idIndexPf> or <empIdIndexPf2>    was expected but found "+temp.toString());
            }
            //assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
          }

          int indxs = observer.indexesUsed.size();
          assertTrue("Indexes used is not of size = 4 but of size = "+indxs,indxs == 4);
          CacheUtils.log("**************************************************Indexes Used :::::: "+indxs+" Index Name: "+observer.indexName);

        } catch (Exception e) {
          e.printStackTrace();
          fail(q.getQueryString());
        }
      }
      StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
      ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length,queries);
    }finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;

    }
  }//end of test1

  class QueryObserverImpl extends QueryObserverAdapter{
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();
    String indexName;
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexName = index.getName();
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if(results != null){
        isIndexesUsed = true;
      }
    }
  }


}//end of class
