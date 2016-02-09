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
package com.gemstone.gemfire.management;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.dunit.QueryAPITestPartitionResolver;
import com.gemstone.gemfire.cache.query.dunit.QueryUsingFunctionContextDUnitTest;
import com.gemstone.gemfire.cache.query.partitioned.PRQueryDUnitHelper;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.partitioned.fixed.SingleHopQuarterPartitionResolver;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.ManagementStrings;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.beans.BeanUtilFuncs;
import com.gemstone.gemfire.management.internal.cli.json.TypedJson;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;
import com.gemstone.gemfire.pdx.internal.PdxInstanceFactoryImpl;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * @author rishim
 * 
 */

// 1) Test Basic Json Strings for Partitioned Regions
// Test Basic Json Strings for Replicated Regions
// Test for all Region Types
// Test for primitive types
// Test for Nested Objects
// Test for Enums
// Test for collections
// Test for huge collection
// Test PDX types
// Test different projects type e.g. SelectResult, normal bean etc..
// Test Colocated Regions
// Test for Limit ( both row count and Depth)
// ORDER by orders
// Test all attributes are covered in an complex type

public class QueryDataDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  private static final int MAX_WAIT = 100 * 1000;

  private static final int cntDest = 30;

  private static final int cnt = 0;

  // PR 5 is co-located with 4
  static String PartitionedRegionName1 = "TestPartitionedRegion1"; // default
                                                                   // name
  static String PartitionedRegionName2 = "TestPartitionedRegion2"; // default
                                                                   // name
  static String PartitionedRegionName3 = "TestPartitionedRegion3"; // default
                                                                   // name
  static String PartitionedRegionName4 = "TestPartitionedRegion4"; // default
                                                                   // name
  static String PartitionedRegionName5 = "TestPartitionedRegion5"; // default
                                                                   // name

  
  static String repRegionName = "TestRepRegion"; // default name
  static String repRegionName2 = "TestRepRegion2"; // default name
  static String repRegionName3 = "TestRepRegion3"; // default name
  static String repRegionName4 = "TestRepRegion4"; // default name
  static String localRegionName = "TestLocalRegion"; // default name

  private static PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  public static String[] queries = new String[] {
      "select * from /" + PartitionedRegionName1 + " where ID>=0",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2 where r1.ID = r2.ID",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2
          + " r2 where r1.ID = r2.ID AND r1.status = r2.status",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2, /"
          + PartitionedRegionName3 + " r3 where r1.ID = r2.ID and r2.ID = r3.ID",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2, /"
          + PartitionedRegionName3 + " r3  , /" + repRegionName
          + " r4 where r1.ID = r2.ID and r2.ID = r3.ID and r3.ID = r4.ID",
      "Select * from /" + PartitionedRegionName4 + " r4 , /" + PartitionedRegionName5 + " r5 where r4.ID = r5.ID" };

  public static String[] nonColocatedQueries = new String[] {
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName4 + " r4 where r1.ID = r4.ID",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName4 + " r4 , /"
          + PartitionedRegionName5 + " r5 where r1.ID = r42.ID and r4.ID = r5.ID" };

  public static String[] queriesForRR = new String[] { "<trace> select * from /" + repRegionName + " where ID>=0",
      "Select * from /" + repRegionName + " r1, /" + repRegionName2 + " r2 where r1.ID = r2.ID",
      "select * from /" + repRegionName3 + " where ID>=0" };
  
  public static String[] queriesForLimit = new String[] { "select * from /" + repRegionName4 };


  public QueryDataDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    initManagement(false);


  }

  private void initCommonRegions(){
    createRegionsInNodes();
    fillValuesInRegions();
  }

  /**
   * This function puts portfolio objects into the created Region (PR or Local)
   * *
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPRPuts(final String regionName,
      final Object[] portfolio, final int from, final int to) {
    SerializableRunnable puts = new CacheSerializableRunnable("Region Puts") {
      @Override
      public void run2() throws CacheException {
        Cache cache = CacheFactory.getAnyInstance();
        Region region = cache.getRegion(regionName);
        for (int j = from; j < to; j++)
          region.put(new Integer(j), portfolio[j]);
        LogWriterUtils.getLogWriter()
            .info(
                "PRQueryDUnitHelper#getCacheSerializableRunnableForPRPuts: Inserted Portfolio data on Region "
                    + regionName);
      }
    };
    return (CacheSerializableRunnable) puts;
  }

  /**
   * This function puts PDX objects into the created Region (REPLICATED) *
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPDXPuts(final String regionName) {
    SerializableRunnable puts = new CacheSerializableRunnable("Region Puts") {
      @Override
      public void run2() throws CacheException {
        putPdxInstances(regionName);

      }
    };
    return (CacheSerializableRunnable) puts;
  }
  
  /**
   * This function puts big collections to created Region (REPLICATED) *
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForBigCollPuts(final String regionName) {
    SerializableRunnable bigPuts = new CacheSerializableRunnable("Big Coll Puts") {
      @Override
      public void run2() throws CacheException {
        putBigInstances(regionName);

      }
    };
    return (CacheSerializableRunnable) bigPuts;
  }

  public void fillValuesInRegions() {
    // Create common Portflios and NewPortfolios
    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);

    // Fill local region
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(localRegionName, portfolio, cnt, cntDest));

    // Fill replicated region
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(repRegionName, portfolio, cnt, cntDest));
    managedNode2.invoke(getCacheSerializableRunnableForPRPuts(repRegionName2, portfolio, cnt, cntDest));

    // Fill Partition Region
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName1, portfolio, cnt, cntDest));
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName2, portfolio, cnt, cntDest));
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName3, portfolio, cnt, cntDest));
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName4, portfolio, cnt, cntDest));
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName5, portfolio, cnt, cntDest));

    managedNode1.invoke(getCacheSerializableRunnableForPDXPuts(repRegionName3));

  }

  public void putPdxInstances(String regionName) throws CacheException {
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    Region r = getCache().getRegion(regionName);
    pf.writeInt("ID", 111);
    pf.writeString("status", "active");
    pf.writeString("secId", "IBM");
    PdxInstance pi = pf.create();
    r.put("IBM", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 222);
    pf.writeString("status", "inactive");
    pf.writeString("secId", "YHOO");
    pi = pf.create();
    r.put("YHOO", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 333);
    pf.writeString("status", "active");
    pf.writeString("secId", "GOOGL");
    pi = pf.create();
    r.put("GOOGL", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 111);
    pf.writeString("status", "inactive");
    pf.writeString("secId", "VMW");
    pi = pf.create();
    r.put("VMW", pi);
  }
  
  public void putBigInstances(String regionName) throws CacheException {
    Region r = getCache().getRegion(regionName);

    for(int i = 0 ; i < 1200 ; i++){
      List<String> bigColl1 = new ArrayList<String>();
      for(int j = 0; j< 200 ; j++){
        bigColl1.add("BigColl_1_ElemenNo_"+j);
      }
      r.put("BigColl_1_"+i, bigColl1);
    }
    
  }

  private void createRegionsInNodes() {

    // Create local Region on servers
    managedNode1.invoke(QueryUsingFunctionContextDUnitTest.class, "createLocalRegion");

    // Create ReplicatedRegion on servers
    managedNode1.invoke(QueryUsingFunctionContextDUnitTest.class, "createReplicatedRegion");
    managedNode2.invoke(QueryUsingFunctionContextDUnitTest.class, "createReplicatedRegion");
    managedNode3.invoke(QueryUsingFunctionContextDUnitTest.class, "createReplicatedRegion");
    try {
      this.createDistributedRegion(managedNode2, repRegionName2);
      this.createDistributedRegion(managedNode1, repRegionName3);
      this.createDistributedRegion(managedNode1, repRegionName4);
    } catch (Exception e1) {
      fail("Test Failed while creating region " + e1.getMessage());
    }

    // Create two colocated PartitionedRegions On Servers.
    managedNode1.invoke(QueryUsingFunctionContextDUnitTest.class, "createColocatedPR");
    managedNode2.invoke(QueryUsingFunctionContextDUnitTest.class, "createColocatedPR");
    managedNode3.invoke(QueryUsingFunctionContextDUnitTest.class, "createColocatedPR");

    this.managingNode.invoke(new SerializableRunnable("Wait for all Region Proxies to get replicated") {

      public void run() {
        Cache cache = getCache();
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();

        try {
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName1, 3);
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName2, 3);
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName3, 3);
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName4, 3);
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName5, 3);
          MBeanUtil.getDistributedRegionMbean("/" + repRegionName, 3);
          MBeanUtil.getDistributedRegionMbean("/" + repRegionName2, 1);
          MBeanUtil.getDistributedRegionMbean("/" + repRegionName3, 1);
          MBeanUtil.getDistributedRegionMbean("/" + repRegionName4, 1);
        } catch (Exception e) {
          fail("Region proxies not replicated in time");
        }
      }
    });

  }

  // disabled for bug 49698, serialization problems introduced by r44615
  public void testQueryOnPartitionedRegion() throws Exception {

    final DistributedMember member1 = getMember(managedNode1);
    final DistributedMember member2 = getMember(managedNode2);
    final DistributedMember member3 = getMember(managedNode3);
    
    initCommonRegions();
    
    
    this.managingNode.invoke(new SerializableRunnable("testQueryOnPartitionedRegion") {

      public void run() {
        Cache cache = getCache();
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();

        assertNotNull(bean);

        try {
          for (int i = 0; i < queries.length; i++) {
            String jsonString = null;
            if (i == 0) {
              jsonString = bean.queryData(queries[i], null, 10);
              if (jsonString.contains("result") && !jsonString.contains("No Data Found")) {
               
                //getLogWriter().info("testQueryOnPartitionedRegion" + queries[i] + " is = " + jsonString);
                JSONObject jsonObj = new JSONObject(jsonString);  
              } else {
                fail("Query On Cluster should have result");
              }
            } else {
              jsonString = bean.queryData(queries[i], member1.getId(), 10);
              if (jsonString.contains("member")) {
                JSONObject jsonObj = new JSONObject(jsonString);
                //getLogWriter().info("testQueryOnPartitionedRegion" + queries[i] + " is = " + jsonString);
              } else {
                fail("Query On Member should have member");
              }
            }

            

          }
        } catch (JSONException e) {
          e.printStackTrace();
          fail(e.getMessage());
        } catch (Exception e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    });
  }

  public void testQueryOnReplicatedRegion() throws Exception {

    
    initCommonRegions();
    
    
    this.managingNode.invoke(new SerializableRunnable("Query Test For REPL1") {

      
      public void run() {
        Cache cache = getCache();
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);

        try {
          for (int i = 0; i < queriesForRR.length; i++) {
            String jsonString1 = null;
            if (i == 0) {
              jsonString1 = bean.queryData(queriesForRR[i], null, 10);
              if (jsonString1.contains("result") && !jsonString1.contains("No Data Found")) {
                JSONObject jsonObj = new JSONObject(jsonString1);
              } else {
                fail("Query On Cluster should have result");
              }
            } else {
              jsonString1 = bean.queryData(queriesForRR[i], null, 10);
              if (jsonString1.contains("result")) {
                JSONObject jsonObj = new JSONObject(jsonString1);
              } else {
                LogWriterUtils.getLogWriter().info("Failed Test String" + queriesForRR[i] + " is = " + jsonString1);
                fail("Join on Replicated did not work.");
              }
            }
          }

        } catch (JSONException e) {
          fail(e.getMessage());
        } catch (IOException e) {
          fail(e.getMessage());
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }
    });
  }
  
  public void testMemberWise() throws Exception {

    final DistributedMember member1 = getMember(managedNode1);
    final DistributedMember member2 = getMember(managedNode2);
    
    
    initCommonRegions();
    
    
    this.managingNode.invoke(new SerializableRunnable("testMemberWise") {

      public void run() {
        Cache cache = getCache();
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);

        try {
          byte[] bytes = bean.queryDataForCompressedResult(queriesForRR[0], member1.getId() + "," + member2.getId(), 2);
          String jsonString = BeanUtilFuncs.decompress(bytes);
          JSONObject jsonObj = new JSONObject(jsonString);
          //String memberID = (String)jsonObj.get("member");
          
          //getLogWriter().info("testMemberWise " + queriesForRR[2] + " is = " + jsonString);

        } catch (JSONException e) {
          fail(e.getMessage());
        } catch (IOException e) {
          fail(e.getMessage());
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }
    });
  }

  
 
  public void testLimitForQuery() throws Exception {
    
    initCommonRegions();
    managedNode1.invoke(getCacheSerializableRunnableForBigCollPuts(repRegionName4));
    
    managingNode.invoke(new SerializableRunnable("testLimitForQuery") {
      public void run() {
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);

        try {

          // Query With Default values
          assertEquals(TypedJson.DEFAULT_COLLECTION_ELEMENT_LIMIT, bean.getQueryCollectionsDepth());
          assertEquals(ManagementConstants.DEFAULT_QUERY_LIMIT, bean.getQueryResultSetLimit());

          String jsonString1 = bean.queryData(queriesForLimit[0], null, 0);
          if (jsonString1.contains("result") && !jsonString1.contains("No Data Found")) {
            JSONObject jsonObj = new JSONObject(jsonString1);
            assertTrue(jsonString1.contains("BigColl_1_ElemenNo_"));
            JSONArray arr = jsonObj.getJSONArray("result");
            assertEquals(ManagementConstants.DEFAULT_QUERY_LIMIT, arr.length());
            // Get the first element

            JSONArray array1 = (JSONArray) arr.getJSONArray(0);
            // Get the ObjectValue

            JSONObject collectionObject = (JSONObject) array1.get(1);
            assertEquals(100, collectionObject.length());

          } else {
            fail("Query On Cluster should have result");
          }

          // Query With Ovverride Values
          
          int newQueryCollectionDepth = 150;
          int newQueryResultSetLimit = 500;
          bean.setQueryCollectionsDepth(newQueryCollectionDepth);
          bean.setQueryResultSetLimit(newQueryResultSetLimit);
          
          assertEquals(newQueryCollectionDepth, bean.getQueryCollectionsDepth());
          assertEquals(newQueryResultSetLimit, bean.getQueryResultSetLimit());

          jsonString1 = bean.queryData(queriesForLimit[0], null, 0);
          if (jsonString1.contains("result") && !jsonString1.contains("No Data Found")) {
            JSONObject jsonObj = new JSONObject(jsonString1);
            assertTrue(jsonString1.contains("BigColl_1_ElemenNo_"));
            JSONArray arr = jsonObj.getJSONArray("result");
            assertEquals(newQueryResultSetLimit, arr.length());
            // Get the first element

            JSONArray array1 = (JSONArray) arr.getJSONArray(0);
            // Get the ObjectValue

            JSONObject collectionObject = (JSONObject) array1.get(1);
            assertEquals(newQueryCollectionDepth, collectionObject.length());

          } else {
            fail("Query On Cluster should have result");
          }

        } catch (JSONException e) {
          fail(e.getMessage());
        } catch (IOException e) {
          fail(e.getMessage());
        } catch (Exception e) {
          fail(e.getMessage());
        }

      }
    });
  }

  public void testErrors() throws Exception{
    
    final DistributedMember member1 = getMember(managedNode1);
    final DistributedMember member2 = getMember(managedNode2);
    final DistributedMember member3 = getMember(managedNode3);
    
    initCommonRegions();
    
    this.managingNode.invoke(new SerializableRunnable("Test Error") {
      public void run() {
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);

        try {
          Cache cache = getCache();
          try {
            String message = bean.queryData("Select * from TestPartitionedRegion1", null, 2); 
            
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("message", ManagementStrings.QUERY__MSG__INVALID_QUERY.toLocalizedString("Region mentioned in query probably missing /"));
            String expectedMessage = jsonObject.toString();
            assertEquals(expectedMessage,message);
            
          } catch (Exception e) {
            fail(e.getLocalizedMessage());
          }
          
          try {
            String query = "Select * from /PartitionedRegionName9 r1, PartitionedRegionName2 r2 where r1.ID = r2.ID";
            String message = bean.queryData(query, null, 2);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("message", ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND.toLocalizedString("/PartitionedRegionName9"));
            String expectedMessage = jsonObject.toString();
            assertEquals(expectedMessage,message);
          } catch (Exception e) {
            fail(e.getLocalizedMessage());
          
          }
          
          final String testTemp = "testTemp";
          try {
            RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
            
            rf.create(testTemp);
            String query = "Select * from /"+testTemp;
            
            String message = bean.queryData(query, member1.getId(), 2);
            
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("message", ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND_ON_MEMBERS.toLocalizedString("/"+testTemp));
            String expectedMessage = jsonObject.toString();
            assertEquals(expectedMessage,message);
          } catch (Exception e) {
            fail(e.getLocalizedMessage());
          }
          
          try {
            String query = queries[1];            
            String message = bean.queryData(query,null, 2);
            
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("message", ManagementStrings.QUERY__MSG__JOIN_OP_EX.toLocalizedString());
            String expectedMessage = jsonObject.toString();
            
            assertEquals(expectedMessage,message);
          } catch (Exception e) {
            fail(e.getLocalizedMessage());
          }

        } catch (Exception e) {
          fail(e.getMessage());
        }

      }
    });
  }
  
 public void testNormalRegions() throws Exception{
    
    final DistributedMember member1 = getMember(managedNode1);
    final DistributedMember member2 = getMember(managedNode2);
    final DistributedMember member3 = getMember(managedNode3);
    initCommonRegions();
    
    this.managingNode.invoke(new SerializableRunnable("Test Error") {
      public void run() {
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);
        final String testNormal = "testNormal";
        final String testTemp = "testTemp";
        
        final String testSNormal = "testSNormal"; // to Reverse order of regions while getting Random region in QueryDataFunction
        final String testATemp = "testATemp";
        
        try {
          Cache cache = getCache();
          RegionFactory rf = cache.createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU);          
          rf.create(testNormal);
          rf.create(testSNormal);
          
          
          Region region = cache.getRegion("/"+testNormal);
          assertTrue(region.getAttributes().getDataPolicy() == DataPolicy.NORMAL);
          
          RegionFactory rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE);
          rf1.create(testTemp);
          rf1.create(testATemp);
          String query1 = "Select * from /testTemp r1,/testNormal r2 where r1.ID = r2.ID";
          String query2 = "Select * from /testSNormal r1,/testATemp r2 where r1.ID = r2.ID";
          String query3 = "Select * from /testSNormal";
          
          try {
           
            bean.queryDataForCompressedResult(query1,null, 2);
            bean.queryDataForCompressedResult(query2,null, 2);
            bean.queryDataForCompressedResult(query3,null, 2);
          } catch (Exception e) {
            e.printStackTrace();
          }

        } catch (Exception e) {
          fail(e.getMessage());
        }

      }
    });
  }
 
  public void testRegionsLocalDataSet() throws Exception {

    final DistributedMember member1 = getMember(managedNode1);
    final DistributedMember member2 = getMember(managedNode2);
    final DistributedMember member3 = getMember(managedNode3);

    final String PartitionedRegionName6 = "LocalDataSetTest";

    final String[] valArray1 = new String[] { "val1", "val2", "val3" };
    final String[] valArray2 = new String[] { "val4", "val5", "val6" };
    this.managedNode1.invoke(new SerializableRunnable("testRegionsLocalDataSet:Create Region") {
      public void run() {
        try {
    
          Cache cache = getCache();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();

          paf.setRedundantCopies(2).setTotalNumBuckets(12);
          
          List<FixedPartitionAttributes> fpaList = createFixedPartitionList(1);
          for (FixedPartitionAttributes fpa : fpaList) {
            paf.addFixedPartitionAttributes(fpa);
          }
          paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());
          
          RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(paf.create());
              
          Region r = rf.create(PartitionedRegionName6);

          for (int i = 0; i < valArray1.length; i++) {
            r.put(new Date(2013,1,i+5), valArray1[i]);
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail(e.getMessage());
        }

      }
    });

    this.managedNode2.invoke(new SerializableRunnable("testRegionsLocalDataSet: Create Region") {
      public void run() {
        try {

          Cache cache = getCache();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();

          paf.setRedundantCopies(2).setTotalNumBuckets(12);
          
          List<FixedPartitionAttributes> fpaList = createFixedPartitionList(2);
          for (FixedPartitionAttributes fpa : fpaList) {
            paf.addFixedPartitionAttributes(fpa);
          }
          paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());
          
          RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(paf.create());
              
          Region r = rf.create(PartitionedRegionName6);
          
          for (int i = 0; i < valArray2.length; i++) {
            r.put(new Date(2013,5,i+5), valArray2[i]);
          }
          
        } catch (Exception e) {
          fail(e.getMessage());
        }

      }
    });

    this.managedNode3.invoke(new SerializableRunnable("testRegionsLocalDataSet: Create Region") {
      public void run() {
        try {

          Cache cache = getCache();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();

          paf.setRedundantCopies(2).setTotalNumBuckets(12);
          
          List<FixedPartitionAttributes> fpaList = createFixedPartitionList(3);
          for (FixedPartitionAttributes fpa : fpaList) {
            paf.addFixedPartitionAttributes(fpa);
          }
          paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());
          
          RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(paf.create());
              
          Region r = rf.create(PartitionedRegionName6);
          

          
        } catch (Exception e) {
          fail(e.getMessage());
        }

      }
    });

    final List<String> member1RealData = (List<String>)managedNode1.invoke(QueryDataDUnitTest.class, "getLocalDataSet", new Object[] { PartitionedRegionName6 });
   
    final List<String> member2RealData = (List<String>) managedNode2.invoke(QueryDataDUnitTest.class, "getLocalDataSet", new Object[] { PartitionedRegionName6 });
    
    final List<String> member3RealData = (List<String>) managedNode3.invoke(QueryDataDUnitTest.class, "getLocalDataSet", new Object[] { PartitionedRegionName6 });
    

    
    this.managingNode.invoke(new SerializableRunnable("testRegionsLocalDataSet") {
      public void run() {
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);

        try {
          String query = "Select * from /" + PartitionedRegionName6;

          try {
            final DistributedRegionMXBean regionMBean = MBeanUtil.getDistributedRegionMbean("/"
                + PartitionedRegionName6, 3);

            Wait.waitForCriterion(new WaitCriterion() {

              public String description() {
                return "Waiting for all entries to get reflected at managing node";
              }

              public boolean done() {

                boolean done = (regionMBean.getSystemRegionEntryCount() == (valArray1.length + valArray2.length));
                return done;
              }

            }, MAX_WAIT, 1000, true);

            LogWriterUtils.getLogWriter().info("member1RealData  is = " + member1RealData);
            LogWriterUtils.getLogWriter().info("member2RealData  is = " + member2RealData);
            LogWriterUtils.getLogWriter().info("member3RealData  is = " + member3RealData);
            
            String member1Result = bean.queryData(query, member1.getId(), 0);
            LogWriterUtils.getLogWriter().info("member1Result " + query + " is = " + member1Result);


            String member2Result = bean.queryData(query, member2.getId(), 0);
            LogWriterUtils.getLogWriter().info("member2Result " + query + " is = " + member2Result);
            
            String member3Result = bean.queryData(query, member3.getId(), 0);
            LogWriterUtils.getLogWriter().info("member3Result " + query + " is = " + member3Result);
            
            for (String val : member1RealData) {
              assertTrue(member1Result.contains(val));
             }
            
            for (String val : member2RealData) {
              assertTrue(member2Result.contains(val));
            }

            assertTrue(member3Result.contains("No Data Found"));
          } catch (Exception e) {
            fail(e.getMessage());
          }

        } catch (Exception e) {
          fail(e.getMessage());
        }

      }
    });
  }
  
  
  private static List<String> getLocalDataSet(String region){
    PartitionedRegion parRegion = PartitionedRegionHelper.getPartitionedRegion(region, GemFireCacheImpl.getExisting());
    Set<BucketRegion> localPrimaryBucketRegions = parRegion.getDataStore().getAllLocalPrimaryBucketRegions();
    List<String> allPrimaryVals = new ArrayList<String>();
    for(BucketRegion brRegion : localPrimaryBucketRegions){
      for(Object obj : brRegion.values()){
        allPrimaryVals.add((String)obj);
      }
      
    }
    
   return allPrimaryVals;
  }

  /**
   * creates a Fixed Partition List to be used for Fixed Partition Region
   * 
   * @param primaryIndex
   *          index for each fixed partition
   */
  private static List<FixedPartitionAttributes> createFixedPartitionList(int primaryIndex) {
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    if (primaryIndex == 1) {
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 2) {
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 3) {
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));
    }
   return fpaList;
  }
}
