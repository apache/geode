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
package com.gemstone.gemfire.internal.cache.execute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Ignore;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.ClientMetadataService;
import com.gemstone.gemfire.cache.client.internal.ClientPartitionAdvisor;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;

public class SingleHopGetAllPutAllDUnitTest extends PRClientServerTestBase{


  private static final long serialVersionUID = 3873751456134028508L;
  
  public SingleHopGetAllPutAllDUnitTest(String name) {
    super(name);
    
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
 
  /*
   * Do a getAll from client and see if all the values are returned.
   * Will also have to see if the function was routed from client to all the servers
   * hosting the data. 
   */
  @Ignore("Disabled due to bug #50618")
  public void testServerGetAllFunction(){
    createScenario();
    client.invoke(SingleHopGetAllPutAllDUnitTest.class,
        "getAll");
  }  
  
  private void createScenario() {
    ArrayList commonAttributes =  createCommonServerAttributes("TestPartitionedRegion", null, 1, 13, null);
    createClientServerScenarioSingleHop(commonAttributes,20, 20, 20);
  }

  public static void getAll() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final List testValueList = new ArrayList();
    final List testKeyList = new ArrayList();
    for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
      testValueList.add("execKey-" + i);    
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    try {
      int j = 0;
      Map origVals = new HashMap();
      for (Iterator i = testValueList.iterator(); i.hasNext();) {
        testKeyList.add(j);
        Integer key = new Integer(j++);
        Object val = i.next();
        origVals.put(key, val);
        region.put(key, val);
      }

      // check if the client meta-data is in synch
      verifyMetadata();
      
      // check if the function was routed to pruned nodes
      Map resultMap = region.getAll(testKeyList);
      assertTrue(resultMap.equals(origVals));
      pause(2000);
      Map secondResultMap = region.getAll(testKeyList);
      assertTrue(secondResultMap.equals(origVals));
    }
    catch (Exception e) {
      fail("Test failed after the getAll operation", e);
    }
  }
  
  private static void verifyMetadata() {
    Region region = cache.getRegion(PartitionedRegionName);
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion)region);
    
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms
        .getClientPRMetadata_TEST_ONLY();
    
    WaitCriterion wc = new WaitCriterion() {

      public boolean done() {
        return (regionMetaData.size() == 1);
      }

      public String description() {
        return "Region metadat size is not 1. Exisitng size of regionMetaData is "
            + regionMetaData.size();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 5000, 200, true);
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    final ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    wc = new WaitCriterion() {

      public boolean done() {
        return (prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() == 13);
      }

      public String description() {
        return "Bucket server location map size is not 13. Exisitng size is :"
            + prMetaData.getBucketServerLocationsMap_TEST_ONLY().size();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 5000, 200, true);
    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      assertEquals(2, ((List)entry.getValue()).size());
    }
  }
  /*
   * Do a getAll from client and see if all the values are returned.
   * Will also have to see if the function was routed from client to all the servers
   * hosting the data. 
   */
  public void testServerPutAllFunction(){
    createScenario();
    client.invoke(SingleHopGetAllPutAllDUnitTest.class,
        "putAll");
  }
  
  public static void putAll() {
    Region<String, String> region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final Map<String, String> keysValuesMap = new HashMap<String, String>();
    final List<String> testKeysList = new ArrayList<String>();
    for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
      testKeysList.add("execKey-" + i);
      keysValuesMap.put("execKey-" + i, "values-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    try {
      // check if the client meta-data is in synch

      // check if the function was routed to pruned nodes
      region.putAll(keysValuesMap);
      // check the listener
      // check how the function was executed
      pause(2000);
      region.putAll(keysValuesMap);
      
      // check if the client meta-data is in synch
      verifyMetadata();
      
      // check if the function was routed to pruned nodes
      Map<String, String> resultMap = region.getAll(testKeysList);
      assertTrue(resultMap.equals(keysValuesMap));
      pause(2000);
      Map<String, String> secondResultMap = region.getAll(testKeysList);
      assertTrue(secondResultMap.equals(keysValuesMap));

      // Now test removeAll
      region.removeAll(testKeysList);
      HashMap<String, Object> noValueMap = new HashMap<String, Object>();
      for (String key: testKeysList) {
        noValueMap.put(key, null);
      }
      assertEquals(noValueMap, region.getAll(testKeysList));
      pause(2000); // Why does this test keep pausing for 2 seconds and then do the exact same thing?
      region.removeAll(testKeysList);
      assertEquals(noValueMap, region.getAll(testKeysList));
    }
    catch (Exception e) {
      fail("Test failed after the putAll operation", e);
    }
  }
  
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }
}
