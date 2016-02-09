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

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class LocalFunctionExecutionDUnitTest extends DistributedTestCase{
  
  protected static Cache cache = null;

  protected static VM dataStore1 = null;

  protected static Region region = null;
  

  public LocalFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
  }
  
  
  public void testLocalDataSetPR(){
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "createCacheInVm");
    Object args[] = new Object[] { "testRegion", new Integer(1), new Integer(50),
        new Integer(10), null };
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "createPR", args);
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "put");
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "executeFunction");
  }
  
  public void testLocalDataSetDR(){
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "createCacheInVm");
    Object args[] = new Object[] { "testRegion",DataPolicy.REPLICATE };
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "createDR", args);
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "put");
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "executeFunction");
  }
  
  public void testLocalMember(){
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "createCacheInVm");
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "executeFunctionOnMember");
  }

  public static void createCacheInVm() {
    new LocalFunctionExecutionDUnitTest("temp").createCache();
  }

  public void createCache() {
    try {
      Properties props = new Properties();
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    }
    catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }
  
  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, String colocatedWith) {

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy.intValue())
        .setLocalMaxMemory(localMaxMemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith).create();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(prAttr);
    assertNotNull(cache);
    
    region = cache.createRegion(partitionedRegionName, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + partitionedRegionName
            + " created Successfully :" + region);
  }
  

  public static void createDR(String distributedRegionName, DataPolicy dataPolicy) {
    AttributesFactory attr = new AttributesFactory();
    attr.setScope(Scope.DISTRIBUTED_ACK);
    attr.setDataPolicy(dataPolicy);
    assertNotNull(cache);
    region = cache.createRegion(distributedRegionName, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info(
        "Distributed Region " + distributedRegionName
            + " created Successfully :" + region);
  }
  
  
  public static void put() {
    for (int i = 0; i < 120; i++) {
      region.put("YOYO-CUST-KEY-" + i, "YOYO-CUST-VAL-" + i);
    }
  }
  
  public static void executeFunction() {
    try {
      Function function1 = new TestFunction(true,TestFunction.TEST_FUNCTION_EXCEPTION);
      FunctionService.registerFunction(function1);
      ResultCollector rc = FunctionService.onRegion(region).withArgs(Boolean.TRUE).execute(function1.getId());
      rc.getResult();
      Assert.fail("Exception should occur",new Exception("Test Failed"));
    }
    catch (Exception e) {
      assertTrue(e.getMessage().contains("I have been thrown from TestFunction"));
    }    
  }
  
  public static void executeFunctionOnMember() {
    try {
      Function function1 = new TestFunction(true,TestFunction.TEST_FUNCTION_EXCEPTION);
      FunctionService.registerFunction(function1);
      DistributedMember localmember = system.getDistributedMember();
      ResultCollector rc = FunctionService.onMember(system, localmember).withArgs(Boolean.TRUE).execute(function1.getId());
      rc.getResult();
      Assert.fail("Exception should occur",new Exception("Test Failed"));
    }
    catch (Exception e) {
      assertTrue(e.getMessage().contains("I have been thrown from TestFunction"));
      return;
    }    
  }
  
  @Override
  protected final void preTearDown() throws Exception {
    if(cache != null) {
      cache.close();
    }
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() {
      if(cache != null) {
        cache.close();
      }
      cache = null; 
      } });
  }
}
