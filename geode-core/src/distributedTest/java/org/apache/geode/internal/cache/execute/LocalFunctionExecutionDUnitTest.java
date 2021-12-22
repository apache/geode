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
package org.apache.geode.internal.cache.execute;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@Category({FunctionServiceTest.class})
public class LocalFunctionExecutionDUnitTest extends JUnit4DistributedTestCase {

  protected static Cache cache = null;

  protected static VM dataStore1 = null;

  protected static Region region = null;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
  }

  @Test
  public void testLocalDataSetPR() {
    dataStore1.invoke(LocalFunctionExecutionDUnitTest::createCacheInVm);
    Object[] args =
        new Object[] {"testRegion", 1, 50, 10, null};
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "createPR", args);
    dataStore1.invoke(LocalFunctionExecutionDUnitTest::put);
    dataStore1.invoke(LocalFunctionExecutionDUnitTest::executeFunction);
  }

  @Test
  public void testLocalDataSetDR() {
    dataStore1.invoke(LocalFunctionExecutionDUnitTest::createCacheInVm);
    Object[] args = new Object[] {"testRegion", DataPolicy.REPLICATE};
    dataStore1.invoke(LocalFunctionExecutionDUnitTest.class, "createDR", args);
    dataStore1.invoke(LocalFunctionExecutionDUnitTest::put);
    dataStore1.invoke(LocalFunctionExecutionDUnitTest::executeFunction);
  }

  @Test
  public void testLocalMember() {
    dataStore1.invoke(LocalFunctionExecutionDUnitTest::createCacheInVm);
    dataStore1.invoke(LocalFunctionExecutionDUnitTest::executeFunctionOnMember);
  }

  public static void createCacheInVm() {
    new LocalFunctionExecutionDUnitTest().createCache();
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
    } catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, String colocatedWith) {

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy)
        .setLocalMaxMemory(localMaxMemory).setTotalNumBuckets(totalNumBuckets)
        .setColocatedWith(colocatedWith).create();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(prAttr);
    assertNotNull(cache);

    region = cache.createRegion(partitionedRegionName, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region " + partitionedRegionName + " created Successfully :" + region);
  }


  public static void createDR(String distributedRegionName, DataPolicy dataPolicy) {
    AttributesFactory attr = new AttributesFactory();
    attr.setScope(Scope.DISTRIBUTED_ACK);
    attr.setDataPolicy(dataPolicy);
    assertNotNull(cache);
    region = cache.createRegion(distributedRegionName, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region " + distributedRegionName + " created Successfully :" + region);
  }


  public static void put() {
    for (int i = 0; i < 120; i++) {
      region.put("YOYO-CUST-KEY-" + i, "YOYO-CUST-VAL-" + i);
    }
  }

  public static void executeFunction() {
    try {
      Function function1 = new TestFunction(true, TestFunction.TEST_FUNCTION_EXCEPTION);
      FunctionService.registerFunction(function1);
      ResultCollector rc =
          FunctionService.onRegion(region).setArguments(Boolean.TRUE).execute(function1.getId());
      rc.getResult();
      Assert.fail("Exception should occur", new Exception("Test Failed"));
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("I have been thrown from TestFunction"));
    }
  }

  public static void executeFunctionOnMember() {
    try {
      Function function1 = new TestFunction(true, TestFunction.TEST_FUNCTION_EXCEPTION);
      FunctionService.registerFunction(function1);
      DistributedMember localmember = getSystemStatic().getDistributedMember();
      ResultCollector rc = FunctionService.onMember(localmember).setArguments(Boolean.TRUE)
          .execute(function1.getId());
      rc.getResult();
      Assert.fail("Exception should occur", new Exception("Test Failed"));
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("I have been thrown from TestFunction"));
      return;
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    if (cache != null) {
      cache.close();
    }
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        if (cache != null) {
          cache.close();
        }
        cache = null;
      }
    });
  }
}
