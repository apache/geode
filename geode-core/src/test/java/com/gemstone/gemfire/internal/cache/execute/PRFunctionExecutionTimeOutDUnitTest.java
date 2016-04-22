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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

public class PRFunctionExecutionTimeOutDUnitTest extends
    PartitionedRegionDUnitTestCase {
  private static final String TEST_FUNCTION_TIMEOUT = TestFunction.TEST_FUNCTION_TIMEOUT;
  private static final String TEST_FUNCTION7 = TestFunction.TEST_FUNCTION7;
  
  private static final long serialVersionUID = 1L;

  public PRFunctionExecutionTimeOutDUnitTest(String name) {
    super(name);
  }
  
  /**
   * Test remote execution by a pure accessor. Then test it using timeout and multiple getResult.
   * @throws Exception
   */
  public void testRemoteSingleKeyExecution_byName() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(2);
    final VM datastore = host.getVM(3);
    getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);

        getCache().createRegion(
            rName, ra);
        return Boolean.TRUE;
      }
    });

    datastore
        .invoke(new SerializableCallable("Create PR with Function Factory") {
          public Object call() throws Exception {
            RegionAttributes ra = PartitionedRegionTestHelper
                .createRegionAttrsForPR(0, 10);
            AttributesFactory raf = new AttributesFactory(ra);

            PartitionAttributesImpl pa = new PartitionAttributesImpl();
            pa.setAll(ra.getPartitionAttributes());
            raf.setPartitionAttributes(pa);

            getCache().createRegion(
                rName, raf.create());
            Function function = new TestFunction(true,TEST_FUNCTION_TIMEOUT);
            FunctionService.registerFunction(function);
            return Boolean.TRUE;
          }
        });

    Object o = accessor.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        final String testKey = "execKey";
        final Set testKeysSet = new HashSet();
        testKeysSet.add(testKey);
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        try {
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
              function.getId());
        }
        catch (Exception expected) {
          // No data should cause exec to throw
          assertTrue(expected.getMessage().contains(
              "No target node found for KEY = " + testKey));
        }
        pr.put(testKey, new Integer(1));
        ResultCollector rs1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
            function.getId());
        assertEquals(Boolean.TRUE, ((List)rs1.getResult()).get(0));
        try {
          rs1.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains("Result already collected"));
        }

        
        ResultCollector rs2 = dataSet.withFilter(testKeysSet).withArgs(testKey).execute(
            function.getId());
        assertEquals(new Integer(1), ((List)rs2.getResult()).get(0));
        try {
          rs1.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        
        HashMap putData = new HashMap();
        putData.put(testKey + "1", new Integer(2));
        putData.put(testKey + "2", new Integer(3));
        ResultCollector rs3 =dataSet.withFilter(testKeysSet).withArgs(putData).execute(
            function.getId());
        assertEquals(Boolean.TRUE, ((List)rs3.getResult(4000, TimeUnit.MILLISECONDS)).get(0));
        try {
          rs1.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }

        assertEquals(new Integer(2), pr.get(testKey + "1"));
        assertEquals(new Integer(3), pr.get(testKey + "2"));


        ResultCollector rst1 = dataSet.withFilter(testKeysSet).withArgs(
            Boolean.TRUE).execute(function.getId());
        try {
          rst1.getResult(1000, TimeUnit.MILLISECONDS);
          fail("Did not get the expected timeout exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "All results not recieved in time provided."));
        }
        try {
          rst1.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }

        ResultCollector rst2 = dataSet.withFilter(testKeysSet)
            .withArgs(testKey).execute(function.getId());
        try {
          rst2.getResult(1000, TimeUnit.MILLISECONDS);
          fail("Did not get the expected timeout exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "All results not recieved in time provided."));
        }
        try {
          rst2.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        
        HashMap putDataTimeOut = new HashMap();
        putDataTimeOut.put(testKey + "4", new Integer(4));
        putDataTimeOut.put(testKey + "5", new Integer(5));
        ResultCollector rst3 = dataSet.withFilter(testKeysSet).withArgs(
            putDataTimeOut).execute(function.getId());
        try {
          rst3.getResult(1000, TimeUnit.MILLISECONDS);
          fail("Did not get the expected timeout exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "All results not recieved in time provided."));
        }
        try {
          rst3.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

 

  
  /**
   * Test multi-key remote execution by a pure accessor.Then test it using
   * timeout and multiple getResult.
   * 
   * @throws Exception
   */
  public void testRemoteMultiKeyExecution_byName() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(3);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);
        getCache().createRegion(
            rName, ra);
        return Boolean.TRUE;
      }
    });

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(true,TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    Object o = accessor.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        final HashSet testKeysSet = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 2); i > 0; i--) {
          testKeysSet.add("execKey-" + i);
        }
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);

        try {
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
              function.getId());
        }
        catch (Exception expected) {
          assertTrue(expected.getMessage(), expected.getMessage().contains(
              "No target node found for KEY"));
        }

        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }
        ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function.getId());
        List l = ((List)rs.getResult());
        assertEquals(3, l.size());
        
        for (Iterator i = l.iterator(); i.hasNext();) {
          assertEquals(Boolean.TRUE, i.next());
        }

        try {
          rs.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        
        //DefaultResultCollector rc2 = new DefaultResultCollector();
        ResultCollector rc2 = dataSet.withFilter(testKeysSet).withArgs(testKeysSet)
            .execute(function.getId());
        List l2 = ((List)rc2.getResult());

        assertEquals(3, l2.size());
        HashSet foundVals = new HashSet();
        for (Iterator i = l2.iterator(); i.hasNext();) {
          ArrayList subL = (ArrayList)i.next();
          assertTrue(subL.size() > 0);
          for (Iterator subI = subL.iterator(); subI.hasNext();) {
            assertTrue(foundVals.add(subI.next()));
          }
        }
        assertEquals(origVals, foundVals);
        try {
          rc2.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        
        
        
        ResultCollector rst = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function.getId());
        try {
          rst.getResult(1000,TimeUnit.MILLISECONDS);
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "All results not recieved in time provided."));
        }

        try {
          rst.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        
        ResultCollector rct2 = dataSet.withFilter(testKeysSet).withArgs(testKeysSet)
            .execute(function.getId());
        
        try {
          rct2.getResult(1000,TimeUnit.MILLISECONDS);
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "All results not recieved in time provided."));
        }

        try {
          rct2.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  

  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the
   * function factory present.
   * ResultCollector = CustomResultCollector
   * haveResults = true;
   * 
   * @throws Exception
   */
  public void testRemoteMultiKeyExecutionWithCollector_byName() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(3);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);
        getCache().createRegion(
            rName, ra);
        return Boolean.TRUE;
      }
    });

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(true,TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    Object o = accessor.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        final HashSet testKeysSet = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 2); i > 0; i--) {
          testKeysSet.add("execKey-" + i);
        }
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        dataSet.withCollector(new CustomResultCollector());
        int j = 0;
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          pr.put(i.next(), val);
        }
        ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function.getId());
        List l = (List)rs.getResult();
        assertEquals(3, l.size());
        
        for (Iterator i = l.iterator(); i.hasNext();) {
          assertEquals(Boolean.TRUE, i.next());
        }
        ResultCollector rst = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function.getId());

        try {
          rst.getResult(1000, TimeUnit.MILLISECONDS);
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
              "All results not recieved in time provided."));
        }
        
        try {
          rst.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
  
  /**
   * Test multi-key remote execution by a pure accessor.
   * haveResults = false;
   * Then test it using timeout.
   * @throws Exception
   */
  public void testRemoteMultiKeyExecutionNoResult_byName() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(3);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);
        getCache().createRegion(
            rName, ra);
        return Boolean.TRUE;
      }
    });

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(false,TEST_FUNCTION7);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    Object o = accessor.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call(){
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        final HashSet testKeysSet = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 2); i > 0; i--) {
          testKeysSet.add("execKey-" + i);
        }
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(false,TEST_FUNCTION7);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }
        ResultCollector rs;
        try {
          rs = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function.getId());
          rs.getResult(1000,TimeUnit.MILLISECONDS);
          fail("Did not get the expected exception.");
        }
        catch (Exception expected) {
          assertTrue(expected.getMessage().startsWith(LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("return any")));
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
  
  /**
   * Test multi-key remote execution by a pure accessor.
   * @throws Exception
   */
  public void testRemoteMultiKeyExecution_timeout() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(3);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);
        getCache().createRegion(
            rName, ra);
        return Boolean.TRUE;
      }
    });

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(true,TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    Object o = accessor.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        final HashSet testKeysSet = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 2); i > 0; i--) {
          testKeysSet.add("execKey-" + i);
        }
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        
        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }
        ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs("TestingTimeOut")
        .execute(function.getId());
        List l = ((List)rs.getResult(8000,TimeUnit.MILLISECONDS));
        assertEquals(3, l.size());  // this test may fail..but rarely
        
        ResultCollector rst = dataSet.withFilter(testKeysSet).withArgs("TestingTimeOut")
        .execute(function.getId());
        rst.getResult(8000,TimeUnit.MILLISECONDS);
        assertEquals(3, l.size());  
        
        try {
          rs.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
  


  /**
   * Test ability to execute a multi-key function by a local data store
   * Then test it using timeout and multiple getResult.
   * @throws Exception
   */
  public void testLocalMultiKeyExecution_byName() throws Exception {
    IgnoredException.addIgnoredException("BucketMovedException");
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    VM localOnly = host.getVM(3);
    getCache();
    Object o = localOnly.invoke(new SerializableCallable(
        "Create PR, validate local execution)") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        PartitionedRegion pr = (PartitionedRegion)getCache().createRegion(
            rName, raf.create());
        final String testKey = "execKey";
        DistributedSystem.setThreadsSocketPolicy(false);
        //Function function = new TestFunction(true,"TestFunction2");
        Function function = new TestFunction(true,TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        final HashSet testKeysSet = new HashSet();
        testKeysSet.add(testKey);
        try {
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function.getId());
        }
        catch (Exception expected) {
          // No data should cause exec to throw
          assertTrue(expected.getMessage().contains(
              "No target node found for KEY = " + testKey));
        }

        final HashSet testKeys = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 2); i > 0; i--) {
          testKeys.add("execKey-" + i);
        }

        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeys.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }

        ResultCollector rc1 = dataSet.withFilter(testKeys).withArgs(Boolean.TRUE)
            .execute(function.getId());
        List l = ((List)rc1.getResult());
        assertEquals(1, l.size());
        for (Iterator i = l.iterator(); i.hasNext();) {
          assertEquals(Boolean.TRUE, i.next());
        }

        //DefaultResultCollector rc2 = new DefaultResultCollector();
        ResultCollector rc2 = dataSet.withFilter(testKeys).withArgs(testKeys)
            .execute(function.getId());
        List l2 = ((List)rc2.getResult());
        assertEquals(1, l2.size());

        HashSet foundVals = new HashSet();
        for (Iterator i = l2.iterator(); i.hasNext();) {
          ArrayList subL = (ArrayList)i.next();
          assertTrue(subL.size() > 0);
          for (Iterator subI = subL.iterator(); subI.hasNext();) {
            assertTrue(foundVals.add(subI.next()));
          }
        }
        assertEquals(origVals, foundVals);

        ResultCollector rct1 = dataSet.withFilter(testKeys).withArgs(
            Boolean.TRUE).execute(function.getId());

        try {
          rct1.getResult(1000, TimeUnit.MILLISECONDS);
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          LocalizedStrings.ExecuteFunction_RESULTS_NOT_COLLECTED_IN_TIME_PROVIDED.toLocalizedString()));
        }

        try {
          rct1.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          LocalizedStrings.ExecuteFunction_RESULTS_ALREADY_COLLECTED.toLocalizedString()));
        }
        
        ResultCollector rct2 = dataSet.withFilter(testKeys).withArgs(testKeys)
            .execute(function.getId());

        try {
          rct2.getResult(1000, TimeUnit.MILLISECONDS);
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          LocalizedStrings.ExecuteFunction_RESULTS_NOT_COLLECTED_IN_TIME_PROVIDED.toLocalizedString()));
        }
        
        try {
          rct2.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
              LocalizedStrings.ExecuteFunction_RESULTS_ALREADY_COLLECTED.toLocalizedString()));
        }
        
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  /**
   * Ensure that the execution is happening all the PR as a whole
   * Then test it using timeout and multiple getResult.
   * @throws Exception
   */
  public void testExecutionOnAllNodes_byName()
      throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM datastore3 = host.getVM(3);
    IgnoredException.addIgnoredException("BucketMovedException");
    getCache();
    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        pa.setTotalNumBuckets(17);
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        //Function function = new TestFunction(true,"TestFunction2");
        Function function = new TestFunction(true, TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);
    datastore3.invoke(dataStoreCreate);

    Object o = datastore3.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        DistributedSystem.setThreadsSocketPolicy(false);
        final HashSet testKeys = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 3); i > 0; i--) {
          testKeys.add("execKey-" + i);
        }
        int j = 0;
        for (Iterator i = testKeys.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          pr.put(i.next(), val);
        }
        // Assert there is data in each bucket
        for (int bid = 0; bid < pr.getTotalNumberOfBuckets(); bid++) {
          assertTrue(pr.getBucketKeys(bid).size() > 0);
        }
        Function function = new TestFunction(true, TEST_FUNCTION_TIMEOUT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc1 = dataSet.withArgs(Boolean.TRUE)
            .execute(function.getId());
        List l = ((List)rc1.getResult());
        assertEquals(4, l.size());
        
        for (int i=0; i<4; i++) {
          assertEquals(Boolean.TRUE, l.iterator().next());
        }
        
        
        ResultCollector rct1 = dataSet.withArgs(Boolean.TRUE).execute(
            function.getId());
        
        try {
          rct1.getResult(1000, TimeUnit.MILLISECONDS);
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "All results not recieved in time provided."));
        }
    
        try {
          rct1.getResult();
          fail("Did not get the expected exception.");
        }
        catch (FunctionException fe) {
          assertTrue(fe.getMessage(), fe.getMessage().contains(
          "Result already collected"));
        }
        
        return Boolean.TRUE;

      }
    });
    assertEquals(Boolean.TRUE, o);
  }

}
