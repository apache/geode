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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class FunctionExecution_ExceptionDUnitTest extends
    PartitionedRegionDUnitTestCase {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public FunctionExecution_ExceptionDUnitTest(String name) {
    super(name);
  }
  
  public void testSingleKeyExecution_SendException_Datastore()
      throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore = host.getVM(3);
    getCache();
    datastore
        .invoke(new SerializableCallable("Create PR with Function Factory") {
          public Object call() throws Exception {
            RegionAttributes ra = PartitionedRegionTestHelper
                .createRegionAttrsForPR(0, 10);
            AttributesFactory raf = new AttributesFactory(ra);

            PartitionAttributesImpl pa = new PartitionAttributesImpl();
            pa.setAll(ra.getPartitionAttributes());
            raf.setPartitionAttributes(pa);

            getCache().createRegion(rName, raf.create());
            Function function = new TestFunction(true,
                TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
            FunctionService.registerFunction(function);
            return Boolean.TRUE;
          }
        });

    Object o = datastore.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        final String testKey = "execKey";
        final Set testKeysSet = new HashSet();
        testKeysSet.add(testKey);
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
        FunctionService.registerFunction(function);
        // DefaultResultCollector rs = new DefaultResultCollector();
        Execution dataSet = FunctionService.onRegion(pr);// .withCollector(rs);

        pr.put(testKey, new Integer(1));
        ResultCollector rs1 = null;
        rs1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
            function);
        ArrayList results = (ArrayList)rs1.getResult();
        assertTrue(results.get(0) instanceof Exception);
        
        rs1 = dataSet.withFilter(testKeysSet).withArgs((Serializable)testKeysSet).execute(
            function);
        results = (ArrayList)rs1.getResult();
        assertEquals((testKeysSet.size()+1), results.size());
        Iterator resultIterator = results.iterator();
        int exceptionCount = 0;
        while(resultIterator.hasNext()){
          Object o = resultIterator.next();
          if(o instanceof MyFunctionExecutionException){
            exceptionCount++;
          }
        }
        assertEquals(1, exceptionCount);
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  public void testSingleKeyExecution_SendException_MultipleTimes_Datastore()
      throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore = host.getVM(3);
    getCache();
    datastore
        .invoke(new SerializableCallable("Create PR with Function Factory") {
          public Object call() throws Exception {
            RegionAttributes ra = PartitionedRegionTestHelper
                .createRegionAttrsForPR(0, 10);
            AttributesFactory raf = new AttributesFactory(ra);

            PartitionAttributesImpl pa = new PartitionAttributesImpl();
            pa.setAll(ra.getPartitionAttributes());
            raf.setPartitionAttributes(pa);

            getCache().createRegion(rName, raf.create());
            Function function = new TestFunction(true,
                TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
            FunctionService.registerFunction(function);
            return Boolean.TRUE;
          }
        });

    Object o = datastore.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        final String testKey = "execKey";
        final Set testKeysSet = new HashSet();
        testKeysSet.add(testKey);
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);// .withCollector(rs);

        pr.put(testKey, new Integer(1));
        ResultCollector rs1 = null;
        rs1 = dataSet.withFilter(testKeysSet).withArgs("Multiple").execute(
            function);
        ArrayList results = (ArrayList)rs1.getResult();
        assertTrue(results.get(0) instanceof Exception);
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
  
  public void testRemoteSingleKeyExecution_ThrowException_Datastore()
      throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore = host.getVM(3);
    datastore
        .invoke(new SerializableCallable("Create PR with Function Factory") {
          public Object call() throws Exception {
            RegionAttributes ra = PartitionedRegionTestHelper
                .createRegionAttrsForPR(0, 10);
            AttributesFactory raf = new AttributesFactory(ra);

            PartitionAttributesImpl pa = new PartitionAttributesImpl();
            pa.setAll(ra.getPartitionAttributes());
            raf.setPartitionAttributes(pa);

            getCache().createRegion(rName, raf.create());
            Function function = new TestFunction(true,
                TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
            FunctionService.registerFunction(function);
            return Boolean.TRUE;
          }
        });

    Object o = datastore.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        final String testKey = "execKey";
        final Set testKeysSet = new HashSet();
        testKeysSet.add(testKey);
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
        FunctionService.registerFunction(function);
        // DefaultResultCollector rs = new DefaultResultCollector();
        Execution dataSet = FunctionService.onRegion(pr);// .withCollector(rs);

        pr.put(testKey, new Integer(1));
        ResultCollector rs1 = null;
        rs1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
            function);
        try {
          ArrayList results = (ArrayList)rs1.getResult();
          fail("Expecting Exception");
        }
        catch (Exception e) {
          e.printStackTrace();
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  public void testRemoteSingleKeyExecution_SendException_Accessor()
      throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(2);
    final VM datastore = host.getVM(3);
    getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);

        getCache().createRegion(rName, ra);
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

            getCache().createRegion(rName, raf.create());
            Function function = new TestFunction(true,
                TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
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
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
        FunctionService.registerFunction(function);
        // DefaultResultCollector rs = new DefaultResultCollector();
        Execution dataSet = FunctionService.onRegion(pr);// .withCollector(rs);

        pr.put(testKey, new Integer(1));
        ResultCollector rs1 = null;
//        rs1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
//            function);
//        ArrayList results = (ArrayList)rs1.getResult();
//        assertTrue(results.get(0) instanceof Exception);
//        
        rs1 = dataSet.withFilter(testKeysSet).withArgs((Serializable)testKeysSet).execute(
            function);
        ArrayList results = (ArrayList)rs1.getResult();
        assertEquals((testKeysSet.size()+1), results.size());
        Iterator resultIterator = results.iterator();
        int exceptionCount = 0;
        while(resultIterator.hasNext()){
          Object o = resultIterator.next();
          if(o instanceof MyFunctionExecutionException){
            exceptionCount++;
          }
        }
        assertEquals(1, exceptionCount);
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  public void testRemoteSingleKeyExecution_ThrowException_Accessor()
      throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(2);
    final VM datastore = host.getVM(3);
    getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);

        getCache().createRegion(rName, ra);
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

            getCache().createRegion(rName, raf.create());
            Function function = new TestFunction(true,
                TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
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
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
        FunctionService.registerFunction(function);
        // DefaultResultCollector rs = new DefaultResultCollector();
        Execution dataSet = FunctionService.onRegion(pr);// .withCollector(rs);

        pr.put(testKey, new Integer(1));
        ResultCollector rs1 = null;
        rs1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
            function);
        try {
          ArrayList results = (ArrayList)rs1.getResult();
          fail("Expecting Exception");
        }
        catch (Exception e) {
          e.printStackTrace();
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  public void testRemoteMultiKeyExecution_SendException() throws Exception {
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
        getCache().createRegion(rName, ra);
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
        getCache().createRegion(rName, raf.create());
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
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
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);

        HashSet origVals = new HashSet();
        for (int i = 0; i < 3; i++) {
          Integer val = new Integer(i);
          origVals.add(val);
          pr.put(val, "MyValue_" + i);
        }
        ResultCollector rs1 = dataSet.withFilter(origVals).withArgs((Serializable)origVals).execute(
            function);
        List results = (ArrayList)rs1.getResult();
        assertEquals(((origVals.size()*3)+3), results.size());
        Iterator resultIterator = results.iterator();
        int exceptionCount = 0;
        while(resultIterator.hasNext()){
          Object o = resultIterator.next();
          if(o instanceof MyFunctionExecutionException){
            exceptionCount++;
          }
        }
        assertEquals(3, exceptionCount);
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  public void testRemoteAllKeyExecution_SendException() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM datastore3 = host.getVM(3);
    getCache();
    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(rName, raf.create());
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);
    datastore3.invoke(dataStoreCreate);

    Object o = datastore0.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);

        HashSet origVals = new HashSet();
        for (int i = 0; i < 4; i++) {
          Integer val = new Integer(i);
          origVals.add(val);
          pr.put(val, "MyValue_" + i);
        }
        ResultCollector rc2 = null;
        ResultCollector rs1 = dataSet.withFilter(origVals).withArgs((Serializable)origVals).execute(
            function);
        List results = (ArrayList)rs1.getResult();
        assertEquals(((origVals.size()*4)+4), results.size());
        Iterator resultIterator = results.iterator();
        int exceptionCount = 0;
        while(resultIterator.hasNext()){
          Object o = resultIterator.next();
          if(o instanceof MyFunctionExecutionException){
            exceptionCount++;
          }
        }
        assertEquals(4, exceptionCount);

        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  public void testRemoteMultiKeyExecution_ThrowException() throws Exception {
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
        getCache().createRegion(rName, ra);
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
        getCache().createRegion(rName, raf.create());
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
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
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);

        HashSet origVals = new HashSet();
        for (int i = 0; i < 3; i++) {
          Integer val = new Integer(i);
          origVals.add(val);
          pr.put(val, "MyValue_" + i);
        }
        ResultCollector rc2 = null;
        rc2 = dataSet.withFilter(origVals).withArgs(origVals).execute(
            function.getId());
        try {
          ArrayList results = (ArrayList)rc2.getResult();
          fail("Expecting Exception");
        }
        catch (Exception e) {
          e.printStackTrace();
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  public void testRemoteAllKeyExecution_ThrowException() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM datastore3 = host.getVM(3);
    getCache();

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(rName, raf.create());
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);
    datastore3.invoke(dataStoreCreate);

    Object o = datastore0.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);

        HashSet origVals = new HashSet();
        for (int i = 0; i < 4; i++) {
          Integer val = new Integer(i);
          origVals.add(val);
          pr.put(val, "MyValue_" + i);
        }
        ResultCollector rc2 = null;
        rc2 = dataSet.withFilter(origVals).withArgs(origVals).execute(
            function.getId());
        try {
          ArrayList results = (ArrayList)rc2.getResult();
          fail("Expecting Exception");
        }
        catch (Exception e) {
          e.printStackTrace();
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

}
