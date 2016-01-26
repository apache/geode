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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
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
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

public class PRFunctionExecutionWithResultSenderDUnitTest extends
    PartitionedRegionDUnitTestCase {
  private static final String TEST_FUNCTION7 = TestFunction.TEST_FUNCTION7;

  private static final String TEST_FUNCTION2 = TestFunction.TEST_FUNCTION2;
  
  private static final long serialVersionUID = 1L;

  public PRFunctionExecutionWithResultSenderDUnitTest(String name) {
    super(name);
  }

  /**
   * Test remote execution by a pure accessor which doesn't have the function
   * factory present.
   * 
   * @throws Exception
   */
  public void testRemoteSingleKeyExecution_byName() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(2);
    final VM datastore = host.getVM(3);
    final Cache c = getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);

        PartitionedRegion pr = (PartitionedRegion)getCache().createRegion(
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

            PartitionedRegion pr = (PartitionedRegion)getCache().createRegion(
                rName, raf.create());
            Function function = new TestFunction(true, TEST_FUNCTION2);
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
        Function function = new TestFunction(true, TEST_FUNCTION2);
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
        pr.put(testKey+"3", new Integer(3));
        pr.put(testKey+"4", new Integer(4));
        testKeysSet.add(testKey + "3");
        testKeysSet.add(testKey + "4");
        
        ResultCollector rs1 = dataSet.withFilter(testKeysSet).withArgs(
            Boolean.TRUE).execute(function.getId());
        assertEquals(Boolean.TRUE, ((List)rs1.getResult()).get(0));
        ResultCollector rs2 = dataSet.withFilter(testKeysSet).withArgs((Serializable)testKeysSet)
            .execute(function.getId());

        HashMap putData = new HashMap();
        putData.put(testKey + "1", new Integer(2));
        putData.put(testKey + "2", new Integer(3));
        ResultCollector rs3 = dataSet.withFilter(testKeysSet).withArgs(putData)
            .execute(function.getId());
        assertEquals(Boolean.TRUE, ((List)rs3.getResult()).get(0));

        assertEquals(new Integer(2), pr.get(testKey + "1"));
        assertEquals(new Integer(3), pr.get(testKey + "2"));
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  /**
   * Test remote execution by a pure accessor which doesn't have the function
   * factory present And the function doesn't send last result.
   * FunctionException is expected in this case
   * 
   * @throws Exception
   */
  public void testRemoteExecution_NoLastResult() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(0);
    final VM datastore = host.getVM(1);
    final Cache c = getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);

        PartitionedRegion pr = (PartitionedRegion)getCache().createRegion(
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

            PartitionedRegion pr = (PartitionedRegion)getCache().createRegion(
                rName, raf.create());
            Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT);
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
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        
        pr.put(testKey, new Integer(1));
        pr.put(testKey+"3", new Integer(3));
        pr.put(testKey+"4", new Integer(4));
        testKeysSet.add(testKey + "3");
        testKeysSet.add(testKey + "4");
        
        ResultCollector rs1 = dataSet.withFilter(testKeysSet).withArgs(
            Boolean.TRUE).execute(function.getId());
        try{
          assertEquals(Boolean.TRUE, ((List)rs1.getResult()).get(0));
          fail("Expected FunctionException : Function did not send last result");
        }catch(FunctionException fe){
          assertTrue(fe.getMessage().contains(
              "did not send last result"));
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
  
  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the
   * function factory present.
   * ResultCollector = DefaultResultCollector
   * haveResults = true;
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
    
    final Cache c = getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);
        PartitionedRegion pr = (PartitionedRegion)getCache().createRegion(
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
        PartitionedRegion pr = (PartitionedRegion)getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION9);
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
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION9);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);

        try {
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
              function.getId());
        }
        catch (Exception expected) {
          assertTrue(expected.getMessage().contains(
              "No target node found for KEY = "));
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

        ResultCollector rc2 = dataSet.withFilter(testKeysSet).withArgs(testKeysSet)
            .execute(function.getId());
        List l2 = ((List)rc2.getResult());
        assertEquals(pr.getTotalNumberOfBuckets() * 2 * 3, l2.size());

        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  
  }

  /**
   * Test ability to execute a multi-key function by a local data store
   * ResultCollector = DefaultResultCollector
   * haveResult = true
   * @throws Exception
   */
  public void testLocalMultiKeyExecution_byName() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    VM localOnly = host.getVM(3);
    final Cache c = getCache();
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
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION9);
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

        ResultCollector rc2 = dataSet.withFilter(testKeys).withArgs(testKeys)
            .execute(function.getId());
        List l2 = ((List)rc2.getResult());
        assertEquals(226, l2.size());

        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
  
  /**
   * Test local execution on datastore with function that doesn't send last result.
   * FunctionException is expected in this case
   * 
   * @throws Exception
   */
  public void testLocalExecution_NoLastResult() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    VM localOnly = host.getVM(0);
    final Cache c = getCache();
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
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_NO_LASTRESULT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);

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
        try{
          assertEquals(Boolean.TRUE, ((List)rc1.getResult()).get(0));
          fail("Expected FunctionException : Function did not send last result");
        }catch(Exception fe){
          assertTrue(fe.getMessage().contains(
              "did not send last result"));
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
  
  /**
   * 
   * Test execution on all datastores with function that doesn't send last result.
   * FunctionException is expected in this case
   * 
   * @throws Exception
   */
  public void testExecutionOnAllNodes_NoLastResult()
      throws Exception {
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
        pa.setTotalNumBuckets(17);
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT);
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
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc1 = dataSet.withArgs(Boolean.TRUE)
            .execute(function.getId());
        try{
          assertEquals(Boolean.TRUE, ((List)rc1.getResult()).get(0));
          fail("Expected FunctionException : Function did not send last result");
        }catch(Exception fe){
          assertTrue(fe.getMessage().contains(
              "did not send last result"));
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
  
  public void testExecutionOnAllNodes_byName() throws Exception {
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
        pa.setTotalNumBuckets(17);
        raf.setPartitionAttributes(pa);
        getCache().createRegion(rName, raf.create());
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION9);
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
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION9);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc1 = dataSet.withArgs(Boolean.TRUE).execute(
            function.getId());
        List l = ((List)rc1.getResult());
        getLogWriter().info(
            "PRFunctionExecutionDUnitTest#testExecutionOnAllNodes_byName : Result size :"
                + l.size() + " Result : " + l);
        assertEquals(4, l.size());

        for (int i = 0; i < 4; i++) {
          assertEquals(Boolean.TRUE, l.iterator().next());
        }
        return Boolean.TRUE;

      }
    });
    assertEquals(Boolean.TRUE, o);
 }
  /**
   * Ensure that the execution is happening all the PR as a whole
   * 
   * @throws Exception
   */

  public static class TestResolver implements PartitionResolver, Serializable {
    public String getName() {
      return "ResolverName_" + getClass().getName();
    }

    public Serializable getRoutingObject(EntryOperation opDetails) {
      return (Serializable)opDetails.getKey();
    }

    public void close() {
    }

    public Properties getProperties() {
      return new Properties();
    }
  }

  private RegionAttributes createColoRegionAttrs(int red, int mem,
      String coloRegion) {
    final TestResolver resolver = new TestResolver();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(new PartitionAttributesFactory()
        .setPartitionResolver(resolver).setRedundantCopies(red)
        .setLocalMaxMemory(mem).setColocatedWith(coloRegion).create());
    return attr.create();
  }

  public void testlonerSystem_Bug41832() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore = host.getVM(3);
    Object o = datastore.invoke(new SerializableCallable("Create region") {
      public Object call() throws Exception {
        createLonerCache(); // creates loner cache
        AttributesFactory factory = new AttributesFactory();
        factory.setDataPolicy(DataPolicy.EMPTY);
        Region region = getCache().createRegion(rName, factory.create());
        Function function = new TestFunction(true, TEST_FUNCTION2);
        FunctionService.registerFunction(function);

        Execution dataSet = FunctionService.onRegion(region);
        try {
          ResultCollector rc = dataSet.withArgs(Boolean.TRUE).execute(
              function.getId());
          Object o = rc.getResult();
          fail("Expected Function Exception");
        }
        catch (Exception expected) {
          assertTrue(expected.getMessage().startsWith(
              "No Replicated Region found for executing function"));
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
}
