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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.LonerDistributionManager;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Customer;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class PRFunctionExecutionDUnitTest extends
    PartitionedRegionDUnitTestCase {
  private static final String TEST_FUNCTION7 = TestFunction.TEST_FUNCTION7;
  private static final String TEST_FUNCTION2 = TestFunction.TEST_FUNCTION2;
  static Cache cache = null;
  static String regionName = null;
  private static final long serialVersionUID = 1L;

  public PRFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  /**
   * Test to validate that the function execution is successful on PR with Loner Distributed System
   * @throws Exception
   */
  public void testFunctionExecution() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore = host.getVM(3);

    datastore
        .invoke(new SerializableCallable("Create PR with Function Factory") {
          public Object call() throws Exception {
            
            Properties props = new Properties();
            props.setProperty("mcast-port", "0");
            props.setProperty("locators", "");
            
            DistributedSystem ds = getSystem(props);
            assertNotNull(ds);
            ds.disconnect();
            ds = getSystem(props);
            cache = CacheFactory.create(ds);
            assertNotNull(cache);
            
            RegionAttributes ra = PartitionedRegionTestHelper
                .createRegionAttrsForPR(0, 10);
            AttributesFactory raf = new AttributesFactory(ra);

            PartitionAttributesImpl pa = new PartitionAttributesImpl();
            pa.setAll(ra.getPartitionAttributes());
            raf.setPartitionAttributes(pa);

            Region pr = cache.createRegion(rName, raf.create());

            final String testKey = "execKey";
            final Set testKeysSet = new HashSet();
            testKeysSet.add(testKey);
            
            Function function = new TestFunction(true,
                TestFunction.TEST_FUNCTION1);
              FunctionService.registerFunction(function);

              Execution dataSet = FunctionService.onRegion(pr);
              ResultCollector result = dataSet.withArgs(
                  Boolean.TRUE).withFilter(testKeysSet).execute(function);
              System.out.println("KBKBKB : Result I got : " + result.getResult());
            return Boolean.TRUE;
          }
        });
  }

  public void testHAFunctionExecution() throws Exception {
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

            Region pr = getCache().createRegion(rName, raf.create());
            final String testKey = "execKey";
            final Set testKeysSet = new HashSet();
            testKeysSet.add(testKey);

            Function function = new TestFunction(false,
                TestFunction.TEST_FUNCTION10);
            try {
              FunctionService.registerFunction(function);
              fail("It should have failed with Function attributes don't match");
            }
            catch (Exception expected) {
              expected.printStackTrace();
              assertTrue(expected.getMessage().contains(
                  "For Functions with isHA true, hasResult must also be true."));
            }

            try {
              Execution dataSet = FunctionService.onRegion(pr);
              dataSet.withFilter(testKeysSet).withArgs(
                  testKey).execute(function);
              fail("It should have failed with Function attributes don't match");
            }
            catch (Exception expected) {
              expected.printStackTrace();
              assertTrue(expected.getMessage().contains(
                  "For Functions with isHA true, hasResult must also be true."));
            }
            return Boolean.TRUE;
          }
        });
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
            Function function = new TestFunction(true,TEST_FUNCTION2);
            FunctionService.registerFunction(function);
            return Boolean.TRUE;
          }
        });

    accessor.invoke(new SerializableCallable(
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
        ResultCollector rs1 = dataSet.withFilter(testKeysSet).withArgs(
            Boolean.TRUE).execute(function.getId());
        assertEquals(Boolean.TRUE, ((List)rs1.getResult()).get(0));
        ResultCollector rs2 = dataSet.withFilter(testKeysSet).withArgs(testKey)
            .execute(function.getId());
        assertEquals(new Integer(1), ((List)rs2.getResult()).get(0));

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
  }

  /**
   * Test local execution by a datastore Function throws the
   * FunctionInvocationTargetException. As this is the case of HA then system
   * should retry the function execution. After 5th attempt function will send
   * Boolean as last result. factory present.
   *
   * @throws Exception
   */
  public void testLocalSingleKeyExecution_byName_FunctionInvocationTargetException()
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

            Region pr = getCache().createRegion(rName, raf.create());
            final String testKey = "execKey";
            final Set testKeysSet = new HashSet();
            testKeysSet.add(testKey);

            Function function = new TestFunction(true,
                TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION);
            FunctionService.registerFunction(function);
            Execution dataSet = FunctionService.onRegion(pr);
            pr.put(testKey, new Integer(1));
            try {
              ResultCollector rs1 = dataSet.withFilter(testKeysSet).withArgs(
                  Boolean.TRUE).execute(function.getId());
              List list = (ArrayList)rs1.getResult();
              assertEquals(list.get(0), 5);
            }
            catch (Throwable e) {
              e.printStackTrace();
              fail("This is not expected Exception", e);
            }
            return Boolean.TRUE;
          }
        });
  }
 
 
  /**
   * Test remote execution by a pure accessor which doesn't have the function
   * factory present.Function throws the FunctionInvocationTargetException. As
   * this is the case of HA then system should retry the function execution.
   * After 5th attempt function will send Boolean as last result.
   *
   * @throws Exception
   */
  public void testRemoteSingleKeyExecution_byName_FunctionInvocationTargetException()
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
                TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION);
            FunctionService.registerFunction(function);
            return Boolean.TRUE;
          }
        });

    accessor.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        final String testKey = "execKey";
        final Set testKeysSet = new HashSet();
        testKeysSet.add(testKey);
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        pr.put(testKey, new Integer(1));
        try {
          ResultCollector rs1 = dataSet.withFilter(testKeysSet).withArgs(
              Boolean.TRUE).execute(function.getId());
          List list = (ArrayList)rs1.getResult();
          assertEquals(list.get(0), 5);
        }
        catch (Throwable e) {
          e.printStackTrace();
          fail("This is not expected Exception", e);
        }
        return Boolean.TRUE;
      }
    });
  }
 
  /**
   * Test remote execution by a pure accessor which doesn't have the function
   * factory present.
   *
   * @throws Exception
   */
  public void testRemoteSingleKeyExecution_byInstance() throws Exception {
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
            Function function = new TestFunction(true,TEST_FUNCTION2);
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        //DefaultResultCollector rs = new DefaultResultCollector();
        Execution dataSet = FunctionService.onRegion(pr); //withCollector(rs);

        try {
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
              function);

        }
        catch (Exception expected) {
          // No data should cause exec to throw
          assertTrue(expected.getMessage().contains(
              "No target node found for KEY = " + testKey));
        }

        pr.put(testKey, new Integer(1));
        ResultCollector rs1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE)
            .execute(function);
        assertEquals(Boolean.TRUE, ((List)rs1.getResult()).get(0));
        ResultCollector rs2 =dataSet.withFilter(testKeysSet).withArgs(testKey).execute(function);
        assertEquals(new Integer(1), ((List)rs2.getResult()).get(0));

        HashMap putData = new HashMap();
        putData.put(testKey + "1", new Integer(2));
        putData.put(testKey + "2", new Integer(3));
        ResultCollector rs3 = dataSet.withFilter(testKeysSet).withArgs(putData).execute(function);
        assertEquals(Boolean.TRUE, ((List)rs3.getResult()).get(0));

        assertEquals(new Integer(2), pr.get(testKey + "1"));
        assertEquals(new Integer(3), pr.get(testKey + "2"));
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  /**
   * Test remote execution of inline function by a pure accessor
   *
   * @throws Exception
   */
  public void testRemoteSingleKeyExecution_byInlineFunction() throws Exception {
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
        Execution dataSet = FunctionService.onRegion(pr);
        pr.put(testKey, new Integer(1));
        ResultCollector rs1 =dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(new FunctionAdapter(){
          @Override
          public void execute(FunctionContext context) {
            if (context.getArguments() instanceof String) {
              context.getResultSender().lastResult("Success");
            }else if(context.getArguments() instanceof Boolean){
              context.getResultSender().lastResult(Boolean.TRUE);
            }
          }

          @Override
          public String getId() {
            return getClass().getName();
          }

          @Override
          public boolean hasResult() {
            return true;
          }
        });
        assertEquals(Boolean.TRUE, ((List)rs1.getResult()).get(0));
        return Boolean.TRUE;
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
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
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  
  public void testRemoteMultiKeyExecution_BucketMoved() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(3);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        AttributesFactory factory = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(113);
        paf.setLocalMaxMemory(0);
        paf.setRedundantCopies(1);
        paf.setStartupRecoveryDelay(0);
        PartitionAttributes partitionAttributes = paf.create();
        factory.setDataPolicy(DataPolicy.PARTITION);
        factory.setPartitionAttributes(partitionAttributes);
        RegionAttributes attrs = factory.create();
        getCache().createRegion(
            rName, attrs);
        return Boolean.TRUE;
      }
    });

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        AttributesFactory factory = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(113);
        paf.setLocalMaxMemory(40);
        paf.setRedundantCopies(1);
        paf.setStartupRecoveryDelay(0);
        PartitionAttributes partitionAttributes = paf.create();
        factory.setDataPolicy(DataPolicy.PARTITION);
        factory.setPartitionAttributes(partitionAttributes);
        RegionAttributes attrs = factory.create();
        getCache().createRegion(
            rName, attrs);
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_LASTRESULT);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    
    SerializableCallable put = new SerializableCallable(
        "put in PR") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        for (int i = 0; i < 113 ; i++) {
          pr.put(i, "execKey-" + i);
        }
        return Boolean.TRUE;
      }
    };
    accessor.invoke(put);

    datastore2.invoke(dataStoreCreate);
    
    Object result = accessor.invoke(new SerializableCallable(
        "invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_LASTRESULT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc2 = dataSet.withArgs(Boolean.TRUE).execute(function.getId());
        List l = ((List)rc2.getResult());
        return l;
      }
    });
    List l = (List)result;
    assertEquals(2, l.size());
    
  }
  
  public void testLocalMultiKeyExecution_BucketMoved() throws Exception {
    addExpectedException("BucketMovedException");
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    getCache();

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        AttributesFactory factory = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(113);
        paf.setLocalMaxMemory(40);
        paf.setRedundantCopies(0);
        paf.setStartupRecoveryDelay(0);
        PartitionAttributes partitionAttributes = paf.create();
        factory.setDataPolicy(DataPolicy.PARTITION);
        factory.setPartitionAttributes(partitionAttributes);
        RegionAttributes attrs = factory.create();
        getCache().createRegion(
            rName, attrs);
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_LASTRESULT);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    
    SerializableCallable put = new SerializableCallable(
        "put in PR") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        for (int i = 0; i < 113 ; i++) {
          pr.put(i, "execKey-" + i);
        }
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(put);

    datastore1.invoke(dataStoreCreate);

    Object result = datastore0.invoke(new SerializableCallable(
        "invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);

        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_LASTRESULT);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc2 = dataSet.withArgs(Boolean.TRUE).execute(
            function.getId());
        List l = ((List)rc2.getResult());
        return l;
      }
    });
    
    List l = (List)result;
    assertEquals(2, l.size());
    
  }
  
  
  
  /**
   * Test remote execution by a pure accessor which doesn't have the function
   * factory present.Function throws the FunctionInvocationTargetException. As
   * this is the case of HA then system should retry the function execution.
   * After 5th attempt function will send Boolean as last result.
   *
   * @throws Exception
   */
  public void testRemoteMultipleKeyExecution_byName_FunctionInvocationTargetException()
      throws Exception {
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
            TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION);
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
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);

        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }
        try {
          ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs(
              Boolean.TRUE).execute(function.getId());
          List list = (ArrayList)rs.getResult();
          assertEquals(list.get(0), 5);
        }
        catch (Throwable e) {
          e.printStackTrace();
          fail("This is not expected Exception", e);
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
 
  public void testRemoteMultiKeyExecutionHA_CacheClose() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(3);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        cache = getCache();
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(1, 0);
        cache.createRegion(rName, ra);
        regionName = rName;
        return Boolean.TRUE;
      }
    });

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(1, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(rName, raf.create());
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_HA);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    Object o = accessor.invoke(new SerializableCallable("Create data") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)cache.getRegion(regionName);

        final HashSet testKeysSet = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 2); i > 0; i--) {
          testKeysSet.add("execKey-" + i);
        }

        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }
        return Boolean.TRUE;
      }
    });

    assertEquals(Boolean.TRUE, o);

    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = accessor.invokeAsync(PRFunctionExecutionDUnitTest.class,
        "executeFunction");

    o = datastore0.invoke(new SerializableCallable("close cache") {
      public Object call() throws Exception {
        long startTime = System.currentTimeMillis();
        WaitCriterion wc = new WaitCriterion() {
          String excuse;

          public boolean done() {
            return false;
          }

          public String description() {
            return excuse;
          }
        };
        DistributedTestCase.waitForCriterion(wc, 3000, 200, false);
        long endTime = System.currentTimeMillis();
        getCache().getLogger().fine(
            "Time wait for Cache Close = " + (endTime - startTime));
        getCache().close();
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);

    DistributedTestCase.join(async[0], 60 * 1000, getLogWriter());

    if (async[0].getException() != null) {
      fail("UnExpected Exception Occured : ", async[0].getException());
    }
    List l = (List)async[0].getReturnValue();
    assertEquals(2, l.size());
  }
 
  public void testRemoteMultiKeyExecutionHA_Disconnect() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(3);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        cache = getCache();
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(1, 0);
        cache.createRegion(rName, ra);
        regionName = rName;
        return Boolean.TRUE;
      }
    });

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(1, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(rName, raf.create());
        Function function = new TestFunction(true,
            TestFunction.TEST_FUNCTION_HA);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    Object o = accessor.invoke(new SerializableCallable("Create data") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)cache.getRegion(regionName);

        final HashSet testKeysSet = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 2); i > 0; i--) {
          testKeysSet.add("execKey-" + i);
        }

        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }
        return Boolean.TRUE;
      }
    });

    assertEquals(Boolean.TRUE, o);

    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = accessor.invokeAsync(PRFunctionExecutionDUnitTest.class,
        "executeFunction");

    o = datastore0.invoke(new SerializableCallable("disconnect") {
      public Object call() throws Exception {
        long startTime = System.currentTimeMillis();
        WaitCriterion wc = new WaitCriterion() {
          String excuse;

          public boolean done() {
            return false;
          }

          public String description() {
            return excuse;
          }
        };
        DistributedTestCase.waitForCriterion(wc, 3000, 200, false);
        long endTime = System.currentTimeMillis();
        getCache().getLogger().fine(
            "Time wait for Cache Close = " + (endTime - startTime));
        getCache().getDistributedSystem().disconnect();
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);

    DistributedTestCase.join(async[0], 60 * 1000, getLogWriter());

    if (async[0].getException() != null) {
      fail("UnExpected Exception Occured : ", async[0].getException());
    }
    List l = (List)async[0].getReturnValue();
    assertEquals(2, l.size());
  }
 
  public static Object executeFunction() {
    PartitionedRegion pr = (PartitionedRegion)cache.getRegion(regionName);
    final HashSet testKeysSet = new HashSet();
    for (int i = (pr.getTotalNumberOfBuckets() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(pr);
    ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE)
        .execute(function.getId());
    List l = ((List)rs.getResult());
    return l;
  }
 
  /**
   * Test multi-key remote execution of inline function by a pure accessor
   * ResultCollector = DefaultResultCollector
   * haveResults = true;
   *
   * @throws Exception
   */
  public void testRemoteMultiKeyExecution_byInlineFunction() throws Exception {
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
        Execution dataSet = FunctionService.onRegion(pr);
        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }
        ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(new FunctionAdapter(){
          @Override
          public void execute(FunctionContext context) {
            if (context.getArguments() instanceof String) {
              context.getResultSender().lastResult("Success");
            }else if(context.getArguments() instanceof Boolean){
              context.getResultSender().lastResult(Boolean.TRUE);
            }
          }

          @Override
          public String getId() {
            return getClass().getName();
          }

          @Override
          public boolean hasResult() {
            return true;
          }
        });
        List l = ((List)rs.getResult());
        assertEquals(3, l.size());
        for (Iterator i = l.iterator(); i.hasNext();) {
          assertEquals(Boolean.TRUE, i.next());
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        dataSet.withCollector(new CustomResultCollector());
        int j = 0;
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          pr.put(i.next(), val);
        }
        ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function.getId());
        List l = ((List)rs.getResult());
        assertEquals(3, l.size());
        
        for (Iterator i = l.iterator(); i.hasNext();) {
          assertEquals(Boolean.TRUE, i.next());
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
 
  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the
   * function factory present.
   * ResultCollector = DefaultResultCollector
   * haveResults = false;
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
          rs.getResult();
        }
        catch (Exception expected) {
          expected.printStackTrace();
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
   * Test multi-key remote execution by a pure accessor which doesn't have the
   * function factory present.
   * ResultCollector = DefaultResultCollector
   * haveResults = true;
   * result Timeout = 10 milliseconds
   * expected result to be 0.(as the execution gets the timeout)
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        
        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }
//        long startTime = System.currentTimeMillis();
        ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs("TestingTimeOut")
        .execute(function.getId());
//        long endTime = System.currentTimeMillis();
        List l = ((List)rs.getResult(10000, TimeUnit.MILLISECONDS));
        assertEquals(3, l.size());  // this test may fail..but rarely
        
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
 
  /**
   * Test multi-key remote execution by a pure accessor which doesn't have the
   * function factory present.
   * ResultCollector = CustomResultCollector
   * haveResults = false;
   * @throws Exception
   */
  public void testRemoteMultiKeyExecutionWithCollectorNoResult_byName() throws Exception {
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
        dataSet.withCollector(new CustomResultCollector());
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
          rs.getResult();
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
   * Test multi-key remote execution by a pure accessor which doesn't have the
   * function factory present.
   *
   * @throws Exception
   */
  public void testRemoteMultiKeyExecution_byInstance() throws Exception {
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        try {
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
              function);
        }
        catch (Exception expected) {
          // No data should cause exec to throw
          getLogWriter().warning("Exception Occured : "+ expected.getMessage());
          // boolean expectedStr = expected.getMessage().startsWith("No target
          // node was found for routingKey");
          // assertTrue("Unexpected exception: " + expected, expectedStr);
        }

        int j = 0;
        HashSet origVals = new HashSet();
        for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          origVals.add(val);
          pr.put(i.next(), val);
        }
        //DefaultResultCollector rc1 = new DefaultResultCollector();
        ResultCollector rc1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE)
            .execute(function);
        List l = ((List)rc1.getResult());
        assertEquals(3, l.size());
        
        for (Iterator i = l.iterator(); i.hasNext();) {
          assertEquals(Boolean.TRUE, i.next());
        }

        //DefaultResultCollector rc2 = new DefaultResultCollector();
        ResultCollector rc2 = dataSet.withFilter(testKeysSet).withArgs(testKeysSet)
            .execute(function);
        List l2 = ((List)rc2.getResult());
        //assertEquals(pr.getTotalNumberOfBuckets(), l2.size());
        assertEquals(3, l2.size());
        
        // assertEquals(pr.getTotalNumberOfBuckets(), l.size());
        HashSet foundVals = new HashSet();
        for (Iterator i = l2.iterator(); i.hasNext();) {
          ArrayList subL = (ArrayList)i.next();
          assertTrue(subL.size() > 0);
          for (Iterator subI = subL.iterator(); subI.hasNext();) {
            assertTrue(foundVals.add(subI.next()));            
          }
        }
        assertEquals(origVals, foundVals);
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  
  /**
   * Test bucketFilter functionality
   *
   * @throws Exception
   */
  public void testBucketFilter_1() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(3);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        PartitionResolver resolver = new BucketFilterPRResolver();
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0, resolver);
        getCache().createRegion(
            rName, ra);
        return Boolean.TRUE;
      }
    });

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        PartitionResolver resolver = new BucketFilterPRResolver();
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10, resolver);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    Object o = accessor.invoke(new SerializableCallable("Create data") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        for(int i = 0; i < 50; ++i) {
          pr.put(i, i);
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
    
    o = accessor.invoke(new SerializableCallable(
        "Execute function single filter") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
        FunctionService.registerFunction(function);
        InternalExecution dataSet = (InternalExecution) FunctionService.onRegion(pr);
        Set<Integer> bucketSet = new HashSet<Integer>();
        bucketSet.add(2);
        ResultCollector<Integer, List<Integer>> rc = (ResultCollector<Integer, List<Integer>>) dataSet
            .withBucketFilter(bucketSet).execute(function);
        
        List<Integer> results = rc.getResult();
        assertEquals(bucketSet.size(), results.size());
        for(Integer bucket: results) {
          bucketSet.remove(bucket) ; 
        }
        assertTrue(bucketSet.isEmpty());
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
    
    o = accessor.invoke(new SerializableCallable(
        "Execute function multiple filter") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
        FunctionService.registerFunction(function);
        InternalExecution dataSet = (InternalExecution) FunctionService.onRegion(pr);
        Set<Integer> bucketSet = new HashSet<Integer>();
        bucketSet.add(2);
        bucketSet.add(3);
        ResultCollector<Integer, List<Integer>> rc = (ResultCollector<Integer, List<Integer>>) dataSet
            .withBucketFilter(bucketSet).execute(function);
        
        List<Integer> results = rc.getResult();
        assertEquals(bucketSet.size(), results.size());
        for(Integer bucket: results) {
          bucketSet.remove(bucket) ; 
        }
        assertTrue(bucketSet.isEmpty());
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
    
    o = accessor.invoke(new SerializableCallable(
        "Execute function multiple filter") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
        FunctionService.registerFunction(function);
        InternalExecution dataSet =  (InternalExecution) FunctionService.onRegion(pr);
        Set<Integer> bucketSet = new HashSet<Integer>();
        bucketSet.add(1);
        bucketSet.add(2);
        bucketSet.add(3);
        bucketSet.add(0);
        bucketSet.add(4);
        ResultCollector<Integer, List<Integer>> rc = (ResultCollector<Integer, List<Integer>>) dataSet
            .withBucketFilter(bucketSet).execute(function);
        
        List<Integer> results = rc.getResult();
        assertEquals(bucketSet.size(), results.size());
        for(Integer bucket: results) {
          bucketSet.remove(bucket) ; 
        }
        getCache().getLogger().info("results buckets="+ results);
        getCache().getLogger().info("bucketset="+ bucketSet);
        assertTrue(bucketSet.isEmpty()); 
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
    
  }
  
  public void testBucketFilterOverride() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(3);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    getCache();
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        PartitionResolver resolver = new BucketFilterPRResolver();
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0, resolver);
        getCache().createRegion(
            rName, ra);
        return Boolean.TRUE;
      }
    });

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        PartitionResolver resolver = new BucketFilterPRResolver();
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10, resolver);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    Object o = accessor.invoke(new SerializableCallable("Create data") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        for(int i = 0; i < 50; ++i) {
          pr.put(i, i);
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
    
    o = accessor.invoke(new SerializableCallable(
        "Execute function with bucket filter override") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
        FunctionService.registerFunction(function);
        InternalExecution dataSet = (InternalExecution) FunctionService.onRegion(pr);
        Set<Integer> bucketSet = new HashSet<Integer>();
        bucketSet.add(1);
        Set<Integer> keySet = new HashSet<Integer>();
        keySet.add(33);
        keySet.add(43);
        Set<Integer> expectedBucketSet = new HashSet<Integer>();
        expectedBucketSet.add(3);
        expectedBucketSet.add(4);
        ResultCollector<Integer, List<Integer>> rc = (ResultCollector<Integer, List<Integer>>) dataSet
            .withBucketFilter(bucketSet).withFilter(keySet).execute(function);
        
        List<Integer> results = rc.getResult();
        assertEquals(keySet.size(), results.size());
        for(Integer bucket: results) {
          expectedBucketSet.remove(bucket) ; 
        }
        assertTrue(expectedBucketSet.isEmpty());
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
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION2);
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
        //assertEquals(pr.getTotalNumberOfBuckets(), l.size());
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
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  /**
   * Test ability to execute a multi-key function by a local data store
   *
   * @throws Exception
   */
  public void testLocalMultiKeyExecution_byInstance() throws Exception {
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
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        //DefaultResultCollector rs = new DefaultResultCollector();
        Execution dataSet = FunctionService.onRegion(pr);
        final HashSet testKeysSet = new HashSet();
        testKeysSet.add(testKey);
        try {
          dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(function);
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

        //DefaultResultCollector rc1 = new DefaultResultCollector();
        ResultCollector rc1 = dataSet.withFilter(testKeys).withArgs(Boolean.TRUE)
            .execute(function);
        List l = ((List)rc1.getResult());
        assertEquals(1, l.size());
        for (Iterator i = l.iterator(); i.hasNext();) {
          assertEquals(Boolean.TRUE, i.next());
        }

        //DefaultResultCollector rc2 = new DefaultResultCollector();
        ResultCollector rc2 = dataSet.withFilter(testKeys).withArgs(testKeys)
            .execute(function);
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
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  /**
   * Ensure that the execution is limited to a single bucket put another way,
   * that the routing logic works correctly such that there is not extra
   * execution
   *
   * @throws Exception
   */
  public void testMultiKeyExecutionOnASingleBucket_byName() throws Exception {
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
        Function function = new TestFunction(true,TEST_FUNCTION2);
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
        // Assert there is data each bucket
        for (int bid = 0; bid < pr.getTotalNumberOfBuckets(); bid++) {
          assertTrue(pr.getBucketKeys(bid).size() > 0);
        }
        for (Iterator kiter = testKeys.iterator(); kiter.hasNext();) {
          Set singleKeySet = Collections.singleton(kiter.next());
          Function function = new TestFunction(true,TEST_FUNCTION2);
          FunctionService.registerFunction(function);
          Execution dataSet = FunctionService.onRegion(pr);
          ResultCollector rc1 = dataSet.withFilter(singleKeySet).withArgs(Boolean.TRUE)
              .execute(function.getId());
          List l = ((List)rc1.getResult());
          assertEquals(1, l.size());
          assertEquals(Boolean.TRUE, l.iterator().next());

          //DefaultResultCollector rc2 = new DefaultResultCollector();
          ResultCollector rc2 = dataSet.withFilter(singleKeySet).withArgs(new HashSet(singleKeySet))
              .execute(function.getId());
          List l2 = ((List)rc2.getResult());

          assertEquals(1, l2.size());
          List subList = (List)l2.iterator().next();
          assertEquals(1, subList.size());
          assertEquals(pr.get(singleKeySet.iterator().next()), subList
              .iterator().next());
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  /**
   * Ensure that the execution is limited to a single bucket put another way,
   * that the routing logic works correctly such that there is not extra
   * execution
   *
   * @throws Exception
   */
  public void testMultiKeyExecutionOnASingleBucket_byInstance()
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
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
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
        // Assert there is data each bucket
        for (int bid = 0; bid < pr.getTotalNumberOfBuckets(); bid++) {
          assertTrue(pr.getBucketKeys(bid).size() > 0);
        }
        for (Iterator kiter = testKeys.iterator(); kiter.hasNext();) {
          Set singleKeySet = Collections.singleton(kiter.next());
          Function function = new TestFunction(true,TEST_FUNCTION2);
          FunctionService.registerFunction(function);
          Execution dataSet = FunctionService.onRegion(pr);
          ResultCollector rc1 = dataSet.withFilter(singleKeySet).withArgs(Boolean.TRUE)
              .execute(function);
          List l = ((List)rc1.getResult());
          assertEquals(1, l.size());
          assertEquals(Boolean.TRUE, l.iterator().next());

          //DefaultResultCollector rc2 = new DefaultResultCollector();
          ResultCollector rc2 = dataSet.withFilter(singleKeySet).withArgs(new HashSet(singleKeySet))
              .execute(function);
          List l2 = ((List)rc2.getResult());

          assertEquals(1, l2.size());
          List subList = (List)l2.iterator().next();
          assertEquals(1, subList.size());
          assertEquals(pr.get(singleKeySet.iterator().next()), subList
              .iterator().next());
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
  public void testExecutionOnAllNodes_byName()
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
        //Function function = new TestFunction(true,"TestFunction2");
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
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
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc1 = dataSet.withArgs(Boolean.TRUE)
            .execute(function.getId());
        List l = ((List)rc1.getResult());
        getLogWriter().info(
            "PRFunctionExecutionDUnitTest#testExecutionOnAllNodes_byName : Result size :"
                + l.size() + " Result : " + l);
        assertEquals(4, l.size());
         
        for (int i=0; i<4; i++) {
          assertEquals(Boolean.TRUE, l.iterator().next());
        }
        return Boolean.TRUE;
//        for (int i=0; i<4; i++) {
//          List l = ((List)rc1.getResult());
//          assertEquals(4, l.size());
//          assertEquals(Boolean.TRUE, l.iterator().next());
//        }
//
//        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
 
  /**
   * Ensure that the execution is happening all the PR as a whole
   *
   * @throws Exception
   */
  public void testExecutionOnAllNodes_byInstance()
      throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM accessor = host.getVM(3);
    getCache();
    
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        pa.setTotalNumBuckets(17);
        raf.setPartitionAttributes(pa);
        
        getCache().createRegion(
            rName, raf.create());
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
        pa.setTotalNumBuckets(17);
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        //Function function = new TestFunction(true,"TestFunction2");
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION2);
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
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc1 = dataSet.withArgs(Boolean.TRUE)
            .execute(function);
       
        List l = ((List)rc1.getResult());
        assertEquals(3, l.size());
         
        for (int i=0; i<3; i++) {
          assertEquals(Boolean.TRUE, l.iterator().next());
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
 
  /**
   * Ensure that the execution of inline function is happening all the PR as a whole
   *
   * @throws Exception
   */
  public void testExecutionOnAllNodes_byInlineFunction()
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
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc1 = dataSet.withArgs(Boolean.TRUE)
            .execute(new FunctionAdapter(){
              @Override
              public void execute(FunctionContext context) {
                if (context.getArguments() instanceof String) {
                  context.getResultSender().lastResult("Success");
                }else if(context.getArguments() instanceof Boolean){
                  context.getResultSender().lastResult(Boolean.TRUE);
                }
              }

              @Override
              public String getId() {
                return getClass().getName();
              }

              @Override
              public boolean hasResult() {
                return true;
              }
            });
        List l = ((List)rc1.getResult());
        getLogWriter().info(
            "PRFunctionExecutionDUnitTest#testExecutionOnAllNodes_byName : Result size :"
                + l.size() + " Result : " + l);
        assertEquals(4, l.size());
        Iterator iterator = l.iterator();
        for (int i=0; i<4; i++) {
          assertEquals(Boolean.TRUE, iterator.next());
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
   
  public void testBug40714() throws Exception {
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
        FunctionService.registerFunction(
            new FunctionAdapter() {
              @Override
              public void execute(FunctionContext context) {
                if (context.getArguments() instanceof String) {
                  context.getResultSender().lastResult("Failure");
                }
                else if (context.getArguments() instanceof Boolean) {
                  context.getResultSender().lastResult(Boolean.FALSE);
                }
              }

              @Override
              public String getId() {
                return "Function";
              }

              @Override
              public boolean hasResult() {
                return true;
              }
            });
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
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc1 = dataSet.withArgs(Boolean.TRUE).execute(
            new FunctionAdapter() {
              @Override
              public void execute(FunctionContext context) {
                if (context.getArguments() instanceof String) {
                  context.getResultSender().lastResult("Success");
                }
                else if (context.getArguments() instanceof Boolean) {
                  context.getResultSender().lastResult(Boolean.TRUE);
                }
              }

              @Override
              public String getId() {
                return "Function";
              }

              @Override
              public boolean hasResult() {
                return true;
              }
            });
        List l = ((List)rc1.getResult());
        getLogWriter().info(
            "PRFunctionExecutionDUnitTest#testExecutionOnAllNodes_byName : Result size :"
                + l.size() + " Result : " + l);
        assertEquals(4, l.size());
        Iterator iterator = l.iterator();
        for (int i = 0; i < 4; i++) {
          Boolean res = (Boolean)iterator.next();
          assertEquals(Boolean.TRUE, res);
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }
  /**
   * Ensure that the execution is happening on all the PR as a whole
   * with LocalReadPR as LocalDataSet
   *
   * @throws Exception
   */
  public void testExecutionOnAllNodes_LocalReadPR()
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
        pa.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION3);
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
        
        // here put custid and Customer into the PR
        // later check for them      
        for (int i = 1; i <= 10; i++) {
          CustId custid = new CustId(i);
          Customer customer = new Customer("name" + i, "Address" + i);
          try {
            pr.put(custid, customer);
            assertNotNull(pr.get(custid));
            assertEquals(customer, pr.get(custid));
            testKeys.add(custid);
          }
          catch (Exception e) {
            fail(
                "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
                e);
          }
          getLogWriter().fine("Customer :- { " + custid + " : " + customer + " }");
        }
        
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION3);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc1 = dataSet.withArgs(testKeys)
            .execute(function.getId());
        
        List l = ((List)rc1.getResult());        
        assertEquals(4, l.size());
        ArrayList vals = new ArrayList();
        Iterator itr = l.iterator();
        for (int i=0; i<4; i++) {          
          vals.addAll((ArrayList)itr.next());              
        }
        assertEquals(vals.size(),testKeys.size());  
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  /**
   * Ensure that the execution is happening on all the PR as a whole
   * with LocalReadPR as LocalDataSet
   *
   * @throws Exception
   */
  public void testExecutionOnMultiNodes_LocalReadPR()
      throws Exception {
    //final String rName = getUniqueName();
    final String rName1 = "CustomerPartitionedRegionName";
    final String rName2 = "OrderPartitionedRegionName";
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM datastore3 = host.getVM(3);
    getCache();
    SerializableCallable dataStoreCreate1 = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        pa.setTotalNumBuckets(17);
        pa.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        raf.setPartitionAttributes(pa);
        getCache().createRegion(
            rName1, raf.create());
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
        FunctionService.registerFunction(function);
        
        return Boolean.TRUE;
      }
    };
    
    SerializableCallable dataStoreCreate2 = new SerializableCallable(
    "Create PR with Function Factory") {
  public Object call() throws Exception {
    RegionAttributes ra = PartitionedRegionTestHelper
        .createRegionAttrsForPR(0, 10);
    AttributesFactory raf = new AttributesFactory(ra);
    PartitionAttributesImpl pa = new PartitionAttributesImpl();
    pa.setAll(ra.getPartitionAttributes());
    pa.setTotalNumBuckets(17);
    pa.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    pa.setColocatedWith(rName1);
    raf.setPartitionAttributes(pa);
    getCache().createRegion(
        rName2, raf.create());
    return Boolean.TRUE;
  }
};
    datastore0.invoke(dataStoreCreate1);
    datastore1.invoke(dataStoreCreate1);
    datastore2.invoke(dataStoreCreate1);
    datastore3.invoke(dataStoreCreate1);

    datastore0.invoke(dataStoreCreate2);
    datastore1.invoke(dataStoreCreate2);
    datastore2.invoke(dataStoreCreate2);
    datastore3.invoke(dataStoreCreate2);
    
    Object o = datastore3.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName1);
        DistributedSystem.setThreadsSocketPolicy(false);
        final HashSet testKeys = new HashSet();
        
        // here put custid and Customer into the PR
        // later check for them        
        for (int i = 1; i <= 100; i++) {
          CustId custid = new CustId(i);
          Customer customer = new Customer("name" + i, "Address" + i);
          try {
            pr.put(custid, customer);
            assertNotNull(pr.get(custid));
            assertEquals(customer, pr.get(custid));
            if(i>5)
              testKeys.add(custid);
          }
          catch (Exception e) {
            fail(
                "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
                e);
          }
          getLogWriter().fine("Customer :- { " + custid + " : " + customer + " }");
        }

        PartitionedRegion partitionedregion = (PartitionedRegion)getCache().getRegion(rName2);
        assertNotNull(partitionedregion);
        for (int i = 1; i <= 100; i++) {
          CustId custid = new CustId(i);
          for (int j = 1; j <= 10; j++) {
            int oid = (i * 10) + j;
            OrderId orderId = new OrderId(oid, custid);
            Order order = new Order("OREDR" + oid);
            try {
              partitionedregion.put(orderId, order);
              //assertTrue(partitionedregion.containsKey(orderId));
              //assertEquals(order,partitionedregion.get(orderId));
              
            }
            catch (Exception e) {
              fail(
                  "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
                  e);
            }
            getLogWriter().fine("Order :- { " + orderId + " : " + order + " }");
          }
        }
                
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
                
        ResultCollector rc1 =  dataSet.withFilter(testKeys).execute(function.getId());
        List l = ((List)rc1.getResult());
        assertTrue(4>=l.size());        
        ArrayList vals = new ArrayList();
        Iterator itr = l.iterator();
        for (int i=0; i<l.size(); i++) {          
          vals.addAll((ArrayList)itr.next());              
        }        
        assertEquals(testKeys.size(),vals.size());  
        return Boolean.TRUE;
      }
    });
    assertEquals(Boolean.TRUE, o);
  }

  /**
   * Assert the {@link RegionFunctionContext} yields the proper objects and works
   * in concert with the associated {@link PartitionedRegionUtil} methods.
   * @throws Exception
   */
  public void testLocalDataContext() throws Exception
  {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(1);
    final VM datastore1 = host.getVM(2);
    final VM datastore2 = host.getVM(3);
    getCache();
    final Integer key1 = new Integer(1);
    final Integer key2 = new Integer(2);

    final SerializableCallable createDataStore = new SerializableCallable("Create datastore for " + rName) {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
        .createRegionAttrsForPR(0, 10);
        getCache().createRegion(rName, ra);
        return Boolean.TRUE;
      }
    };
    datastore1.invoke(createDataStore);
    datastore2.invoke(createDataStore);

    accessor.invoke(new SerializableCallable("Create accessor for " + rName +
    ", create buckets") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
        .createRegionAttrsForPR(0, 0);
        Region pr = getCache().createRegion(rName, ra);
        // Assuming that bucket balancing will create a single bucket (per key)
        // in different datastores
        pr.put(key1, key1);
        pr.put(key2, key2);
        return Boolean.TRUE;
      }
    });

    final SerializableCallable assertFuncionContext = new SerializableCallable("Invoke function, assert context") {
      public Object call() throws Exception {
        Region r = getCache().getRegion(rName);
        Function f = new FunctionAdapter() {
          // @Override
          @Override
          public void execute(FunctionContext context) {
            RegionFunctionContext rContext = (RegionFunctionContext) context;
            assertEquals(Collections.singleton(key1), rContext.getFilter());  
            assertTrue(PartitionRegionHelper.isPartitionedRegion(rContext.getDataSet()));
            final Region ld = PartitionRegionHelper.getLocalDataForContext(rContext);
            assertTrue(PartitionRegionHelper.getColocatedRegions(ld).isEmpty());
            // Assert the data is local only
            assertNull(ld.get(key2));
            assertEquals(key1, ld.get(key1));
            assertLocalKeySet(key1, ld.keySet());
            assertLocalValues(key1, ld.values());
            assertLocalEntrySet(key1, ld.entrySet());
            context.getResultSender().lastResult(Boolean.TRUE);
          }
          // @Override
          @Override
          public String getId() {
            return getClass().getName();
          }
        };
        ArrayList res = (ArrayList) FunctionService.onRegion(r).withFilter(Collections.singleton(key1)).execute(f).getResult();
        assertEquals(1, res.size());
        return res.get(0);
      }
    };
    assertTrue(((Boolean) accessor.invoke(assertFuncionContext)).booleanValue());
    assertTrue(((Boolean) datastore1.invoke(assertFuncionContext)).booleanValue());
    assertTrue(((Boolean) datastore2.invoke(assertFuncionContext)).booleanValue());
  }
 
  /**
   * Assert the {@link RegionFunctionContext} yields the proper objects and works
   * in concert with the associated {@link PartitionedRegionUtil} methods when
   * there are colocated regions.
   * @throws Exception
   */
  public void testLocalDataContextWithColocation() throws Exception
  {
    String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM accessor = host.getVM(1);
    final VM datastore1 = host.getVM(2);
    final VM datastore2 = host.getVM(3);
    getCache();
    final Integer key1 = new Integer(1);
    final Integer key2 = new Integer(2);

    final String rName_top = rName + "_top";
    final String rName_colo1 = rName + "_colo1";
    final String rName_colo2 = rName + "_colo2";
    
    final SerializableCallable createDataStore = new SerializableCallable("Create datastore for " + rName + " with colocated Regions") {
      public Object call() throws Exception {
        // create the "top" root region
        createRootRegion(rName_top, createColoRegionAttrs(0, 10, null));
        // create a root region colocated with top
        RegionAttributes colo = createColoRegionAttrs(0, 10, rName_top);
        createRootRegion(rName_colo1, colo);
        // Create a subregion colocated with top
        createRegion(rName_colo2, colo);
        return Boolean.TRUE;
      }
    };
    datastore1.invoke(createDataStore);
    datastore2.invoke(createDataStore);

    accessor.invoke(new SerializableCallable("Create accessor for " + rName
        + " with colocated Regions and create buckets") {
      public Object call() throws Exception {
        // create the "top" root region
        Region rtop = createRootRegion(rName_top, createColoRegionAttrs(0, 0, null));
        // create a root region colocated with top
        RegionAttributes colo = createColoRegionAttrs(0, 0, rName_top);
        Region rc1 = createRootRegion(rName_colo1, colo);
        // Create a subregion colocated with top
        Region rc2 = createRegion(rName_colo2, colo);

        // Assuming that bucket balancing will create a single bucket (per key)
        // in different datastores
        rtop.put(key1, key1);
        rtop.put(key2, key2);
        rc1.put(key1, key1);
        rc1.put(key2, key2);
        rc2.put(key1, key1);
        rc2.put(key2, key2);
        return Boolean.TRUE;
      }
    });
    
    final SerializableCallable assertFuncionContext = new SerializableCallable("Invoke function, assert context with colocation") {
      public Object call() throws Exception {
        Region r = getRootRegion(rName_top);
        Function f = new FunctionAdapter() {
          // @Override
          @Override
          public void execute(FunctionContext context) {
            RegionFunctionContext rContext = (RegionFunctionContext) context;
            assertEquals(Collections.singleton(key1), rContext.getFilter());  
            assertTrue(PartitionRegionHelper.isPartitionedRegion(rContext.getDataSet()));

            final Region pr = rContext.getDataSet();
            final Map<String, ? extends Region> prColos =
              PartitionRegionHelper.getColocatedRegions(pr);

            final Region ld = PartitionRegionHelper.getLocalDataForContext(rContext);
            final Map<String, ? extends Region> ldColos =
              PartitionRegionHelper.getColocatedRegions(ld);

            // Assert the colocation set doesn't contain the "top"
            assertFalse(prColos.containsKey(rName_top));
            assertFalse(ldColos.containsKey(rName_top));

            Region c1 = getRootRegion(rName_colo1);
            Region c2 = getRootRegion().getSubregion(rName_colo2);
            // assert colocated regions and local forms of colocated regions
            {
              assertSame(c1, prColos.get(c1.getFullPath()));
              Region lc = PartitionRegionHelper.getLocalData(c1);
              assertTrue(lc instanceof LocalDataSet);
              assertLocalKeySet(key1, lc.keySet());
              assertLocalValues(key1, lc.values());
              assertLocalEntrySet(key1, lc.entrySet());
            }
            {
              assertSame(c2, prColos.get(c2.getFullPath()));
              Region lc = PartitionRegionHelper.getLocalData(c2);
              assertTrue(lc instanceof LocalDataSet);
              assertLocalEntrySet(key1, lc.entrySet());
              assertLocalKeySet(key1, lc.keySet());
              assertLocalValues(key1, lc.values());
            }
            
            // Assert context's local colocated data
            {
              Region lc1 = ldColos.get(c1.getFullPath());
              assertEquals(c1.getFullPath(), lc1.getFullPath());
              assertTrue(lc1 instanceof LocalDataSet);
              assertLocalEntrySet(key1, lc1.entrySet());
              assertLocalKeySet(key1, lc1.keySet());
              assertLocalValues(key1, lc1.values());
            }
            {
              Region lc2 = ldColos.get(c2.getFullPath());
              assertEquals(c2.getFullPath(), lc2.getFullPath());
              assertTrue(lc2 instanceof LocalDataSet);
              assertLocalEntrySet(key1, lc2.entrySet());
              assertLocalKeySet(key1, lc2.keySet());
              assertLocalValues(key1, lc2.values());
            }

            // Assert both local forms of the "target" region is local only
            assertNull(ld.get(key2));
            assertEquals(key1, ld.get(key1));

            assertLocalEntrySet(key1, ld.entrySet());
            assertLocalKeySet(key1, ld.keySet());
            assertLocalValues(key1, ld.values());
            context.getResultSender().lastResult(Boolean.TRUE);
          }
          // @Override
          @Override
          public String getId() {
            return getClass().getName();
          }
        };
        ArrayList res = (ArrayList) FunctionService.onRegion(r).withFilter(Collections.singleton(key1)).execute(f).getResult();
        assertEquals(1, res.size());
        return res.get(0);
      }
    };
    assertTrue(((Boolean) accessor.invoke(assertFuncionContext)).booleanValue());
    assertTrue(((Boolean) datastore1.invoke(assertFuncionContext)).booleanValue());
    assertTrue(((Boolean) datastore2.invoke(assertFuncionContext)).booleanValue());
    
  }
 
  /**
   * This tests make sure that, in case of LonerDistributedSystem we dont get ClassCast Exception.
   * Just making sure that the function executed on lonerDistribuedSystem
   */
 
  public void testBug41118() {
    Host host = Host.getHost(0);
    final VM lonerVM = host.getVM(1);
    lonerVM.invoke(PRFunctionExecutionDUnitTest.class, "bug41118");
  }
 
  public static void bug41118(){
    InternalDistributedSystem ds = new PRFunctionExecutionDUnitTest("temp").getSystem();
    assertNotNull(ds);
    ds.disconnect();
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    ds = (InternalDistributedSystem)DistributedSystem.connect(props);
    
    DM dm = ds.getDistributionManager();
    assertEquals("Distributed System is not loner", true, dm instanceof LonerDistributionManager);
    
    Cache cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.PARTITION);
    assertNotNull(cache);
    Region region = cache.createRegion("PartitonedRegion", factory.create());
    for (int i = 0; i < 20; i++) {
      region.put("KEY_" + i, "VALUE_" + i);
    }
    Set<String> keysForGet = new HashSet<String>();
    keysForGet.add("KEY_4");
    keysForGet.add("KEY_9");
    keysForGet.add("KEY_7");
    try {
      Execution execution = FunctionService.onRegion(region).withFilter(
          keysForGet).withArgs(Boolean.TRUE);
      ResultCollector rc = execution.execute(new FunctionAdapter() {
        @Override
        public void execute(FunctionContext fc) {
          RegionFunctionContext context = (RegionFunctionContext)fc;
          Set keys = context.getFilter();
          Set keysTillSecondLast = new HashSet();
          int setSize = keys.size();
          Iterator keysIterator = keys.iterator();
          for (int i = 0; i < (setSize - 1); i++) {
            keysTillSecondLast.add(keysIterator.next());
          }
          for (Object k : keysTillSecondLast) {
            context.getResultSender().sendResult(
                (Serializable)PartitionRegionHelper.getLocalDataForContext(
                    context).get(k));
          }
          Object lastResult = keysIterator.next();
          context.getResultSender().lastResult(
              (Serializable)PartitionRegionHelper.getLocalDataForContext(
                  context).get(lastResult));
        }

        @Override
        public String getId() {
          return getClass().getName();
        }
      });
      rc.getResult();
      ds.disconnect();
    }
    catch (Exception e) {
      getLogWriter().info("Exception Occured : " + e.getMessage());
      e.printStackTrace();
      fail("Test failed", e);
    }
  }

  /**
   * Test function that only throws exception.
   */
  public static class TestFunctionException implements Function {

    private static final long serialVersionUID = 3745637361374073217L;

    static final String ID = "TestFunctionException";

    public boolean hasResult() {
      return true;
    }

    public void execute(FunctionContext context) {
      // first send some results
      for (int index = 0; index < 5; ++index) {
        context.getResultSender().sendResult(Integer.valueOf(index));
      }
      // then throw an exception
      throw new NullPointerException("simulated exception with myId: "
          + GemFireCacheImpl.getInstance().getMyId());
    }

    public String getId() {
      return ID;
    }

    public boolean optimizeForWrite() {
      return false;
    }

    public boolean isHA() {
      return false;
    }
  }

  public void testFunctionExecutionException_41779() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore1 = host.getVM(0);
    final VM datastore2 = host.getVM(1);
    final VM datastore3 = host.getVM(2);
    final VM datastore4 = host.getVM(3);
    final Cache cache = getCache();

    @SuppressWarnings("serial")
    final SerializableRunnable createFact = new SerializableRunnable(
        "Create PR with Function Factory") {
      public void run() {
        RegionAttributes<?, ?> ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        @SuppressWarnings("unchecked")
        AttributesFactory<?, ?> raf = new AttributesFactory(ra);

        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        raf.setPartitionAttributes(pa);

        getCache().createRegionFactory(raf.create()).create(rName);
        Function function = new TestFunctionException();
        FunctionService.registerFunction(function);
      }
    };
    // create stores on all VMs including controller
    datastore1.invoke(createFact);
    datastore2.invoke(createFact);
    datastore3.invoke(createFact);
    datastore4.invoke(createFact);
    createFact.run();

    InternalExecution exec = (InternalExecution)FunctionService.onRegion(cache
        .getRegion(rName));
    exec.setWaitOnExceptionFlag(true);
    try {
      List results = (List)exec.execute(TestFunctionException.ID).getResult();
      fail("expected a function exception");
    } catch (FunctionException fe) {
      // expect exceptions from all VMs
      assertEquals(
          "did not get expected number of exceptions: " + fe.getExceptions(),
          5, fe.getExceptions().size());
    }
  }

  protected static void assertLocalValues(final Integer value, final Collection values) {
    assertEquals(values, Collections.singleton(value));
    assertTrue(values.contains(value));
    assertEquals(1, values.size());
    Iterator vsi = values.iterator();
    assertTrue(vsi.hasNext());
    assertEquals(value, vsi.next());
    assertFalse(vsi.hasNext());
  }
  protected static void assertLocalKeySet(final Integer key, final Set keySet) {
    assertEquals(keySet, Collections.singleton(key));
    assertEquals(1, keySet.size());
  }
  protected static void assertLocalEntrySet(final Integer key, final Set entrySet) {
    assertEquals(1, entrySet.size());
    Iterator esi = entrySet.iterator();
    assertTrue(esi.hasNext());
    Region.Entry re = (Entry)esi.next();
    if (re instanceof EntrySnapshot) {
      assertTrue(((EntrySnapshot)re).wasInitiallyLocal());
    } else {
      assertTrue(re.isLocal());
    }
    assertEquals(key, re.getKey());
    assertEquals(key, re.getValue());
    assertFalse(esi.hasNext());
  }
  public static class TestResolver implements PartitionResolver, Serializable
  {
    public String getName() {
      return "ResolverName_" + getClass().getName();
    }
    public Serializable getRoutingObject(EntryOperation opDetails) {
      return (Serializable) opDetails.getKey();
    }
    public void close() {}
    public Properties getProperties() { return new Properties(); }
  }
  protected RegionAttributes createColoRegionAttrs(int red, int mem, String coloRegion) {
    final TestResolver resolver = new TestResolver();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(new PartitionAttributesFactory()
      .setPartitionResolver(resolver)
      .setRedundantCopies(red)
      .setLocalMaxMemory(mem)
      .setColocatedWith(coloRegion)
      .create());
    return attr.create();
  }
  
  public static class BucketFilterPRResolver implements PartitionResolver {    
    
    @Override
    public void close() {            
    }

    @Override
    public Object getRoutingObject(EntryOperation opDetails) {
      Object key = opDetails.getKey();
      return key.hashCode() / 10 ;
    }

    @Override
    public String getName() {
      return "testBucketFilter_1";
    }    
  }
}

