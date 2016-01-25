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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.LonerDistributionManager;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.functions.DistribuedRegionFunctionFunctionInvocationException;
import com.gemstone.gemfire.internal.cache.functions.DistributedRegionFunction;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

public class DistributedRegionFunctionExecutionDUnitTest extends
    DistributedTestCase {

  VM replicate1 = null;

  VM replicate2 = null;

  VM replicate3 = null;

  VM normal = null;

  public static final String REGION_NAME = "DistributedRegionFunctionExecutionDUnitTest";

  public static Cache cache = null;

  public static Region region = null;

  public static final Function function = new DistributedRegionFunction();
  
  public static final Function functionWithNoResultThrowsException = new MyFunctionException();

  public DistributedRegionFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    replicate1 = host.getVM(0);
    replicate2 = host.getVM(1);
    replicate3 = host.getVM(2);
    normal = host.getVM(3);
  }
  
  @Override
  public void tearDown2() throws Exception {
    // this test creates a cache that is incompatible with CacheTestCase,
    // so we need to close it and null out the cache variable
    disconnectAllFromDS();
  }

  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    executeFunction();
  }
  
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_SendException() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    executeFunction_SendException();
  }
  
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_NoLastResult() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    executeFunction_NoLastResult();
  }


  
  public void testDistributedRegionFunctionExecutionOnDataPolicyNormal() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    try {
      normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
          "executeFunction");
      fail("Function execution was expecting an Exception as it was executed on Region with DataPolicy.NORMAL");
    }
    catch (Exception expected) {
      
    }
  }

  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "executeFunction");
  }
  
  
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_SendException() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "executeFunction_SendException");
  }
  
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_NoLastResult() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "executeFunction_NoLastResult");
  }
  
  public void testDistributedRegionFunctionExecutionWithFunctionInvocationTargetException() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    registerFunction(new Boolean(true), new Integer(5));

    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });

    // add expected exception to avoid suspect strings
    final ExpectedException ex = addExpectedException("I have been thrown");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "executeFunctionFunctionInvocationTargetException");
    ex.remove();
  }

  public void testDistributedRegionFunctionExecutionWithFunctionInvocationTargetException_WithoutHA() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    registerFunction(new Boolean(false), new Integer(0));
    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    normal
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });
    replicate1
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });
    replicate2
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });
    replicate3
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });

    // add expected exception to avoid suspect strings
    final ExpectedException ex = addExpectedException("I have been thrown");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "executeFunctionFunctionInvocationTargetExceptionWithoutHA");
    ex.remove();
  }

  public void testDistributedRegionFunctionExecutionWithFunctionInvocationTargetExceptionForEmptyDataPolicy() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    registerFunction(new Boolean(true), new Integer(5));

    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });

    // add expected exception to avoid suspect strings
    final ExpectedException ex = addExpectedException("I have been thrown");
    executeFunctionFunctionInvocationTargetException();
    ex.remove();
  }

  public void testDistributedRegionFunctionExecutionWithFunctionInvocationTargetExceptionForEmptyDataPolicy_WithoutHA() {
    createCacheInVm(); // Empty
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    registerFunction(new Boolean(false), new Integer(0));
    createPeer(DataPolicy.EMPTY);
    populateRegion();
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(false),
            new Integer(0) });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(false),
            new Integer(0) });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(false),
            new Integer(0) });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(false),
            new Integer(0) });

    // add expected exception to avoid suspect strings
    final ExpectedException ex = addExpectedException("I have been thrown");
    executeFunctionFunctionInvocationTargetExceptionWithoutHA();
    ex.remove();
  }

  public void testDistributedRegionFunctionExecutionHACacheClosedException() {
    VM empty = normal;

    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.EMPTY });

    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "populateRegion");

    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    /* (original code below is not proper since function execution may not find
     *  any node for execution when the first node closes; now closing cache
     *  from within function body)
    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = empty.invokeAsync(
        DistributedRegionFunctionExecutionDUnitTest.class, "executeFunctionHA");

    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "closeCache");

    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    DistributedTestCase.join(async[0], 50 * 1000, getLogWriter());
    if (async[0].getException() != null) {
      fail("UnExpected Exception Occured : ", async[0].getException());
    }
    List l = (List)async[0].getReturnValue();
    */
    List l = (List)empty.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "executeFunctionHACacheClose");
    assertEquals(5001, l.size());
    for (int i = 0; i < 5001; i++) {
      assertEquals(l.get(i), Boolean.TRUE);
    }
  }
  
  public void testDistributedRegionFunctionExecutionHANodeFailure() {
    VM empty = normal;

    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.EMPTY });

    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "populateRegion");

    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = empty.invokeAsync(
        DistributedRegionFunctionExecutionDUnitTest.class, "executeFunctionHA");

    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "disconnect");

    DistributedTestCase.join(async[0], 50 * 1000, getLogWriter());
    if (async[0].getException() != null) {
      fail("UnExpected Exception Occured : ", async[0].getException());
    }
    List l = (List)async[0].getReturnValue();
    assertEquals(5001, l.size());
    for (int i = 0; i < 5001; i++) {
      assertEquals(l.get(i), Boolean.TRUE);
    }
  }
  

  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer() {
    VM empty1 = replicate3;
    VM empty2 = normal;
    empty2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();

    Integer port1 = (Integer)empty1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.EMPTY });    
    Integer port2 = (Integer) empty2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.EMPTY });
    replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    executeFunction();
  }
  
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer_SendException() {
    VM empty1 = replicate3;
    VM empty2 = normal;
    empty2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();

    Integer port1 = (Integer)empty1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.EMPTY });    
    Integer port2 = (Integer) empty2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.EMPTY });
    replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    executeFunction_SendException();
  }
  
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer_NoLastResult() {
    VM empty1 = replicate3;
    VM empty2 = normal;
    empty2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();

    Integer port1 = (Integer)empty1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.EMPTY });    
    Integer port2 = (Integer) empty2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.EMPTY });
    replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);
    // add ExpectedException's to servers since client can connect to any
    // one of those
    final ExpectedException expectedEx = addExpectedException(
        "did not send last result", empty1);
    final ExpectedException expectedEx2 = addExpectedException(
        "did not send last result", empty2);
    try {
      executeFunction_NoLastResult();
    } finally {
      expectedEx.remove();
      expectedEx2.remove();
    }
  }

  /*
   * Ensure that the while executing the function if the servers is down then
   * the execution is failover to other available server
   */
  public void testServerFailoverWithTwoServerAliveHA()
      throws InterruptedException {

    VM emptyServer1 = replicate1;
    VM client = normal;

    emptyServer1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    client.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInClientVm");

    Integer port1 = (Integer)emptyServer1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.EMPTY });
    Integer port2 = (Integer)replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.REPLICATE });
    replicate3.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    client.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createClientAndPopulateClientRegion", new Object[] { DataPolicy.EMPTY,
            port1, port2 });

    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "stopServerHA");
    int AsyncInvocationArrSize = 1;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = client.invokeAsync(
        DistributedRegionFunctionExecutionDUnitTest.class, "executeFunctionHA");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "startServerHA");
    emptyServer1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "closeCacheHA");
    DistributedTestCase.join(async[0], 4 * 60 * 1000, getLogWriter());
    if (async[0].getException() != null) {
      fail("UnExpected Exception Occured : ", async[0].getException());
    }
    List l = (List)async[0].getReturnValue();
    assertEquals(5001, l.size());
    for (int i = 0; i < 5001; i++) {
      assertEquals(l.get(i), Boolean.TRUE);
    }

  }
  
  public void testDistributedRegionFunctionExecutionOnDataPolicyNormal_ClientServer() {
    VM normal1 = normal;
    VM normal2 = replicate3;
    VM empty = replicate2 ;

    normal1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    normal2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();
    
    Integer port1 = (Integer)normal1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.NORMAL });
    Integer port2 = (Integer)normal2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.NORMAL });    
    empty.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.EMPTY });
    replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);
    // add expected exception
    final ExpectedException ex = addExpectedException(
        "DataPolicy.NORMAL is not supported");
    try {
      executeFunction();
      fail("Function execution was expecting an Exception as it was executed "
          + "on Region with DataPolicy.NORMAL");
    } catch (Exception expected) {
      assertTrue("Got unexpected exception message: " + expected, expected
          .getMessage().contains("DataPolicy.NORMAL is not supported"));
    } finally {
      ex.remove();
    }
  }

  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_ClientServer() {
    VM empty = replicate3;
    
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();
    
    Integer port1 = (Integer)replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.REPLICATE });
    Integer port2 = (Integer)replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.REPLICATE });    
    normal.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    empty.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.EMPTY });

    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    executeUnregisteredFunction();
  }
  
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_ClientServer_WithoutRegister() {
    VM empty = replicate3;
    
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();
    
    Integer port1 = (Integer)replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.REPLICATE });
    Integer port2 = (Integer)replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.REPLICATE });    
    normal.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    empty.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.EMPTY });

    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    executeFunction();
  }
  
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_ClientServer_FunctionInvocationTargetException() {
    VM empty = replicate3;

    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();
    registerFunction(new Boolean(true), new Integer(5));

    Integer port1 = (Integer)replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.REPLICATE });
    Integer port2 = (Integer)replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.REPLICATE });
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.EMPTY });

    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });

    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    // add expected exception to avoid suspect strings
    final ExpectedException ex = addExpectedException("I have been thrown");
    executeFunctionFunctionInvocationTargetException_ClientServer();
    ex.remove();
  }

  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_ClientServer_FunctionInvocationTargetException_WithoutHA() {
    VM empty = replicate3;

    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();
    registerFunction(new Boolean(false), new Integer(0));

    Integer port1 = (Integer)replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.REPLICATE });
    Integer port2 = (Integer)replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.REPLICATE });
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.NORMAL });
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.EMPTY });

    replicate1
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });
    replicate2
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });
    normal
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });
    empty
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });

    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    // add expected exception to avoid suspect strings
    final ExpectedException ex = addExpectedException("I have been thrown");
    executeFunctionFunctionInvocationTargetException_ClientServer_WithoutHA();
    ex.remove();
  }

  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer_FunctionInvocationTargetException() {
    VM empty1 = replicate3;
    VM empty2 = normal;
    empty2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();
    registerFunction(new Boolean(true), new Integer(5));

    Integer port1 = (Integer)empty1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.EMPTY });
    Integer port2 = (Integer)empty2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.EMPTY });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    empty1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    empty2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction", new Object[] { new Boolean(true), new Integer(5) });

    // add expected exception to avoid suspect strings
    final ExpectedException ex = addExpectedException("I have been thrown");
    executeFunctionFunctionInvocationTargetException_ClientServer();
    ex.remove();
  }

  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer_FunctionInvocationTargetException_WithoutHA() {
    VM empty1 = replicate3;
    VM empty2 = normal;
    empty2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();
    registerFunction(new Boolean(false), new Integer(0));

    Integer port1 = (Integer)empty1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.EMPTY });
    Integer port2 = (Integer)empty2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.EMPTY });
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    empty1
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });
    empty2
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });
    replicate1
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });
    replicate2
        .invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "registerFunction", new Object[] { new Boolean(false),
                new Integer(0) });

    // add expected exception to avoid suspect strings
    final ExpectedException ex = addExpectedException("I have been thrown");
    executeFunctionFunctionInvocationTargetException_ClientServer_WithoutHA();
    ex.remove();
  }

  public void testBug40714() {
    VM empty = replicate3;
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    Integer port1 = (Integer)replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.REPLICATE });
    Integer port2 = (Integer)replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.REPLICATE });
    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createPeer", new Object[] { DataPolicy.REPLICATE });

    normal.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction");
    empty.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "registerFunction");
    createCacheInVm();
    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    registerFunction();
    executeInlineFunction();

  }
  
  /**
   * This tests make sure that, in case of LonerDistributedSystem we don't get
   * ClassCast Exception. Just making sure that the function is executed successfully on
   * lonerDistribuedSystem
   */

  public void testBug41118() {
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
    "bug41118");
  }

  /**
   * Test for bug41367: This test is to verify that the"com.gemstone.gemfire.security.AuthenticationRequiredException: No security-* properties are provided"
   * is not thrown. We have to grep for this exception in logs for any
   * occerence.
   */
  public void testBug41367() {
    VM client = replicate1;
    VM server = replicate2;

    server.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm_41367");

    client.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInClientVm_41367");

    Integer port1 = (Integer)server.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class, "createServer",
        new Object[] { DataPolicy.REPLICATE });

    client.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createClient_41367", new Object[] { port1 });

    for (int i = 0; i < 3; i++) {
      try {
        client.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
            "executeFunction_NoResult");
        client.invoke(DistributedRegionFunctionExecutionDUnitTest.class, "put");
      }
      catch (Exception e) {
        fail("Exception " + e + " not expected");
        e.printStackTrace();
      }
    }
  }
  
  public void testFunctionWithNoResultThrowsException(){
    addExpectedException("RuntimeException");
    replicate1.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");
    replicate2.invoke(DistributedRegionFunctionExecutionDUnitTest.class,
        "createCacheInVm");

    createCacheInClientVm();
    
    Integer port1 = (Integer)replicate1.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.REPLICATE });
    Integer port2 = (Integer)replicate2.invoke(
        DistributedRegionFunctionExecutionDUnitTest.class,
        "createServer", new Object[] { DataPolicy.REPLICATE });    
    
    createClientAndPopulateClientRegion(DataPolicy.EMPTY, port1, port2);

    executeFunctionWithNoResultThrowException();
    pause(10000);
  }
  
  public static void executeFunction_NoResult() {
    Function function = new TestFunction(false, TestFunction.TEST_FUNCTION1);
    Execution dataset = FunctionService.onRegion(region);
    dataset.execute(function);
  }

  public static void put() {
    region.put("K1", "B1");
  }

  public static void bug41118(){
    InternalDistributedSystem ds = new DistributedRegionFunctionExecutionDUnitTest("temp").getSystem();
    assertNotNull(ds);
    ds.disconnect();
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    ds = (InternalDistributedSystem)DistributedSystem.connect(props);
    
    DM dm = ds.getDistributionManager();
    assertEquals("Distributed System is not loner", true, dm instanceof LonerDistributionManager);
    
    Cache cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    assertNotNull(cache);
    region = cache.createRegion(REGION_NAME, factory.create());
    try {
      executeInlineFunction();
      ds.disconnect();
    }
    catch (Exception e) {
      getLogWriter().info("Exception Occured : " + e.getMessage());
      e.printStackTrace();
      fail("Test failed", e);
    }
  }
  
  
  public static void executeInlineFunction() {
    List list = (List)FunctionService.onRegion(region).withArgs(Boolean.TRUE)
        .execute(new FunctionAdapter() {
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
        }).getResult();
    assertEquals(1, list.size());
    assertEquals(Boolean.TRUE, list.get(0));
  }

  public static void registerFunction() {
    FunctionService.registerFunction(new FunctionAdapter() {
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
  }
  
  public static void registerFunction(Boolean isHA, Integer retryCount) {
    Function function = new DistribuedRegionFunctionFunctionInvocationException(
        isHA.booleanValue(), retryCount.intValue());
    FunctionService.registerFunction(function);
  }
  
  public static void createCacheInVm() {
    new DistributedRegionFunctionExecutionDUnitTest("temp")
        .createCache(new Properties());
  }

  public static void createCacheInVm_41367() {
    Properties props = new Properties();
    props.put(DistributionConfig.NAME_NAME, "SecurityServer");
    props.put("security-client-authenticator",
        "templates.security.DummyAuthenticator.create");
    new DistributedRegionFunctionExecutionDUnitTest("temp").createCache(props);
  }

  public static void createCacheInClientVm() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    new DistributedRegionFunctionExecutionDUnitTest("temp")
        .createCache(new Properties());
  }

  public static void createCacheInClientVm_41367() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    props.put(DistributionConfig.NAME_NAME, "SecurityClient");
    props.put("security-client-auth-init",
        "templates.security.UserPasswordAuthInit.create");
    props.put("security-username", "reader1");
    props.put("security-password", "reader1");
    new DistributedRegionFunctionExecutionDUnitTest("temp").createCache(props);
  }

  public static void executeFunction() {
    Set filter = new HashSet();
    for (int i = 100; i < 120; i++) {
      filter.add("execKey-" + i);
    }
    FunctionService.onRegion(region).withFilter(filter)
        .execute(function).getResult();
  }
  
  public static void executeFunctionWithNoResultThrowException() {
    Set filter = new HashSet();
    for (int i = 100; i < 120; i++) {
      filter.add("execKey-" + i);
    }
    FunctionService.onRegion(region).withFilter(filter)
        .execute(functionWithNoResultThrowsException);
  }
  
  public static void executeFunction_SendException() {
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
    
    Set filter = new HashSet();
    for (int i = 100; i < 120; i++) {
      filter.add("execKey-" + i);
    }
    ResultCollector rs = FunctionService.onRegion(region).withFilter(filter).withArgs(Boolean.TRUE)
        .execute(function);
    List list = (List)rs.getResult();
    assertTrue(list.get(0) instanceof MyFunctionExecutionException);
    
    rs = FunctionService.onRegion(region).withFilter(filter).withArgs((Serializable)filter)
    .execute(function);
    List resultList = (List)rs.getResult();
    assertEquals((filter.size()+1), resultList.size());
    Iterator resultIterator = resultList.iterator();
    int exceptionCount = 0;
    while(resultIterator.hasNext()){
      Object o = resultIterator.next();
      if(o instanceof MyFunctionExecutionException){
        exceptionCount++;
      }
    }
    assertEquals(1, exceptionCount);
  }
  
  public static void executeFunction_NoLastResult() {
    Set filter = new HashSet();
    for (int i = 100; i < 120; i++) {
      filter.add("execKey-" + i);
    }
    try {
      FunctionService.onRegion(region).withFilter(filter)
          .execute(
              new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT))
          .getResult();
      fail("FunctionException expected : Function did not send last result");
    }
    catch (Exception ex) {
      assertTrue(ex.getMessage().contains("did not send last result"));
    }
  }
  
  
  
  public static void executeUnregisteredFunction() {
    FunctionService.unregisterFunction(function.getId());
    
    Set filter = new HashSet();
    for (int i = 100; i < 120; i++) {
      filter.add("execKey-" + i);
    }
    FunctionService.onRegion(region).withFilter(filter)
        .execute(function).getResult();
  }
  
  public static void executeFunctionFunctionInvocationTargetException() {
    try {
      ResultCollector rc1 = FunctionService.onRegion(region).withArgs(
          Boolean.TRUE).execute(
          "DistribuedRegionFunctionFunctionInvocationException");
      List list = (ArrayList)rc1.getResult();
      assertEquals(5, list.get(0));
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("This is not expected Exception", e);
    }
  }

  public static void executeFunctionFunctionInvocationTargetExceptionWithoutHA() {
    try {
      ResultCollector rc1 = FunctionService.onRegion(region).withArgs(
          Boolean.TRUE).execute(
          "DistribuedRegionFunctionFunctionInvocationException",true,false);
      rc1.getResult();
      fail("Function Invocation Target Exception should be thrown");
    }
    catch (Exception e) {
      e.printStackTrace();
      if (!(e.getCause() instanceof FunctionInvocationTargetException)) {
        fail("FunctionInvocationTargetException should be thrown");
      }
    }
  }

  public static void executeFunctionFunctionInvocationTargetException_ClientServer() {
    try {
      List list = (ArrayList)FunctionService.onRegion(region).withArgs(
          Boolean.TRUE).execute(
          "DistribuedRegionFunctionFunctionInvocationException").getResult();
      assertEquals(1, list.size());
      assertEquals(5, list.get(0));
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("This is not expected Exception", e);
    }
  }

  public static void executeFunctionFunctionInvocationTargetException_ClientServer_WithoutHA() {
    try {
      FunctionService.onRegion(region).withArgs(
          Boolean.TRUE).execute(
          "DistribuedRegionFunctionFunctionInvocationException", true, false).getResult();
      fail("Function Invocation Target Exception should be thrown");
    }
    catch (Exception e) {
      e.printStackTrace();
      if (!(e.getCause() instanceof FunctionInvocationTargetException)) {
        fail("FunctionInvocationTargetException should be thrown");
      }
    }
  }
  
  public static List executeFunctionHA() {
    Set filter = new HashSet();
    for (int i = 100; i < 120; i++) {
      filter.add("execKey-" + i);
    }
    List list = (List)FunctionService.onRegion(region).withFilter(filter)
        .execute(function).getResult();
    return list;
  }

  /**
   * This will do a cache close from within the body of function to simulate
   * failover during function execution.
   */
  public static List executeFunctionHACacheClose() {
    Set filter = new HashSet();
    for (int i = 100; i < 120; i++) {
      filter.add("execKey-" + i);
    }
    // dummy argument Boolean.TRUE indicates that cache should be closed
    // in the function body itself on the first try
    List list = (List)FunctionService.onRegion(region).withFilter(filter)
        .withArgs(Boolean.TRUE).execute(function).getResult();
    return list;
  }

  public static void createClientAndPopulateClientRegion(DataPolicy policy,
      Integer port1, Integer port2) {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port1.intValue())
          .addServer("localhost", port2.intValue())              
          .setPingInterval(3000).setSubscriptionEnabled(false)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(2).create(
              "DistributedRegionFunctionExecutionDUnitTest_pool");
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    factory.setDataPolicy(policy);
    assertNotNull(cache);
    region = cache.createRegion(REGION_NAME, factory.create());
    getLogWriter().info("Client Region Created :" + region);
    assertNotNull(region);
    for (int i = 1; i <= 200; i++) {
      region.put("execKey-" + i, new Integer(i));
    }
  }

  public static void createClient_41367(Integer port1) {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port1.intValue())
          .setPingInterval(3000).setSubscriptionEnabled(false)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(1).setMaxConnections(1)
          .setRetryAttempts(2).create(
              "DistributedRegionFunctionExecutionDUnitTest_pool");
    }
    finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    assertNotNull(cache);
    region = cache.createRegion(REGION_NAME, factory.create());
    getLogWriter().info("Client Region Created :" + region);
    assertNotNull(region);
  }

  public static Integer createServer(DataPolicy policy) {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(policy);
    assertNotNull(cache);
    region = cache.createRegion(REGION_NAME, factory.create());
    getLogWriter().info("Region Created :" + region);
    assertNotNull(region);

    CacheServer server = cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    try {
      server.start();
    }
    catch (IOException e) {
      fail("Failed to start the Server", e);
    }
    assertTrue(server.isRunning());
    return new Integer(server.getPort());
  }
  
  public static void createPeer(DataPolicy policy) {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(policy);
    assertNotNull(cache);
    region = cache.createRegion(REGION_NAME, factory.create());
    getLogWriter().info("Region Created :" + region);
    assertNotNull(region);
  }
  
  public static void populateRegion() {
    assertNotNull(cache);
    region = cache.getRegion(REGION_NAME);
    assertNotNull(region);
    for (int i = 1; i <= 200; i++) {
      region.put("execKey-" + i, new Integer(i));
    }
  }
  
  private void createCache(Properties props) {
    try {
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      getLogWriter().info("Created Cache on peer");
      assertNotNull(cache);
      FunctionService.registerFunction(function);
    }
    catch (Exception e) {
      fail(
          "DistributedRegionFunctionExecutionDUnitTest#createCache() Failed while creating the cache",
          e);
    }
  }
  
  public static void closeCache() {
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
    region.getCache().getLogger().fine(
        "Time wait for Cache Close = " + (endTime - startTime));
    cache.close();
  }
  
  public static void startServerHA() {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 2000, 500, false);
    Collection bridgeServers = cache.getCacheServers();
    getLogWriter().info(
        "Start Server Bridge Servers list : " + bridgeServers.size());
    Iterator bridgeIterator = bridgeServers.iterator();
    CacheServer bridgeServer = (CacheServer)bridgeIterator.next();
    getLogWriter().info("start Server Bridge Server" + bridgeServer);
    try {
      bridgeServer.start();
    }
    catch (IOException e) {
      fail("not able to start the server");
    }
  }  
  
  public static void stopServerHA() {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 1000, 200, false);
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        server.stop();
      }
    }
    catch (Exception e) {
      fail("failed while stopServer()" + e);
    }
  }
  
  public static void closeCacheHA() {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 1000, 200, false);
    if (cache != null && !cache.isClosed()) {
      try {
        Iterator iter = cache.getCacheServers().iterator();
        if (iter.hasNext()) {
          CacheServer server = (CacheServer)iter.next();
          server.stop();
        }
      }
      catch (Exception e) {
        fail("failed while stopServer()" + e);
      }
       cache.close();
       cache.getDistributedSystem().disconnect();
    }
  }
  
  public static void disconnect() {
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
    DistributedTestCase.waitForCriterion(wc, 2000, 200, false);
    long endTime = System.currentTimeMillis();
    region.getCache().getLogger().fine(
        "Time wait for Disconnecting = " + (endTime - startTime));
    cache.getDistributedSystem().disconnect();
  }
  
}

class MyFunctionException extends FunctionAdapter {

  @Override
  public void execute(FunctionContext context) {
    System.out.println("SKSKSK ");
    throw new RuntimeException("failure");
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }

  @Override
  public boolean hasResult() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

}