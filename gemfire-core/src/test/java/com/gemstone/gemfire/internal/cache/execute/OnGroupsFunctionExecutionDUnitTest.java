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
import java.util.Collections;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * 
 * @author sbawaska
 */
public class OnGroupsFunctionExecutionDUnitTest extends DistributedTestCase {

  public OnGroupsFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  @Override
  protected final void preTearDown() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache c = null;
        try {
          c = CacheFactory.getAnyInstance();
          if (c != null) {
            c.close();
          }
        } catch (CacheClosedException e) {
        }
        return null;
      }
    });
  }

  static class OnGroupsFunction extends FunctionAdapter {
    private static final long serialVersionUID = -1032915440862585532L;
    public static final String Id = "OnGroupsFunction";
    public static int invocationCount;

    @Override
    public void execute(FunctionContext context) {
      LogWriterUtils.getLogWriter().fine("SWAP:1:executing OnGroupsFunction:"+invocationCount);
      InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
      synchronized (OnGroupsFunction.class) {
    	  invocationCount++;
      }
      ArrayList<String> l = (ArrayList<String>) context.getArguments();
      if (l != null) {
        assertFalse(Collections.disjoint(l, ds.getDistributedMember().getGroups()));
      }
      if(hasResult()) {
        context.getResultSender().lastResult(Boolean.TRUE);
      }
    }

    @Override
    public String getId() {
      return Id;
    }
  }
  
  private void initVM(VM vm, final String groups, final String regionName, final boolean startServer) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.put("groups", groups);
        if (regionName != null) {
          Cache c = null;
          try {
            c = CacheFactory.getInstance(getSystem(props));
            c.close();
          } catch (CacheClosedException cce) {
          }
          c = CacheFactory.create(getSystem(props));
          c.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
          if (startServer) {
            CacheServer s = c.addCacheServer();
            s.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
            s.start();
          }
        } else {
          getSystem(props);
        }
        return null;
      }
    });
  }

  private void registerFunction(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        FunctionService.registerFunction(new OnGroupsFunction());
        return null;
      }
    });
  }

  private void verifyAndResetInvocationCount(VM vm, final int count) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        OnGroupsFunction f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
        
        // assert succeeded, reset count
        synchronized (OnGroupsFunction.class) {
        	assertEquals(count, f.invocationCount);
        	f.invocationCount = 0;
        }
        return null;
      }
    });
  }

  private int getAndResetInvocationCount(VM vm) {
    return (Integer) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        OnGroupsFunction f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
        int count  = 0 ;
        synchronized (OnGroupsFunction.class) {
        	count = f.invocationCount;
        	f.invocationCount = 0;
        }
        return count;
      }
    });
  }

  private int getInvocationCount(VM vm) {
    return (Integer) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        OnGroupsFunction f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
        int count = 0;
        synchronized (OnGroupsFunction.class) {
        	count = f.invocationCount;
        //    f.invocationCount = 0;
		}
        
        return count;
      }
    });
  }

  private void resetInvocationCount(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        OnGroupsFunction f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
        synchronized (OnGroupsFunction.class) {
        	f.invocationCount = 0;
		}
        return null;
      }
    });
  }

  public void testBasicP2PFunctionNoCache() {
    doBasicP2PFunctionNoCache(false);
  }

  public void testBasicP2pRegisteredFunctionNoCache() {
    doBasicP2PFunctionNoCache(true);
  }

  private void doBasicP2PFunctionNoCache(final boolean registerFunction) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    initVM(vm0, "g0,gm", null, false);
    initVM(vm1, "g1", null, false);
    initVM(vm2, "g0,g1", null, false);
    
    if (registerFunction) {
      registerFunction(vm0);
      registerFunction(vm1);
      registerFunction(vm2);
    }
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().fine("SWAP:invoking on gm");
        DistributedSystem ds = getSystem();
        try {
          FunctionService.onMember("no such group");
          fail("expected exception not thrown");
        } catch (FunctionException e) {
        }
        Execution e = FunctionService.onMembers("gm");
        ArrayList<String> args = new ArrayList<String>();
        args.add("gm");
        e = e.withArgs(args);
        if (registerFunction) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 1);
    verifyAndResetInvocationCount(vm1, 0);
    verifyAndResetInvocationCount(vm2, 0);
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        LogWriterUtils.getLogWriter().fine("SWAP:invoking on g0");
        Execution e = FunctionService.onMembers("g0");
        ArrayList<String> args = new ArrayList<String>();
        args.add("g0");
        e = e.withArgs(args);
        if (registerFunction) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 1);
    verifyAndResetInvocationCount(vm1, 0);
    verifyAndResetInvocationCount(vm2, 1);
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        Execution e = FunctionService.onMembers("g1");
        ArrayList<String> args = new ArrayList<String>();
        args.add("g1");
        e = e.withArgs(args);
        if (registerFunction) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 0);
    verifyAndResetInvocationCount(vm1, 1);
    verifyAndResetInvocationCount(vm2, 1);
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().fine("SWAP:invoking on g0 g1");
        InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
        Execution e = FunctionService.onMembers("g0", "g1");
        ArrayList<String> args = new ArrayList<String>();
        args.add("g0");args.add("g1");
        e = e.withArgs(args);
        if (registerFunction) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 1);
    verifyAndResetInvocationCount(vm1, 1);
    verifyAndResetInvocationCount(vm2, 1);
  }

  public void testonMember() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    initVM(vm0, "g0,gm", null, false);
    initVM(vm1, "g1", null, false);
    initVM(vm2, "g0,g1", null, false);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        try {
          FunctionService.onMember("no such group");
          fail("expected exception not thrown");
        } catch (FunctionException e) {
        }
        try {
          FunctionService.onMember();
          fail("expected exception not thrown");
        } catch (FunctionException e) {
        }
        FunctionService.onMember("g1").execute(new OnGroupsFunction()).getResult();
        return null;
      }
    });
    int c0 = getAndResetInvocationCount(vm0);
    int c1 = getAndResetInvocationCount(vm1);
    int c2 = getAndResetInvocationCount(vm2);
    assertEquals(1, c0 + c1 + c2);

    // test that function is invoked locally when this member belongs to group
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        FunctionService.onMember("g0").execute(new OnGroupsFunction()).getResult();
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 1);
    verifyAndResetInvocationCount(vm1, 0);
    verifyAndResetInvocationCount(vm2, 0);
  }
  
  static class OnGroupMultiResultFunction extends FunctionAdapter {
    private static final long serialVersionUID = 8190290175486881994L;
    public static final String Id = "OnGroupMultiResultFunction";

    @Override
    public void execute(FunctionContext context) {
      // send 5 1s
      for (int i=0; i<4; i++) {
        context.getResultSender().sendResult(1);
      }
      context.getResultSender().lastResult(1);
    }

    @Override
    public String getId() {
      return Id;
    }
  }

  public void testBasicP2PFunction() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String regionName = getName();
    
    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1", regionName, false);
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        Execution e = FunctionService.onMembers("mg");
        ArrayList<Integer> l = (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        int sum = 0;
        for (int i=0; i<l.size(); i++) {
          sum += l.get(i);
        }
        assertEquals(5, sum);
        return null;
      }
    });
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        Execution e = FunctionService.onMembers("g0");
        ArrayList<Integer> l = (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        int sum = 0;
        for (int i=0; i<l.size(); i++) {
          sum += l.get(i);
        }
        assertEquals(10, sum);
        return null;
      }
    });
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        Execution e = FunctionService.onMembers("g0", "g1");
        ArrayList<Integer> l = (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        int sum = 0;
        for (int i=0; i<l.size(); i++) {
          sum += l.get(i);
        }
        assertEquals(15, sum);
        return null;
      }
    });
  }

  private int getLocatorPort(VM locator) {
    return (Integer) locator.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return Locator.getLocator().getPort();
      }
    });
  }

  static class OnGroupsExceptionFunction extends FunctionAdapter {
    private static final long serialVersionUID = 6488843931404616442L;
    public static final String Id = "OnGroupsExceptionFunction";

    @Override
    public void execute(FunctionContext context) {
      ArrayList<String> args = (ArrayList<String>) context.getArguments();
      if (args.get(0).equals("runtime")) {
        if (args.size() > 1) {
          String group = args.get(1);
          InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
          if (ds.getDistributedMember().getGroups().contains(group)) {
            throw new NullPointerException();
          }
        } else {
          throw new NullPointerException();
        }
      } else {
        InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
        if (args.size() > 1) {
          String group = args.get(1);
          if (ds.getDistributedMember().getGroups().contains(group)) {
            ds.disconnect();
          }
        } else {
          ds.disconnect();
        }
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public String getId() {
      return Id;
    }
  }

  public void testP2PException () {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String regionName = getName();
    
    //The test function deliberately throws a null pointer exception.
    //which is logged.
    IgnoredException.addIgnoredException(NullPointerException.class.getSimpleName());
    
    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1,g2", regionName, false);
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        Execution e = FunctionService.onMembers("mg");
        ArrayList<String> args = new ArrayList<String>();
        args.add("runtime");
        e = e.withArgs(args);
        try {
          e.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }
        
        Execution e1 = FunctionService.onMembers("g1");
        e1 = e1.withArgs(args);
        try {
          e1.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }
        
        // fail on only one member
        Execution e2 = FunctionService.onMembers("g1");
        args.add("g2");
        e2 = e2.withArgs(args);
        try {
          e2.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }
        return null;
      }
    });
  }

  public void testP2PMemberFailure() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String regionName = getName();
    
    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1,g2", regionName, false);
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        Execution e1 = FunctionService.onMembers("g1");
        ArrayList<String> args = new ArrayList<String>();
        args.add("shutdown");
        e1 = e1.withArgs(args);
        try {
          e1.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof FunctionInvocationTargetException);
        }
        return null;
      }
    });
  }

  public void testP2POneMemberFailure() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String regionName = getName();
    
    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1,g2", regionName, false);
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        Execution e1 = FunctionService.onMembers("g1");
        ArrayList<String> args = new ArrayList<String>();
        args.add("shutdown");
        args.add("g2");
        e1 = e1.withArgs(args);
        try {
          e1.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof FunctionInvocationTargetException);
        }
        return null;
      }
    });
  }

  public void testP2PIgnoreMemberFailure() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String regionName = getName();
    
    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1,g2", regionName, false);
    
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        Execution e1 = FunctionService.onMembers("g1");
        ArrayList<String> args = new ArrayList<String>();
        args.add("shutdown");
        args.add("g2");
        e1 = e1.withArgs(args);
        ((AbstractExecution)e1).setIgnoreDepartedMembers(true);
        ArrayList l = (ArrayList) e1.execute(new OnGroupsExceptionFunction()).getResult();
        assertEquals(2, l.size());
        if (l.get(0) instanceof FunctionInvocationTargetException) {
          assertTrue((Boolean) l.get(1));
        } else if (l.get(0) instanceof Boolean) {
          assertTrue(l.get(1) instanceof FunctionInvocationTargetException);
        } else {
          fail("expected to find a Boolean or throwable at index 0");
        }
        return null;
      }
    });
  }

  public void testBasicClientServerFunction() {
    dotestBasicClientServerFunction(false, true);
  }

  public void testBasicClientServerRegisteredFunction() {
    dotestBasicClientServerFunction(true, true);
  }

  public void testBasicClientServerFunctionNoArgs() {
    dotestBasicClientServerFunction(false, false);
  }

  public void testBasicClientServerRegisteredFunctionNoArgs() {
    dotestBasicClientServerFunction(true, false);
  }

  private void dotestBasicClientServerFunction(final boolean register, final boolean withArgs) {
    Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    VM locator = Host.getLocator();
    final String regionName = getName();
    
    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1", regionName, true);

    if (register) {
      registerFunction(server0);
      registerFunction(server1);
      registerFunction(server2);
    }

    final int locatorPort = getLocatorPort(locator);
    final String hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Cache c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        ClientCache c = ccf.create();

        c.getLogger().info("SWAP:invoking function from client on g0");
        Execution e = InternalFunctionService.onServers(c, "g0");
        if (withArgs) {
          ArrayList<String> args = new ArrayList<String>();
          args.add("g0");
          e = e.withArgs(args);
        }
        if (register) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(server0, 1);
    verifyAndResetInvocationCount(server1, 0);
    verifyAndResetInvocationCount(server2, 1);

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache c = ClientCacheFactory.getAnyInstance();
        c.getLogger().fine("SWAP:invoking function from client on mg");
        Execution e = InternalFunctionService.onServers(c, "mg");
        if (withArgs) {
          ArrayList<String> args = new ArrayList<String>();
          args.add("mg");
          e = e.withArgs(args);
        }
        if (register) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(server0, 1);
    verifyAndResetInvocationCount(server1, 0);
    verifyAndResetInvocationCount(server2, 0);
    
    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache c = ClientCacheFactory.getAnyInstance();
        c.getLogger().fine("SWAP:invoking function from client on g0 g1");
        Execution e = InternalFunctionService.onServers(c, "g0", "g1");
        if (withArgs) {
          ArrayList<String> args = new ArrayList<String>();
          args.add("g0");args.add("g1");
          e = e.withArgs(args);
        }
        if (register) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;    
      }
    });
    verifyAndResetInvocationCount(server0, 1);
    verifyAndResetInvocationCount(server1, 1);
    verifyAndResetInvocationCount(server2, 1);
  }

  public void testStreamingClientServerFunction() {
    Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    VM locator = Host.getLocator();
    final String regionName = getName();
    
    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1", regionName, true);

    final int locatorPort = getLocatorPort(locator);
    final String hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Cache c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        ClientCache c = ccf.create();

        c.getLogger().info("SWAP:invoking function from client on g0");
        Execution e = InternalFunctionService.onServers(c, "g0");
        ArrayList<Integer> l = (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        int sum = 0;
        for (int i=0; i<l.size(); i++) {
          sum += l.get(i);
        }
        assertEquals(10, sum);
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache c = ClientCacheFactory.getAnyInstance();
        c.getLogger().fine("SWAP:invoking function from client on mg");
        Execution e = InternalFunctionService.onServers(c, "mg");
        ArrayList<Integer> l = (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        int sum = 0;
        for (int i=0; i<l.size(); i++) {
          sum += l.get(i);
        }
        assertEquals(5, sum);
        return null;
      }
    });
    
    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache c = ClientCacheFactory.getAnyInstance();
        c.getLogger().fine("SWAP:invoking function from client on g0 g1");
        Execution e = InternalFunctionService.onServers(c, "g0", "g1");
        ArrayList<Integer> l = (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        int sum = 0;
        for (int i=0; i<l.size(); i++) {
          sum += l.get(i);
        }
        assertEquals(15, sum);
        return null;
      }
    });
  }

  public void testOnServer() {
    Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    VM locator = Host.getLocator();
    final String regionName = getName();
    
    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final int locatorPort = getLocatorPort(locator);
    final String hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Cache c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        ClientCache c = ccf.create();

        IgnoredException ex = IgnoredException.addIgnoredException("No member found");
        try {
          InternalFunctionService.onServer(c, "no such group").execute(new OnGroupsFunction()).getResult();
         fail("expected exception not thrown");
        } catch (FunctionException e) {
        } finally {
          ex.remove();
        }

        InternalFunctionService.onServer(c, "g1").execute(new OnGroupsFunction()).getResult();
        return null;
      }
    });
    int c0 = getAndResetInvocationCount(server0);
    int c1 = getAndResetInvocationCount(server1);
    int c2 = getAndResetInvocationCount(server2);
    assertEquals(1, c0 + c1 + c2);

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache c = ClientCacheFactory.getAnyInstance();
        InternalFunctionService.onServer(c, "g0").execute(new OnGroupsFunction()).getResult();
        
        return null;
      }
    });
    
    verifyAndResetInvocationCount(server0, 1);
    verifyAndResetInvocationCount(server1, 0);
    verifyAndResetInvocationCount(server2, 0);
    
    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache c = ClientCacheFactory.getAnyInstance();
        InternalFunctionService.onServer(c, "mg", "g1").execute(new OnGroupsFunction()).getResult();
        
        return null;
      }
    });
    
    c0 = getAndResetInvocationCount(server0);
    c1 = getAndResetInvocationCount(server1);
    c2 = getAndResetInvocationCount(server2);
    assertEquals(2, c0 + c1 + c2);
  }

  public void testClientServerException() {
    Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    VM locator = Host.getLocator();
    final String regionName = getName();
    
    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final int locatorPort = getLocatorPort(locator);
    final String hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Cache c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        ClientCache c = ccf.create();

        IgnoredException expected = IgnoredException.addIgnoredException("No member found");
        try {
          InternalFunctionService.onServers(c, "no such group").execute(new OnGroupsFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException e) {
        } finally {
          expected.remove();
        }

        IgnoredException.addIgnoredException("NullPointerException");
        Execution e = InternalFunctionService.onServers(c, "mg");
        ArrayList<String> args = new ArrayList<String>();
        args.add("runtime");
        e = e.withArgs(args);
        try {
          e.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }
        
        Execution e1 = InternalFunctionService.onServers(c, "g1");
        e1 = e1.withArgs(args);
        try {
          e1.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }
        
        // only one member
        Execution e2 = InternalFunctionService.onServers(c, "g1");
        args.add("g2");
        e2 = e2.withArgs(args);
        try {
          e2.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }
        return null;
      }
    });
  }

  public void testClientServerMemberFailure() {
    Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    VM locator = Host.getLocator();
    final String regionName = getName();
    
    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final int locatorPort = getLocatorPort(locator);
    final String hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Cache c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        ClientCache c = ccf.create();

        Execution e = InternalFunctionService.onServers(c, "g1");
        ArrayList<String> args = new ArrayList<String>();
        args.add("disconnect");
        e = e.withArgs(args);
        
        IgnoredException.addIgnoredException("FunctionInvocationTargetException");
        try {
          e.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof FunctionInvocationTargetException);
        }
        return null;
      }
    });
  }

  public void testClientServerOneMemberFailure() {
    Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    VM locator = Host.getLocator();
    final String regionName = getName();
    
    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final int locatorPort = getLocatorPort(locator);
    final String hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Cache c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        ClientCache c = ccf.create();

        Execution e = InternalFunctionService.onServers(c, "g1");
        ArrayList<String> args = new ArrayList<String>();
        args.add("disconnect");
        args.add("g2");
        e = e.withArgs(args);
        IgnoredException.addIgnoredException("FunctionInvocationTargetException");
        try {
          e.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof FunctionInvocationTargetException);
        }
        return null;
      }
    });
  }

  public void testClientServerIgnoreMemberFailure() {
    Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    VM locator = Host.getLocator();
    final String regionName = getName();
    
    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final int locatorPort = getLocatorPort(locator);
    final String hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Cache c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        ClientCache c = ccf.create();

        Execution e = InternalFunctionService.onServers(c, "g1");
        ArrayList<String> args = new ArrayList<String>();
        args.add("disconnect");
        args.add("g2");
        e = e.withArgs(args);
        ((AbstractExecution)e).setIgnoreDepartedMembers(true);
        ArrayList l = (ArrayList) e.execute(new OnGroupsExceptionFunction()).getResult();
        LogWriterUtils.getLogWriter().info("SWAP:result:"+l);
        assertEquals(2, l.size());
        if (l.get(0) instanceof Throwable) {
          assertTrue((Boolean) l.get(1));
        } else if (l.get(0) instanceof Boolean) {
          assertTrue(l.get(1) instanceof Throwable);
        } else {
          fail("expected to find a Boolean or throwable at index 0");
        }
        return null;
      }
    });
  }

  static class OnGroupsNoAckFunction extends OnGroupsFunction {
    @Override
    public boolean hasResult() {
      return false;
    }
    @Override
    public boolean isHA() {
      return false;
    }
  }

  public void testNoAckGroupsFunction() {
    //Workaround for #52005. This is a product bug
    //that should be fixed
    IgnoredException.addIgnoredException("Cannot return any result");
    Host host = Host.getHost(0);
    final VM server0 = host.getVM(0);
    final VM server1 = host.getVM(1);
    final VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    VM locator = Host.getLocator();
    final String regionName = getName();
    
    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1", regionName, true);

    final int locatorPort = getLocatorPort(locator);
    final String hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Cache c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        ClientCache c = ccf.create();

        c.getLogger().info("SWAP:invoking function from client on g0");
        Execution e = InternalFunctionService.onServers(c, "g0");
        e.execute(new OnGroupsNoAckFunction());
        return null;
      }
    });
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        int c0 = getInvocationCount(server0);
        int c1 = getInvocationCount(server1);
        int c2 = getInvocationCount(server2);
        return (c0 + c1 + c2) == 2;
      }
      @Override
      public String description() {
        return "OnGroupsNoAck invocation count mismatch";
      }
    };
    Wait.waitForCriterion(wc, 30000, 1000, true);

    resetInvocationCount(server0);
    resetInvocationCount(server1);
    resetInvocationCount(server2);

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache c = ClientCacheFactory.getAnyInstance();
        Execution e = InternalFunctionService.onServer(c, "g1");
        e.execute(new OnGroupsNoAckFunction());
        return null;
      }
    });
    // pause here to verify that we do not get more than 1 invocation
    Wait.pause(5000);
    WaitCriterion wc2 = new WaitCriterion() {
      @Override
      public boolean done() {
        int c0 = getInvocationCount(server0);
        int c1 = getInvocationCount(server1);
        int c2 = getInvocationCount(server2);
        return (c0 + c1 + c2) == 1;
      }
      @Override
      public String description() {
        return "OnGroupsNoAck invocation count mismatch";
      }
    };
    Wait.waitForCriterion(wc2, 30000, 1000, true);
    resetInvocationCount(server0);
    resetInvocationCount(server1);
    resetInvocationCount(server2);
  }
}
