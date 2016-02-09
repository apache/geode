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
package com.gemstone.gemfire.cache30;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.LossAction;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.ResumptionAction;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.ReconnectListener;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.MembershipManagerHelper;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

@SuppressWarnings("serial")
public class ReconnectDUnitTest extends CacheTestCase
{
  static int locatorPort;
  static Locator locator;
  static DistributedSystem savedSystem;
  static int locatorVMNumber = 3;
  static Thread gfshThread;
  
  Properties dsProperties;
  
  public ReconnectDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final int locPort = this.locatorPort;
    Host.getHost(0).getVM(locatorVMNumber)
      .invoke(new SerializableRunnable("start locator") {
      public void run() {
        try {
          InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
          if (ds != null) {
            ds.disconnect();
          }
          locatorPort = locPort;
          Properties props = getDistributedSystemProperties();
          locator = Locator.startLocatorAndDS(locatorPort, new File(""), props);
          IgnoredException.addIgnoredException("com.gemstone.gemfire.ForcedDisconnectException||Possible loss of quorum");
//          MembershipManagerHelper.getMembershipManager(InternalDistributedSystem.getConnectedInstance()).setDebugJGroups(true);
        } catch (IOException e) {
          Assert.fail("unable to start locator", e);
        }
      }
    });

    beginCacheXml();
    createRegion("myRegion", createAtts());
    finishCacheXml("MyDisconnect");
    //Cache cache = getCache();
    closeCache();
    getSystem().disconnect();
    LogWriterUtils.getLogWriter().fine("Cache Closed ");
  }

  @Override
  public Properties getDistributedSystemProperties() {
    if (dsProperties == null) {
      dsProperties = super.getDistributedSystemProperties();
      dsProperties.put(DistributionConfig.MAX_WAIT_TIME_FOR_RECONNECT_NAME, "20000");
      dsProperties.put(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true");
      dsProperties.put(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "false");
      dsProperties.put(DistributionConfig.LOCATORS_NAME, "localHost["+this.locatorPort+"]");
      dsProperties.put(DistributionConfig.MCAST_PORT_NAME, "0");
      dsProperties.put(DistributionConfig.MEMBER_TIMEOUT_NAME, "1000");
      dsProperties.put(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
    }
    return dsProperties;
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    try {
      Host.getHost(0).getVM(locatorVMNumber).invoke(new SerializableRunnable("stop locator") {
        public void run() {
          if (locator != null) {
            LogWriterUtils.getLogWriter().info("stopping locator " + locator);
            locator.stop();
          }
        }
      });
    } finally {
      Invoke.invokeInEveryVM(new SerializableRunnable() {
        public void run() {
          ReconnectDUnitTest.savedSystem = null;
        }
      });
      disconnectAllFromDS();
    }
  }

  /**
   * Creates some region attributes for the regions being created.
   * */
  private RegionAttributes createAtts()
  {
    AttributesFactory factory = new AttributesFactory();

    {
      // TestCacheListener listener = new TestCacheListener(){}; // this needs to be serializable
      //callbacks.add(listener);
      //factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setScope(Scope.DISTRIBUTED_ACK);
      // factory.setCacheListener(listener);
    }

    return factory.create();
  }

  // quorum check fails, then succeeds
  public void testReconnectWithQuorum() throws Exception {
    IgnoredException.addIgnoredException("killing member's ds");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    final int locPort = locatorPort;

    final String xmlFileLoc = (new File(".")).getAbsolutePath();
    
    // disable disconnects in the locator so we have some stability
    host.getVM(locatorVMNumber).invoke(new SerializableRunnable("disable force-disconnect") {
      public void run() {
        GMSMembershipManager mgr = (GMSMembershipManager)MembershipManagerHelper
            .getMembershipManager(InternalDistributedSystem.getConnectedInstance());
        mgr.disableDisconnectOnQuorumLossForTesting();
      }}
    );

    SerializableCallable create = new SerializableCallable(
    "Create Cache and Regions from cache.xml") {
      public Object call() throws CacheException
      {
        //      DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-num-reconnect-tries", "2");
        props.put("log-file", "autoReconnectVM"+VM.getCurrentVMNum()+"_"+getPID()+".log");
        Cache cache = new CacheFactory(props).create();
        IgnoredException.addIgnoredException("com.gemstone.gemfire.ForcedDisconnectException||Possible loss of quorum");
        Region myRegion = cache.getRegion("root/myRegion");
        ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
        myRegion.put("MyKey1", "MyValue1");
//        MembershipManagerHelper.getMembershipManager(cache.getDistributedSystem()).setDebugJGroups(true); 
        // myRegion.put("Mykey2", "MyValue2");
        return savedSystem.getDistributedMember();
      }
    };
    
    System.out.println("creating caches in vm0, vm1 and vm2");
    vm0.invoke(create);
    vm1.invoke(create);
    vm2.invoke(create);
    
    // view is [locator(3), vm0(15), vm1(10), vm2(10)]
    
    /* now we want to cause vm0 and vm1 to force-disconnect.  This may cause the other
     * non-locator member to also disconnect, depending on the timing
     */
    System.out.println("disconnecting vm0");
    forceDisconnect(vm0);
    Wait.pause(10000);
    System.out.println("disconnecting vm1");
    forceDisconnect(vm1);

    /* now we wait for them to auto-reconnect */
    try {
      System.out.println("waiting for vm0 to reconnect");
      waitForReconnect(vm0);
      System.out.println("waiting for vm1 to reconnect");
      waitForReconnect(vm1);
      System.out.println("done reconnecting vm0 and vm1");
    } catch (Exception e) {
      ThreadUtils.dumpAllStacks();
      throw e;
    }
  }
  
  public void testReconnectOnForcedDisconnect() throws Exception  {
    doTestReconnectOnForcedDisconnect(false);
  }
  
  /** bug #51335 - customer is also trying to recreate the cache */
  // this test is disabled due to a high failure rate during CI test runs.
  // see bug #52160
  public void disabledtestReconnectCollidesWithApplication() throws Exception  {
    doTestReconnectOnForcedDisconnect(true);
  }
  
  public void doTestReconnectOnForcedDisconnect(final boolean createInAppToo) throws Exception {

    IgnoredException.addIgnoredException("killing member's ds");
//    getSystem().disconnect();
//    getLogWriter().fine("Cache Closed ");

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    final int locPort = locatorPort;
    final int secondLocPort = AvailablePortHelper.getRandomAvailableTCPPort();

    DistributedTestUtils.deleteLocatorStateFile(locPort, secondLocPort);


    final String xmlFileLoc = (new File(".")).getAbsolutePath();

    SerializableCallable create1 = new SerializableCallable(
    "Create Cache and Regions from cache.xml") {
      public Object call() throws CacheException
      {
        //      DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "1000");
        props.put("max-num-reconnect-tries", "2");
//        props.put("log-file", "autoReconnectVM"+VM.getCurrentVMNum()+"_"+getPID()+".log");
        Cache cache = new CacheFactory(props).create();
        Region myRegion = cache.getRegion("root/myRegion");
        ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
        myRegion.put("MyKey1", "MyValue1");
        // myRegion.put("Mykey2", "MyValue2");
        return savedSystem.getDistributedMember();
      }
    };

    SerializableCallable create2 = new SerializableCallable(
    "Create Cache and Regions from cache.xml") {
      public Object call() throws CacheException
      {
        //            DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        final Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "5000");
        props.put("max-num-reconnect-tries", "2");
        props.put("start-locator", "localhost["+secondLocPort+"]");
        props.put("locators", props.get("locators")+",localhost["+secondLocPort+"]");
//        props.put("log-file", "autoReconnectVM"+VM.getCurrentVMNum()+"_"+getPID()+".log");
        getSystem(props);
//        MembershipManagerHelper.getMembershipManager(system).setDebugJGroups(true);
        final Cache cache = getCache();
        ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
        Region myRegion = cache.getRegion("root/myRegion");
        //myRegion.put("MyKey1", "MyValue1");
        myRegion.put("Mykey2", "MyValue2");
        assertNotNull(myRegion.get("MyKey1"));
        //getLogWriter().fine("MyKey1 value is : "+myRegion.get("MyKey1"));
        if (createInAppToo) {
          Thread recreateCacheThread = new Thread("ReconnectDUnitTest.createInAppThread") {
            public void run() {
              while (!cache.isClosed()) {
                Wait.pause(100);
              }
              try {
                new CacheFactory(props).create();
                LogWriterUtils.getLogWriter().error("testReconnectCollidesWithApplication failed - application thread was able to create a cache");
              } catch (IllegalStateException cacheExists) {
                // expected
              }
            }
          };
          recreateCacheThread.setDaemon(true);
          recreateCacheThread.start();
        }
        return cache.getDistributedSystem().getDistributedMember();
      }
    };

    vm0.invoke(create1);
    DistributedMember dm = (DistributedMember)vm1.invoke(create2);
    forceDisconnect(vm1);
    DistributedMember newdm = (DistributedMember)vm1.invoke(new SerializableCallable("wait for reconnect(1)") {
      public Object call() {
        final DistributedSystem ds = ReconnectDUnitTest.savedSystem;
        ReconnectDUnitTest.savedSystem = null;
        Wait.waitForCriterion(new WaitCriterion() {
          public boolean done() {
            return ds.isReconnecting();
          }
          public String description() {
            return "waiting for ds to begin reconnecting";
          }
        }, 30000, 1000, true);
        LogWriterUtils.getLogWriter().info("entering reconnect wait for " + ds);
        LogWriterUtils.getLogWriter().info("ds.isReconnecting() = " + ds.isReconnecting());
        boolean failure = true;
        try {
          ds.waitUntilReconnected(60, TimeUnit.SECONDS);
          ReconnectDUnitTest.savedSystem = ds.getReconnectedSystem();
          InternalLocator locator = (InternalLocator)Locator.getLocator();
          assertTrue("Expected system to be restarted", ds.getReconnectedSystem() != null);
          assertTrue("Expected system to be running", ds.getReconnectedSystem().isConnected());
          assertTrue("Expected there to be a locator", locator != null);
          assertTrue("Expected locator to be restarted", !locator.isStopped());
          failure = false;
          return ds.getReconnectedSystem().getDistributedMember();
        } catch (InterruptedException e) {
          LogWriterUtils.getLogWriter().warning("interrupted while waiting for reconnect");
          return null;
        } finally {
          if (failure) {
            ds.disconnect();
          }
        }
      }
    });
    assertNotSame(dm, newdm);
    // force another reconnect and show that stopReconnecting works
    forceDisconnect(vm1);
    boolean stopped = (Boolean)vm1.invoke(new SerializableCallable("wait for reconnect and stop") {
      public Object call() {
        final DistributedSystem ds = ReconnectDUnitTest.savedSystem;
        ReconnectDUnitTest.savedSystem = null;
        Wait.waitForCriterion(new WaitCriterion() {
          public boolean done() {
            return ds.isReconnecting() || ds.getReconnectedSystem() != null;
          }
          public String description() {
            return "waiting for reconnect to commence in " + ds;
          }
          
        }, 10000, 1000, true);
        ds.stopReconnecting();
        assertFalse(ds.isReconnecting());
        DistributedSystem newDs = InternalDistributedSystem.getAnyInstance();
        if (newDs != null) {
          LogWriterUtils.getLogWriter().warning("expected distributed system to be disconnected: " + newDs);
          return false;
        }
        return true;
      }
    });
    assertTrue("expected DistributedSystem to disconnect", stopped);

    // recreate the system in vm1 without a locator and crash it 
    dm = (DistributedMember)vm1.invoke(create1);
    forceDisconnect(vm1);
    newdm = waitForReconnect(vm1);
    assertNotSame("expected a reconnect to occur in member", dm, newdm);
    DistributedTestUtils.deleteLocatorStateFile(locPort);
    DistributedTestUtils.deleteLocatorStateFile(secondLocPort);
  }
  
  private DistributedMember getDMID(VM vm) {
    return (DistributedMember)vm.invoke(new SerializableCallable("get ID") {
      public Object call() {
        ReconnectDUnitTest.savedSystem = InternalDistributedSystem.getAnyInstance();
        return ReconnectDUnitTest.savedSystem.getDistributedMember();
      }
    });
  }
  
  private DistributedMember waitForReconnect(VM vm) {
    return (DistributedMember)vm.invoke(new SerializableCallable("wait for Reconnect and return ID") {
      public Object call() {
    	System.out.println("waitForReconnect invoked");
        final DistributedSystem ds = ReconnectDUnitTest.savedSystem;
        ReconnectDUnitTest.savedSystem = null;
        Wait.waitForCriterion(new WaitCriterion() {
          public boolean done() {
            return ds.isReconnecting();
          }
          public String description() {
            return "waiting for ds to begin reconnecting";
          }
        }, 30000, 1000, true);
        long waitTime = 120;
        LogWriterUtils.getLogWriter().info("VM"+VM.getCurrentVMNum() + " waiting up to "+waitTime+" seconds for reconnect to complete");
        try {
          ds.waitUntilReconnected(waitTime, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          fail("interrupted while waiting for reconnect");
        }
        assertTrue("expected system to be reconnected", ds.getReconnectedSystem() != null);
        int oldViewId = MembershipManagerHelper.getMembershipManager(ds).getLocalMember().getVmViewId();
        int newViewId = ((InternalDistributedMember)ds.getReconnectedSystem().getDistributedMember()).getVmViewId();
        if ( !(newViewId > oldViewId) ) {
          fail("expected a new ID to be assigned.  oldViewId="+oldViewId + "; newViewId=" + newViewId);
        }
        return ds.getReconnectedSystem().getDistributedMember();
      }
    });
  }
  
  
  public void testReconnectALocator() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm3 = host.getVM(3);
    DistributedMember dm, newdm;
    
    final int locPort = locatorPort;
    final int secondLocPort = AvailablePortHelper.getRandomAvailableTCPPort();

    DistributedTestUtils.deleteLocatorStateFile(locPort, secondLocPort);

    final String xmlFileLoc = (new File(".")).getAbsolutePath();

    //This locator was started in setUp.
    File locatorViewLog = new File(vm3.getWorkingDirectory(), "locator"+locatorPort+"views.log");
    assertTrue("Expected to find " + locatorViewLog.getPath() + " file", locatorViewLog.exists());
    long logSize = locatorViewLog.length();

    vm0.invoke(new SerializableRunnable("Create a second locator") {
      public void run() throws CacheException
      {
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("max-wait-time-reconnect", "1000");
        props.put("max-num-reconnect-tries", "2");
        props.put("locators", props.get("locators")+",localhost["+locPort+"]");
        props.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
        try {
          Locator.startLocatorAndDS(secondLocPort, null, props);
        } catch (IOException e) {
          Assert.fail("exception starting locator", e);
        }
      }
    });

    File locator2ViewLog = new File(vm0.getWorkingDirectory(), "locator"+secondLocPort+"views.log");
    assertTrue("Expected to find " + locator2ViewLog.getPath() + " file", locator2ViewLog.exists());
    long log2Size = locator2ViewLog.length();

    // create a cache in vm1 so there is more weight in the system
    SerializableCallable create1 = new SerializableCallable(
    "Create Cache and Regions from cache.xml") {
      public Object call() throws CacheException
      {
        //      DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "1000");
        props.put("max-num-reconnect-tries", "2");
        ReconnectDUnitTest.savedSystem = getSystem(props);
        Cache cache = getCache();
        Region myRegion = cache.getRegion("root/myRegion");
        myRegion.put("MyKey1", "MyValue1");
        // myRegion.put("Mykey2", "MyValue2");
        return savedSystem.getDistributedMember();
      }
    };
    vm1.invoke(create1);

    
    try {
      
      dm = getDMID(vm0);
      createGfshWaitingThread(vm0);
      forceDisconnect(vm0);
      newdm = waitForReconnect(vm0);
      assertGfshWaitingThreadAlive(vm0);

      boolean running = (Boolean)vm0.invoke(new SerializableCallable("check for running locator") {
        public Object call() {
          WaitCriterion wc = new WaitCriterion() {
            public boolean done() {
              return Locator.getLocator() != null;
            }
            public String description() {
              return "waiting for locator to restart";
            }
          };
          Wait.waitForCriterion(wc, 30000, 1000, false);
          if (Locator.getLocator() == null) {
            LogWriterUtils.getLogWriter().error("expected to find a running locator but getLocator() returns null");
            return false;
          }
          if (((InternalLocator)Locator.getLocator()).isStopped()) {
            LogWriterUtils.getLogWriter().error("found a stopped locator");
            return false;
          }
          return true;
        }
      });
      if (!running) {
        fail("expected the restarted member to be hosting a running locator");
      }
      
      assertNotSame("expected a reconnect to occur in the locator", dm, newdm);

      // the log should have been opened and appended with a new view
      assertTrue("expected " + locator2ViewLog.getPath() + " to grow in size",
          locator2ViewLog.length() > log2Size);
      // the other locator should have logged a new view
      assertTrue("expected " + locatorViewLog.getPath() + " to grow in size",
          locatorViewLog.length() > logSize);

    } finally {
      vm0.invoke(new SerializableRunnable("stop locator") {
        public void run() {
          Locator loc = Locator.getLocator();
          if (loc != null) {
            loc.stop();
          }
          if (gfshThread != null && gfshThread.isAlive()) {
            gfshThread.interrupt();
          }
          gfshThread = null;
        }
      });
      DistributedTestUtils.deleteLocatorStateFile(locPort);
      DistributedTestUtils.deleteLocatorStateFile(secondLocPort);
    }
  }
  
  @SuppressWarnings("serial")
  private void createGfshWaitingThread(VM vm) {
    vm.invoke(new SerializableRunnable("create Gfsh-like waiting thread") {
      public void run() {
        final Locator loc = Locator.getLocator();
        assertNotNull(loc);
        gfshThread = new Thread("ReconnectDUnitTest_Gfsh_thread") {
          public void run() {
            try {
              ((InternalLocator)loc).waitToStop();
            } catch (InterruptedException e) {
              System.out.println(Thread.currentThread().getName() + " interrupted - exiting");
            }
          }
        };
        gfshThread.setDaemon(true);
        gfshThread.start();
        System.out.println("created gfsh thread: " + gfshThread);
      }
    });
  }
  
  @SuppressWarnings("serial")
  private void assertGfshWaitingThreadAlive(VM vm) {
    vm.invoke(new SerializableRunnable("assert gfshThread is still waiting") {
      public void run() {
        assertTrue(gfshThread.isAlive());
      }
    });
  }
  
  /**
   * Test the reconnect behavior when the required roles are missing.
   * Reconnect is triggered as a Reliability policy. The test is to
   * see if the reconnect is triggered for the configured number of times
   */
  
  public void testReconnectWithRoleLoss() throws TimeoutException,
      RegionExistsException  {

    final String rr1 = "RoleA";
    final String rr2 = "RoleB";
    final String[] requiredRoles = { rr1, rr2 };
    final int locPort = locatorPort;
    final String xmlFileLoc = (new File(".")).getAbsolutePath();


    beginCacheXml();

    locatorPort = locPort;
    Properties config = getDistributedSystemProperties();
    config.put(DistributionConfig.ROLES_NAME, "");
    config.put(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
//    config.put("log-file", "roleLossController.log");
    //creating the DS
    getSystem(config);

    MembershipAttributes ra = new MembershipAttributes(requiredRoles,
        LossAction.RECONNECT, ResumptionAction.NONE);

    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);

    RegionAttributes attr = fac.create();
    createRootRegion("MyRegion", attr);

    //writing the cachexml file.

    File file = new File("RoleReconnect-cache.xml");
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(getCache(), pw);
      pw.close();
    }
    catch (IOException ex) {
      Assert.fail("IOException during cache.xml generation to " + file, ex);
    }
    closeCache();
    getSystem().disconnect();

    LogWriterUtils.getLogWriter().info("disconnected from the system...");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);

    // Recreating from the cachexml.

    SerializableRunnable roleLoss = new CacheSerializableRunnable(
        "ROLERECONNECTTESTS") {
      public void run2() throws CacheException, RuntimeException
      {
        LogWriterUtils.getLogWriter().info("####### STARTING THE REAL TEST ##########");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+File.separator+"RoleReconnect-cache.xml");
        props.put("max-wait-time-reconnect", "200");
        final int timeReconnect = 3;
        props.put("max-num-reconnect-tries", "3");
        props.put(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
        props.put("log-file", "roleLossVM0.log");

        getSystem(props);

        addReconnectListener();
        
        system.getLogWriter().info("<ExpectedException action=add>" 
            + "CacheClosedException" + "</ExpectedException");
        try{
          getCache();
          throw new RuntimeException("The test should throw a CancelException ");
        }
        catch (CancelException ignor){ // can be caused by role loss during intialization.
          LogWriterUtils.getLogWriter().info("Got Expected CancelException ");
        }
        finally {
          system.getLogWriter().info("<ExpectedException action=remove>" 
              + "CacheClosedException" + "</ExpectedException");
        }
        LogWriterUtils.getLogWriter().fine("roleLoss Sleeping SO call dumprun.sh");
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return reconnectTries >= timeReconnect;
          }
          public String description() {
            return "Waiting for reconnect count " + timeReconnect + " currently " + reconnectTries;
          }
        };
        Wait.waitForCriterion(ev, 60 * 1000, 200, true);
        LogWriterUtils.getLogWriter().fine("roleLoss done Sleeping");
        assertEquals(timeReconnect,
            reconnectTries);
      }

    };

    vm0.invoke(roleLoss);


  }
  
     
  
  public static volatile int reconnectTries;
  
  public static volatile boolean initialized = false;
  
  public static volatile boolean initialRolePlayerStarted = false;
   
  //public static boolean rPut;
  public static Integer reconnectTries(){
    return new Integer(reconnectTries);
  }
  
  public static Boolean isInitialized(){
	  return new Boolean(initialized);
  }
  
  public static Boolean isInitialRolePlayerStarted(){
	  return new Boolean (initialRolePlayerStarted);
  }
  
  
  // See #50944 before enabling the test.  This ticket has been closed with wontFix
  // for the 2014 8.0 release.
  public void DISABLED_testReconnectWithRequiredRoleRegained()throws Throwable {

    final String rr1 = "RoleA";
    //final String rr2 = "RoleB";
    final String[] requiredRoles = { rr1 };
    //final boolean receivedPut[] = new boolean[1];

    final Integer[] numReconnect = new Integer[1];
    numReconnect[0] = new Integer(-1);
    final String myKey = "MyKey";
    final String myValue = "MyValue"; 
    final String regionName = "MyRegion";
    final int locPort = locatorPort;

    // CREATE XML FOR MEMBER THAT WILL SEE ROLE LOSS (in this VM)
    beginCacheXml();

    locatorPort = locPort;
    Properties config = getDistributedSystemProperties();
    config.put(DistributionConfig.ROLES_NAME, "");
    config.put(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
    //creating the DS
    getSystem(config);

    MembershipAttributes ra = new MembershipAttributes(requiredRoles,
        LossAction.RECONNECT, ResumptionAction.NONE);

    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);
    fac.setDataPolicy(DataPolicy.REPLICATE);

    RegionAttributes attr = fac.create();
    createRootRegion(regionName, attr);

    //writing the cachexml file.

    File file = new File("RoleRegained.xml");
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(getCache(), pw);
      pw.close();
    }
    catch (IOException ex) {
      Assert.fail("IOException during cache.xml generation to " + file, ex);
    }
    closeCache();
    //disconnectFromDS();
    getSystem().disconnect(); //added

    // ################################################################### //
    //
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);

    vm0.invoke(new CacheSerializableRunnable("reset reconnect count") {
      @Override
      public void run2() throws CacheException {
        reconnectTries = 0;
      }
    });

    SerializableRunnable roleAPlayerForCacheInitialization = 
        getRoleAPlayerForCacheInitializationRunnable(vm0, locPort, regionName,
            "starting roleAplayer, which will initialize, wait for "
                + "vm0 to initialize, and then close its cache to cause role loss");
    AsyncInvocation avkVm1 = vm1.invokeAsync(roleAPlayerForCacheInitialization);
    
    CacheSerializableRunnable roleLoss = getRoleLossRunnable(vm1, locPort, regionName, myKey, myValue,
        "starting role loss vm.  When the role is lost it will start"
            + " trying to reconnect");
    final AsyncInvocation roleLossAsync = vm0.invokeAsync(roleLoss);
    
    LogWriterUtils.getLogWriter().info("waiting for role loss vm to start reconnect attempts");

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        if (!roleLossAsync.isAlive()) {
          return true;
        }
        Object res = vm0.invoke(ReconnectDUnitTest.class, "reconnectTries");
        if (((Integer)res).intValue() != 0) {
          return true;
        }
        return false;
      }
      public String description() {
        return "waiting for event";
      }
    };
    Wait.waitForCriterion(ev, 120 * 1000, 200, true);

    VM vm2 = host.getVM(2);
    if (roleLossAsync.isAlive()) {

      SerializableRunnable roleAPlayer = getRoleAPlayerRunnable(locPort, regionName, myKey, myValue,
          "starting roleAPlayer in a different vm."
              + "  After this reconnect should succeed in vm0");

      vm2.invoke(roleAPlayer);

      //      long startTime = System.currentTimeMillis();
      /*
      while (numReconnect[0].intValue() > 0){
        if((System.currentTimeMillis()-startTime )> 120000)
          fail("The test failed because the required role not satisfied" +
               "and the number of reconnected tried is not set to zero for " +
               "more than 2 mins");
        try{
          Thread.sleep(15);
        }catch(Exception ee){
          getLogWriter().severe("Exception : "+ee);
        }
      }*/
      LogWriterUtils.getLogWriter().info("waiting for vm0 to finish reconnecting");
      ThreadUtils.join(roleLossAsync, 120 * 1000);
    }

    if (roleLossAsync.getException() != null){
      Assert.fail("Exception in Vm0", roleLossAsync.getException());
    }

    ThreadUtils.join(avkVm1, 30 * 1000);
    if (avkVm1.getException() != null){
      Assert.fail("Exception in Vm1", avkVm1.getException());
    }

  }
  
  private CacheSerializableRunnable getRoleLossRunnable(final VM otherVM, final int locPort,
      final String regionName, final String myKey, final Object myValue,
      final String startupMessage) {

    return new CacheSerializableRunnable("roleloss runnable") {
      public void run2()
      {
        Thread t = null;
        try {
          //  closeCache();
          //  getSystem().disconnect();
          LogWriterUtils.getLogWriter().info(startupMessage);
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return ((Boolean)otherVM.invoke(ReconnectDUnitTest.class, "isInitialRolePlayerStarted")).booleanValue();
            }
            public String description() {
              return null;
            }
          };
          Wait.waitForCriterion(ev, 10 * 1000, 200, true);

          LogWriterUtils.getLogWriter().info("Starting the test and creating the cache and regions etc ...");
          locatorPort = locPort;
          Properties props = getDistributedSystemProperties();
          props.put("cache-xml-file", "RoleRegained.xml");
          props.put("max-wait-time-reconnect", "3000");
          props.put("max-num-reconnect-tries", "8");
          props.put(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());

          getSystem(props);
          system.getLogWriter().info("<ExpectedException action=add>" 
              + "CacheClosedException" + "</ExpectedException");

          try {
            getCache();
          } catch (CancelException e) {
            // can happen if RoleA goes away during initialization
            LogWriterUtils.getLogWriter().info("cache threw CancelException while creating the cache");      
          }
          
          initialized = true;
          
          addReconnectListener();

          ev = new WaitCriterion() {
            public boolean done() {
              LogWriterUtils.getLogWriter().info("ReconnectTries=" + reconnectTries);
              return reconnectTries != 0;
            }
            public String description() {
              return null;
            }
          };
          Wait.waitForCriterion(ev, 30 * 1000, 200, true);

          //        long startTime = System.currentTimeMillis();

          ev = new WaitCriterion() {
            String excuse;
            public boolean done() {
              if (InternalDistributedSystem.getReconnectCount() != 0) {
                excuse = "reconnectCount is " + reconnectTries
                    + " waiting for it to be zero";
                return false;
              }
              Object key = null;
              Object value= null;
              Region.Entry keyValue = null;
              try {
                Cache cache = CacheFactory.getAnyInstance();
                if (cache == null) {
                  excuse = "no cache";
                  return false;
                }
                Region myRegion = cache.getRegion(regionName);
                if (myRegion == null) {
                  excuse = "no region";
                  return false;
                }

                Set keyValuePair = myRegion.entrySet();
                Iterator it = keyValuePair.iterator();
                while (it.hasNext()) {
                  keyValue = (Region.Entry)it.next();
                  key = keyValue.getKey();
                  value = keyValue.getValue();
                }
                if (key == null) {
                  excuse = "key is null";
                  return false;
                }
                if (!myKey.equals(key)) {
                  excuse = "key is wrong";
                  return false;
                }
                if (value == null) {
                  excuse = "value is null";
                  return false;
                }
                if (!myValue.equals(value)) {
                  excuse = "value is wrong";
                  return false;
                }
                LogWriterUtils.getLogWriter().info("All assertions passed");
                LogWriterUtils.getLogWriter().info("MyKey : "+key+" and myvalue : "+value);
                return true;
              }
              catch (CancelException ecc){ 
                // ignor the exception because the cache can be closed/null some times 
                // while in reconnect.
              }
              catch(RegionDestroyedException rex){

              }
              finally {
                LogWriterUtils.getLogWriter().info("waiting for reconnect.  Current status is '"+excuse+"'");
              }
              return false;
            }
            public String description() {
              return excuse;
            }
          };

          Wait.waitForCriterion(ev,  60 * 1000, 200, true); // was 5 * 60 * 1000

          Cache cache = CacheFactory.getAnyInstance();
          if (cache != null) {
            cache.getDistributedSystem().disconnect();
          }
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Error th) {
          LogWriterUtils.getLogWriter().severe("DEBUG", th);
          throw th;
        } finally {
          if (t != null) {
            ThreadUtils.join(t, 2 * 60 * 1000);
          }
          // greplogs won't care if you remove an exception that was never added,
          // and this ensures that it gets removed.
          system.getLogWriter().info("<ExpectedException action=remove>" 
              + "CacheClosedException" + "</ExpectedException");
        }

      }

    }; // roleloss runnable
  }

  private CacheSerializableRunnable getRoleAPlayerRunnable(
      final int locPort, final String regionName, final String myKey, final String myValue,
      final String startupMessage) {
    return new CacheSerializableRunnable(
        "second RoleA player") {
      public void run2() throws CacheException
      {
        LogWriterUtils.getLogWriter().info(startupMessage);
        //closeCache();
        // getSystem().disconnect();
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
        props.put(DistributionConfig.ROLES_NAME, "RoleA");

        getSystem(props);
        getCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.create();
        Region region = createRootRegion(regionName, attr);
        LogWriterUtils.getLogWriter().info("STARTED THE REQUIREDROLES CACHE");
        try{
          Thread.sleep(120);
        }
        catch (Exception ee) {
          fail("interrupted");
        }

        region.put(myKey,myValue);
        try {
          Thread.sleep(5000); // why are we sleeping for 5 seconds here?
          // if it is to give time to avkVm0 to notice us we should have
          // him signal us that he has seen us and then we can exit.
        }
        catch(InterruptedException ee){
          fail("interrupted");
        }
        LogWriterUtils.getLogWriter().info("RolePlayer is done...");


      }


    };
  }
  
  
  private CacheSerializableRunnable getRoleAPlayerForCacheInitializationRunnable(
      final VM otherVM, final int locPort, final String regionName,
      final String startupMessage) {
    return new CacheSerializableRunnable(
        "first RoleA player") {
      public void run2() throws CacheException
      {
        //  closeCache();
        // getSystem().disconnect();
        LogWriterUtils.getLogWriter().info(startupMessage);
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
        props.put(DistributionConfig.ROLES_NAME, "RoleA");

        getSystem(props);
        getCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.create();
        createRootRegion(regionName, attr);
        LogWriterUtils.getLogWriter().info("STARTED THE REQUIREDROLES CACHE");
        initialRolePlayerStarted = true;

        while(!((Boolean)otherVM.invoke(ReconnectDUnitTest.class, "isInitialized")).booleanValue()){
          try{
            Thread.sleep(15);
          }catch(InterruptedException ignor){
            fail("interrupted");
          }
        }
        LogWriterUtils.getLogWriter().info("RoleAPlayerInitializer is done...");
        closeCache();

      }
    };
  }
  
  
  void addReconnectListener() {
    reconnectTries = 0; // reset the count for this listener
    LogWriterUtils.getLogWriter().info("adding reconnect listener");
    ReconnectListener reconlis = new ReconnectListener() {
      public void reconnecting(InternalDistributedSystem oldSys) {
        LogWriterUtils.getLogWriter().info("reconnect listener invoked");
        reconnectTries++;
      }
      public void onReconnect(InternalDistributedSystem system1, InternalDistributedSystem system2) {}
    };
    InternalDistributedSystem.addReconnectListener(reconlis);
  }
  
  private void waitTimeout() throws InterruptedException
  {
    Thread.sleep(500);

  }
  public boolean forceDisconnect(VM vm) {
    return (Boolean)vm.invoke(new SerializableCallable("crash distributed system") {
      public Object call() throws Exception {
        // since the system will disconnect and attempt to reconnect
        // a new system the old reference to DTC.system can cause
        // trouble, so we first null it out.
        DistributedTestCase.system = null;
        final DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
        final Locator oldLocator = Locator.getLocator();
        MembershipManagerHelper.crashDistributedSystem(msys);
        if (oldLocator != null) {
          WaitCriterion wc = new WaitCriterion() {
            public boolean done() {
              return msys.isReconnecting();
            }
            public String description() {
              return "waiting for locator to start reconnecting: " + oldLocator;
            }
          };
          Wait.waitForCriterion(wc, 10000, 50, true);
        }
        return true;
      }
    });
  }
  
  private static int getPID() {
    String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();            
    int idx = name.indexOf('@'); 
    try {             
      return Integer.parseInt(name.substring(0,idx));
    } catch(NumberFormatException nfe) {
      //something changed in the RuntimeMXBean name
    }                         
    return 0;
  }

}
