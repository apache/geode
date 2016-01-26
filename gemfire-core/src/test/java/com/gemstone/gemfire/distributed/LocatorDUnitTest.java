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
package com.gemstone.gemfire.distributed;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.MembershipTestHook;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.MembershipManagerHelper;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.tcp.Connection;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests the ability of the {@link Locator} API to start and stop
 * locators running in remote VMs.
 *
 * @since 4.0
 */
public class LocatorDUnitTest extends DistributedTestCase {

  static TestHook hook;
  
  /**
   * Creates a new <code>LocatorDUnitTest</code>
   */
  public LocatorDUnitTest(String name) {
    super(name);
  }

  private static final String WAIT2_MS_NAME = "LocatorDUnitTest.WAIT2_MS";
  private static final int WAIT2_MS_DEFAULT = 5000; // 2000 -- see bug 36470
  private static final int WAIT2_MS 
      = Integer.getInteger(WAIT2_MS_NAME, WAIT2_MS_DEFAULT).intValue();
  
  private int port1;
  private int port2;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    port1 = -1;
    port2 = -1;
    addExpectedException("Removing shunned member");
  }
  
  @Override
  public void tearDown2() {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
    // delete locator state files so they don't accidentally
    // get used by other tests
    if (port1 > 0) {
      deleteLocatorStateFile(port1);
    }
    if (port2 > 0) {
      deleteLocatorStateFile(port2);
    }
  }
  
  ////////  Test Methods

  
  /**
   * SQLFire uses a colocated locator in a dm-type=normal VM.  This tests that
   * the locator can resume control as coordinator after all locators have been
   * shut down and one is restarted.  It's necessary to have a lock service
   * start so elder failover is forced to happen.  Prior to fixing how this worked
   * it hung with the restarted locator trying to become elder again because
   * it put its address at the beginning of the new view it sent out.
   */
  public void testCollocatedLocatorWithSecurity() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    deleteLocatorStateFile(port1);

    final String locators = getServerHostName(host) + "[" + port1 + "]";
    final Properties properties = new Properties();
    properties.put("mcast-port", "0");
    properties.put("start-locator", locators);
    properties.put("log-level", getDUnitLogLevel());
    properties.put("security-peer-auth-init","com.gemstone.gemfire.distributed.AuthInitializer.create");
    properties.put("security-peer-authenticator","com.gemstone.gemfire.distributed.MyAuthenticator.create");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    properties.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    system = (InternalDistributedSystem)DistributedSystem.connect(properties);
    InternalDistributedMember mbr = system.getDistributedMember();
    assertEquals("expected the VM to have NORMAL vmKind",
        DistributionManager.NORMAL_DM_TYPE, system.getDistributedMember().getVmKind());
    
    properties.remove("start-locator");
    properties.put("log-level", getDUnitLogLevel());
    properties.put("locators", locators);
    SerializableRunnable startSystem = new SerializableRunnable("start system") {
      public void run() {
        system = (InternalDistributedSystem)DistributedSystem.connect(properties);
      }
    };
    vm1.invoke(startSystem);
    vm2.invoke(startSystem);

    // ensure that I, as a collocated locator owner, can create a cache region
    Cache cache = CacheFactory.create(system);
    Region r = cache.createRegionFactory(RegionShortcut.REPLICATE).create("test region");
    assertNotNull("expected to create a region", r);
    
    // create a lock service and have every vm get a lock
    DistributedLockService service = DistributedLockService.create("test service", system);
    service.becomeLockGrantor();
    service.lock("foo0", 0, 0);
    
    vm1.invoke(new SerializableRunnable("get the lock service and lock something") {
      public void run() {
        final DistributedLockService service = DistributedLockService.create("test service", system);
        service.lock("foo1", 0, 0);
      }
    });

    vm2.invoke(new SerializableRunnable("get the lock service and lock something") {
      public void run() {
        final DistributedLockService service = DistributedLockService.create("test service", system);
        service.lock("foo2", 0, 0);
      }
    });
      
      
    // cause elder failover.  vm1 will become the lock grantor
    system.disconnect();
    
    try {
      vm1.invoke(new SerializableRunnable("ensure grantor failover") {
        public void run() {
          final DistributedLockService service = DistributedLockService.getServiceNamed("test service");
          service.lock("foo3", 0, 0);
          waitForCriterion(new WaitCriterion() {
            @Override
            public boolean done() {
              return service.isLockGrantor();
            }
            @Override
            public String description() {
              return "waiting to become lock grantor after shutting down locator/grantor";
            }
            
          }, DistributionConfig.DEFAULT_MEMBER_TIMEOUT * 2, 1000, true);
          assertTrue(service.isLockGrantor());
        }
      });
  
      properties.put("start-locator", locators);
      properties.put("log-level", getDUnitLogLevel());
      system = (InternalDistributedSystem)DistributedSystem.connect(properties);
      System.out.println("done connecting distributed system");
      
      assertEquals("should be the coordinator", system.getDistributedMember(), MembershipManagerHelper.getCoordinator(system));
      NetView view = MembershipManagerHelper.getMembershipManager(system).getView();
      getLogWriter().info("view after becoming coordinator is " + view);
      assertNotSame("should not be the first member in the view ("+view+")", system.getDistributedMember(), view.get(0));
      
      service = DistributedLockService.create("test service", system);

      // now force a non-elder VM to get a lock.  This will hang if the bug is not fixed 
      vm2.invoke(new SerializableRunnable("get the lock service and lock something") {
        public void run() {
          final DistributedLockService service = DistributedLockService.getServiceNamed("test service");
          service.lock("foo4", 0, 0);
        }
      });

      assertFalse("should not have become lock grantor", service.isLockGrantor());
      
      // Now demonstrate that a new member can join and use the lock service
      properties.remove("start-locator");
      vm3.invoke(startSystem);
      vm3.invoke(new SerializableRunnable("get the lock service and lock something(2)") {
        public void run() {
          final DistributedLockService service = DistributedLockService.create("test service", system);
          service.lock("foo5", 0, 0);
        }
      });
      
    } finally {
      disconnectAllFromDS();
    }
  }

  /**
   * Bug 30341 concerns race conditions in JGroups that allow two locators to start up in a
   * split-brain configuration.  To work around this we have always told customers that they
   * need to stagger the starting of locators.  This test configures two locators to start up
   * simultaneously and shows that they find each other and form a single system.
   * 
   * @throws Exception
   */
  public void testStartTwoLocators() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM loc1 = host.getVM(1);
    VM loc2 = host.getVM(2);
    
    final int port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port1 = port1;
    final int port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port2 = port2; // for cleanup in tearDown2
    deleteLocatorStateFile(port1);
    deleteLocatorStateFile(port2);
    final String host0 = getServerHostName(host); 
    final String locators = host0 + "[" + port1 + "]," +
                            host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put("mcast-port", "0");
    properties.put("locators", locators);
    properties.put("enable-network-partition-detection", "false");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put("log-level", getDUnitLogLevel());
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    SerializableCallable startLocator1 = new SerializableCallable("start locator1") {
      @Override
      public Object call() throws Exception {
        try {
          System.setProperty("p2p.joinTimeout", "5000"); // set a short join timeout.  default is 17000ms
          Locator myLocator = Locator.startLocatorAndDS(port1, new File(""), properties);
        } catch (SystemConnectException e) {
          return Boolean.FALSE;
        } catch (GemFireConfigException e) {
          return Boolean.FALSE;
        } finally {
          System.getProperties().remove("p2p.joinTimeout");
        }
        return Boolean.TRUE;
      }
    };
    SerializableCallable startLocator2 = new SerializableCallable("start locator2") {
      @Override
      public Object call() throws Exception {
        try {
          System.setProperty("p2p.joinTimeout", "5000"); // set a short join timeout.  default is 17000ms
          Locator myLocator = Locator.startLocatorAndDS(port2, new File(""), properties);
        } catch (SystemConnectException e) {
          return Boolean.FALSE;
        } finally {
          System.getProperties().remove("p2p.joinTimeout");
        }
        return Boolean.TRUE;
      }
    };
    AsyncInvocation async1 = null;
    AsyncInvocation async2 = null;
    try {
      async2 = loc2.invokeAsync(startLocator2);
      pause(2000);
      async1 = loc1.invokeAsync(startLocator1);
    } finally {
      try {
        if (async1 != null) {
          async1.join(45000);
          if (async1.isAlive()) {
            dumpAllStacks();
          }
          if (async2 != null) {
            async2.join();
            Object result1 = async1.getReturnValue();
            if (result1 instanceof Exception) {
              throw (Exception)result1;
            }
            Object result2 = async2.getReturnValue();
            if (result2 instanceof Exception) {
              throw (Exception)result2;
            }
            // verify that they found each other
            SerializableCallable verify = new SerializableCallable("verify no split-brain") {
              public Object call() {
                InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
                if (sys == null) {
                  fail("no distributed system found");
                }
                Assert.assertTrue(sys.getDM().getViewMembers().size() == 2,
                    "expected 2 members but found " + sys.getDM().getViewMembers().size()
                    );
                return true;
              }
            };
            loc2.invoke(verify);
            loc1.invoke(verify);
          }
        }
      } finally {
        SerializableRunnable r = new SerializableRunnable("stop locator") {
          public void run() {
            Locator loc = Locator.getLocator();
            if (loc != null) {
              loc.stop();
            }
          }
        };
        loc2.invoke(r);
        loc1.invoke(r);
      }
    }
    
  }
  /**
   * test lead member selection
   */
  public void testLeadMemberSelection() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    deleteLocatorStateFile(port1);
    final String locators = getServerHostName(host) + "[" + port1 + "]";
    final Properties properties = new Properties();
    properties.put("mcast-port", "0");
    properties.put("locators", locators);
    properties.put("enable-network-partition-detection", "true");
    properties.put("disable-auto-reconnect", "true");
    
    File logFile = new File("");
    if (logFile.exists()) {
      logFile.delete();
    }
    Locator locator = Locator.startLocatorAndDS(port1, logFile, properties);
    try {
    DistributedSystem sys = locator.getDistributedSystem();
    
    Object[] connectArgs = new Object[]{ properties };
    
    SerializableRunnable disconnect =
      new SerializableRunnable("Disconnect from " + locators) {
          public void run() {
            DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
            if (sys != null && sys.isConnected()) {
              sys.disconnect();
            }
          }
        };
        
    assertTrue(MembershipManagerHelper.getLeadMember(sys) == null);
    
    // connect three vms and then watch the lead member selection as they
    // are disconnected/reconnected
    properties.put("name", "vm1");
    DistributedMember mem1 = (DistributedMember)vm1.invoke(this.getClass(),
        "getDistributedMember", connectArgs);
    
//    assertTrue(MembershipManagerHelper.getLeadMember(sys) != null);
    assertLeadMember(mem1, sys, 5000);
    
    properties.put("name", "vm2");
    DistributedMember mem2 = (DistributedMember)vm2.invoke(this.getClass(),
        "getDistributedMember", connectArgs);
    assertLeadMember(mem1, sys, 5000);
    
    properties.put("name", "vm3");
    DistributedMember mem3 = (DistributedMember)vm3.invoke(this.getClass(),
        "getDistributedMember", connectArgs);
    assertLeadMember(mem1, sys, 5000);
    
    // after disconnecting the first vm, the second one should become the leader
    vm1.invoke(disconnect);
    MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem1);
    assertLeadMember(mem2, sys, 5000);
    
    properties.put("name", "vm1");
    mem1 = (DistributedMember)vm1.invoke(this.getClass(),
        "getDistributedMember", connectArgs);
    assertLeadMember(mem2, sys, 5000);
    
    vm2.invoke(disconnect);
    MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem2);
    assertLeadMember(mem3, sys, 5000);
    
    vm1.invoke(disconnect);
    MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem1);
    assertLeadMember(mem3, sys, 5000);

    vm3.invoke(disconnect);
    MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem3);
    assertLeadMember(null, sys, 5000);

    } finally {
      locator.stop();
    }
  }

  private void assertLeadMember(final DistributedMember member,
      final DistributedSystem sys, long timeout) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        DistributedMember lead = MembershipManagerHelper.getLeadMember(sys);
        if (member != null) {
          return member.equals(lead);
        }
        return (lead == null);
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, timeout, 200, true);
  }
  
  /**
   * test lead member and coordinator failure with network partition detection
   * enabled.  It would be nice for this test to have more than two "server"
   * vms, to demonstrate that they all exit when the leader and potential-
   * coordinator both disappear in the loss-correlation-window, but there
   * are only four vms available for dunit testing.
   * <p>
   * So, we start two locators with admin distributed systems, then start
   * two regular distributed members.
   * <p>
   * We kill the second locator (which is not
   * the view coordinator) and then kill the non-lead member.  That should be
   * okay - the lead and remaining locator continue to run.
   * <p>
   * We then kill the lead member and demonstrate that the original locator
   * (which is now the sole remaining member) shuts itself down.
   */
  public void testLeadAndCoordFailure() throws Exception {
    addExpectedException("Possible loss of quorum due");
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM locvm = host.getVM(3);
    Locator locator = null;
    
    final int port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port1 = port1;
    final int port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port2 = port2; // for cleanup in tearDown2()
    deleteLocatorStateFile(port1, port2);
    final String host0 = getServerHostName(host); 
    final String locators = host0 + "[" + port1 + "]," +
                            host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put("mcast-port", "0");
    properties.put("locators", locators);
    properties.put("enable-network-partition-detection", "true");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put("log-level", getDUnitLogLevel());
//    properties.put("log-level", "fine");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    try {
      final String uname = getUniqueName();
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      final DistributedSystem sys = locator.getDistributedSystem();
      sys.getLogWriter().info("<ExpectedException action=add>java.net.ConnectException</ExpectedException>");
      MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
      locvm.invoke(new SerializableRunnable() {
        public void run() {
          File lf = new File("");
          try {
            Locator.startLocatorAndDS(port2, lf, properties);
          }
          catch (IOException ios) {
            fail("Unable to start locator2", ios);
          }
        }
      });
      
      Object[] connectArgs = new Object[]{ properties };
      
      SerializableRunnable crashLocator =
        new SerializableRunnable("Crash locator") {
          public void run() {
            Locator loc = Locator.getLocators().iterator().next();
            DistributedSystem msys = loc.getDistributedSystem();
            MembershipManagerHelper.crashDistributedSystem(msys);
            loc.stop();
          }
      };


      assertTrue(MembershipManagerHelper.getLeadMember(sys) == null);
      
//      properties.put("log-level", getDUnitLogLevel());
      
      DistributedMember mem1 = (DistributedMember)vm1.invoke(this.getClass(),
          "getDistributedMember", connectArgs);
      vm2.invoke(this.getClass(),
          "getDistributedMember", connectArgs);
      assertLeadMember(mem1, sys, 5000);
      
      assertEquals(sys.getDistributedMember(), MembershipManagerHelper.getCoordinator(sys));
      
      // crash the second vm and the locator.  Should be okay
      crashDistributedSystem(vm2);
      locvm.invoke(crashLocator);
      
      assertTrue("Distributed system should not have disconnected",
          vm1.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));
      
      // ensure quorumLost is properly invoked
      DistributionManager dm = (DistributionManager)((InternalDistributedSystem)sys).getDistributionManager();
      MyMembershipListener listener = new MyMembershipListener();
      dm.addMembershipListener(listener);
  
      // disconnect the first vm and demonstrate that the third vm and the
      // locator notice the failure and exit
      crashDistributedSystem(vm1);

      /* This vm is watching vm1, which is watching vm2 which is watching locvm.
       * It will take 3 * (3 * member-timeout) milliseconds to detect the full
       * failure and eject the lost members from the view.
       */
      
      getLogWriter().info("waiting for my distributed system to disconnect due to partition detection");
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return !sys.isConnected();
        }
        public String description() {
          return null;
        }
      };
      DistributedTestCase.waitForCriterion(ev, 12 * 2000, 200, true);
      if (sys.isConnected()) {
        fail("Distributed system did not disconnect as expected - network partition detection is broken");
      }
      // quorumLost should be invoked if we get a ForcedDisconnect in this situation
      assertTrue("expected quorumLost to be invoked", listener.quorumLostInvoked);
      assertTrue("expected suspect processing initiated by TCPConduit", listener.suspectReasons.contains(Connection.INITIATING_SUSPECT_PROCESSING));
    }
    finally {
      if (locator != null) {
        locator.stop();
      }
      LogWriter bLogger =
        new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
      bLogger.info("<ExpectedException action=remove>service failure</ExpectedException>");
      bLogger.info("<ExpectedException action=remove>java.net.ConnectException</ExpectedException>");
      bLogger.info("<ExpectedException action=remove>com.gemstone.gemfire.ForcedDisconnectException</ExpectedException>");
      disconnectAllFromDS();
    }
  }
  

  /**
   * test lead member failure and normal coordinator shutdown with network partition detection
   * enabled.
   * <p>
   * Start two locators with admin distributed systems, then start
   * two regular distributed members.
   * <p>
   * We kill the lead member and demonstrate that the other members continue
   * to operate normally.
   * <p>
   * We then shut down the group coordinator and observe the second locator
   * pick up the job and the remaining member continues to operate normally.
   */
  public void testLeadFailureAndCoordShutdown() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM locvm = host.getVM(3);
    Locator locator = null;
    
    final int port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port1 = port1;
    final int port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port2 = port2;
    deleteLocatorStateFile(port1, port2);
    final String host0 = getServerHostName(host);
    final String locators = host0 + "[" + port1 + "],"
          + host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put("mcast-port", "0");
    properties.put("locators", locators);
    properties.put("enable-network-partition-detection", "true");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    SerializableRunnable stopLocator = getStopLocatorRunnable();
    
    try {
      final String uname = getUniqueName();
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      DistributedSystem sys = locator.getDistributedSystem();
      locvm.invoke(new SerializableRunnable() {
        public void run() {
          File lf = new File("");
          try {
            Locator.startLocatorAndDS(port2, lf, properties);
            MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
          }
          catch (IOException ios) {
            fail("Unable to start locator2", ios);
          }
        }
      });
      
      Object[] connectArgs = new Object[]{ properties };
      
      SerializableRunnable crashSystem =
        new SerializableRunnable("Crash system") {
          public void run() {
            DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
            msys.getLogWriter().info("<ExpectedException action=add>service failure</ExpectedException>");
            msys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ConnectException</ExpectedException>");
            msys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ForcedDisconnectException</ExpectedException>");
            MembershipManagerHelper.crashDistributedSystem(msys);
          }
      };

      assertTrue(MembershipManagerHelper.getLeadMember(sys) == null);
      
      DistributedMember mem1 = (DistributedMember)vm1.invoke(this.getClass(),
          "getDistributedMember", connectArgs);
      DistributedMember mem2 = (DistributedMember)vm2.invoke(this.getClass(),
          "getDistributedMember", connectArgs);

      assertEquals(mem1, MembershipManagerHelper.getLeadMember(sys));
      
      assertEquals(sys.getDistributedMember(), MembershipManagerHelper.getCoordinator(sys));
      
      MembershipManagerHelper.inhibitForcedDisconnectLogging(true);

      // crash the lead vm. Should be okay
      vm1.invoke(crashSystem);

      pause(4 * 2000); // 4 x the member-timeout
      
      assertTrue("Distributed system should not have disconnected",
          isSystemConnected());
      
      assertTrue("Distributed system should not have disconnected",
          vm2.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));
      
      assertTrue("Distributed system should not have disconnected",
          locvm.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));

      // stop the locator normally.  This should also be okay
      locator.stop();
      
      if (!Locator.getLocators().isEmpty()) {
        // log this for debugging purposes before throwing assertion error
        getLogWriter().warning("found locator " + Locator.getLocators().iterator().next());
      }
      assertTrue("locator is not stopped", Locator.getLocators().isEmpty());
      
      assertTrue("Distributed system should not have disconnected",
          vm2.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));
      
      assertTrue("Distributed system should not have disconnected",
          locvm.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));

      // the remaining non-locator member should now be the lead member
      assertEquals("This test sometimes fails.  If the log contains " +
      		"'failed to collect all ACKs' it is a false failure.",
      		mem2, vm2.invoke(LocatorDUnitTest.class, "getLeadMember", new Object[]{}));
      
      SerializableRunnable disconnect =
        new SerializableRunnable("Disconnect from " + locators) {
            public void run() {
              DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
              if (sys != null && sys.isConnected()) {
                sys.disconnect();
              }
            }
          };

      // disconnect the first vm and demonstrate that the third vm and the
      // locator notice the failure and exit
      vm2.invoke(disconnect);
      locvm.invoke(stopLocator);
    }
    finally {
      MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
      if (locator != null) {
        locator.stop();
      }
      try {
        locvm.invoke(stopLocator);
      } catch (Exception e) {
        getLogWriter().severe("failed to stop locator in vm 3", e);
      }
    }
  }

  /**
   * test lead member failure and normal coordinator shutdown with network partition detection
   * enabled.
   * <p>
   * Start one locators with admin distributed systems, then start
   * two regular distributed members.
   * <p>
   * We kill the lead member and demonstrate that the other members continue
   * to operate normally.
   * <p>
   * We then shut down the group coordinator and observe the second locator
   * pick up the job and the remaining member continues to operate normally.
   */
  // disabled on trunk - should be reenabled on cedar_dev_Oct12
  // this test leaves a CloserThread around forever that logs "pausing" messages every 500 ms
  public void testForceDisconnectAndPeerShutdownCause() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM locvm = host.getVM(3);
    Locator locator = null;
    
    final int port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port1 = port1;
    final int port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port2 = port2;
    deleteLocatorStateFile(port1, port2);
    final String host0 = getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";

    final Properties properties = new Properties();
    properties.put("mcast-port", "0");
    properties.put("locators", locators);
    properties.put("enable-network-partition-detection", "true");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put("log-level", getDUnitLogLevel());
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    SerializableRunnable stopLocator = getStopLocatorRunnable();
    
    try {
      final String uname = getUniqueName();
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      DistributedSystem sys = locator.getDistributedSystem();
      locvm.invoke(new SerializableRunnable() {
        public void run() {
          File lf = new File("");
          try {
            Locator.startLocatorAndDS(port2, lf, properties);
          }
          catch (IOException ios) {
            fail("Unable to start locator2", ios);
          }
        }
      });
      
      Object[] connectArgs = new Object[]{ properties };
      
      SerializableRunnable crashSystem =
        new SerializableRunnable("Crash system") {
          public void run() {
            DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
            msys.getLogWriter().info("<ExpectedException action=add>service failure</ExpectedException>");
            msys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ConnectException</ExpectedException>");
            msys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ForcedDisconnectException</ExpectedException>");
            msys.getLogWriter().info("<ExpectedException action=add>Possible loss of quorum</ExpectedException>");
            hook = new TestHook();
            MembershipManagerHelper.getMembershipManager(msys).registerTestHook(hook);
            try {
              MembershipManagerHelper.crashDistributedSystem(msys);
            } finally {
              hook.reset();
            }
          }
      };

      assertTrue(MembershipManagerHelper.getLeadMember(sys) == null);
      
      final DistributedMember mem1 = (DistributedMember)vm1.invoke(this.getClass(),
          "getDistributedMember", connectArgs);
      final DistributedMember mem2 = (DistributedMember)vm2.invoke(this.getClass(),
          "getDistributedMember", connectArgs);

      assertEquals(mem1, MembershipManagerHelper.getLeadMember(sys));
      
      assertEquals(sys.getDistributedMember(), MembershipManagerHelper.getCoordinator(sys));
      
      // crash the lead vm. Should be okay. it should hang in test hook thats
      // why call is asynchronous.
      //vm1.invokeAsync(crashSystem);

      assertTrue("Distributed system should not have disconnected",
          isSystemConnected());
      
      assertTrue("Distributed system should not have disconnected",
          vm2.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));
      
      assertTrue("Distributed system should not have disconnected",
          locvm.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));

      vm2.invokeAsync(crashSystem);

      pause(1000); // 4 x the member-timeout
      
      // request member removal for first peer from second peer.
      vm2.invoke(new SerializableRunnable("Request Member Removal") {
        
        @Override
        public void run() {
          DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
          MembershipManager mmgr = MembershipManagerHelper.getMembershipManager(msys);
          
          // check for shutdown cause in MembershipManager. Following call should
          // throw DistributedSystemDisconnectedException which should have cause as
          // ForceDisconnectException.
          try {
            msys.getLogWriter().info("<ExpectedException action=add>Membership: requesting removal of </ExpectedException>");
            mmgr.requestMemberRemoval(mem1, "test reasons");
            msys.getLogWriter().info("<ExpectedException action=remove>Membership: requesting removal of </ExpectedException>");
            fail("It should have thrown exception in requestMemberRemoval");
          } catch (DistributedSystemDisconnectedException e) {
            Throwable cause = e.getCause();
            assertTrue(
                "This should have been ForceDisconnectException but found "
                    + cause, cause instanceof ForcedDisconnectException);
          } finally {
            hook.reset();
          }
        }
      });

    }
    finally {
      if (locator != null) {
        locator.stop();
      }
      locvm.invoke(stopLocator);
      assertTrue("locator is not stopped", Locator.getLocators().isEmpty());
    }
  }

  /**
   * test lead member shutdown and coordinator crashing with network partition detection
   * enabled.
   * <p>
   * Start two locators with admin distributed systems, then start
   * two regular distributed members.
   * <p>
   * We kill the coordinator and shut down the lead member and observe the second locator
   * pick up the job and the remaining member continue to operate normally.
   */
  public void testLeadShutdownAndCoordFailure() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM locvm = host.getVM(3);
    Locator locator = null;
    
    final int port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port1 = port1;
    final int port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.port2 = port2;
    deleteLocatorStateFile(port1, port2);
    final String host0 = getServerHostName(host);
    final String locators = host0 + "[" + port1 + "],"
          + host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put("mcast-port", "0");
    properties.put("locators", locators);
    properties.put("enable-network-partition-detection", "true");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    SerializableRunnable disconnect =
      new SerializableRunnable("Disconnect from " + locators) {
          public void run() {
            DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
            if (sys != null && sys.isConnected()) {
              sys.disconnect();
            }
            MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
          }
        };
    SerializableRunnable expectedException =
      new SerializableRunnable("Add expected exceptions") {
        public void run() {
          MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
        }
    };
    try {
      final String uname = getUniqueName();
      locvm.invoke(new SerializableRunnable() {
        public void run() {
          File lf = new File("");
          try {
            Locator.startLocatorAndDS(port2, lf, properties);
          }
          catch (IOException ios) {
            fail("Unable to start locator1", ios);
          }
        }
      });

      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      DistributedSystem sys = locator.getDistributedSystem();
      sys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ForcedDisconnectException</ExpectedException>");
      Object[] connectArgs = new Object[]{ properties };
      
      SerializableRunnable crashLocator =
        new SerializableRunnable("Crash locator") {
          public void run() {
            Locator loc = Locator.getLocators().iterator().next();
            DistributedSystem msys = loc.getDistributedSystem();
            msys.getLogWriter().info("<ExpectedException action=add>service failure</ExpectedException>");
            msys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ForcedDisconnectException</ExpectedException>");
            msys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ConnectException</ExpectedException>");
            MembershipManagerHelper.crashDistributedSystem(msys);
            loc.stop();
          }
      };

      assertTrue(MembershipManagerHelper.getLeadMember(sys) == null);
      
      DistributedMember mem1 = (DistributedMember)vm1.invoke(this.getClass(),
          "getDistributedMember", connectArgs);
      vm1.invoke(expectedException);
      DistributedMember mem2 = (DistributedMember)vm2.invoke(this.getClass(),
          "getDistributedMember", connectArgs);
      
      DistributedMember loc1Mbr = (DistributedMember)locvm.invoke(this.getClass(),
          "getLocatorDistributedMember", new Object[]{});

      assertLeadMember(mem1, sys, 5000);
      
      assertEquals(loc1Mbr, MembershipManagerHelper.getCoordinator(sys));
      
      // crash the lead locator.  Should be okay
      locvm.invoke(crashLocator);
      pause(10 * 1000);

      assertTrue("Distributed system should not have disconnected",
          sys.isConnected());
      
      assertTrue("Distributed system should not have disconnected",
          vm1.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));
      
      assertTrue("Distributed system should not have disconnected",
          vm2.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));

      // disconnect the first vm and demonstrate that the non-lead vm and the
      // locator notice the failure and continue to run
      vm1.invoke(disconnect);
      pause(10 * 1000);
      
      assertTrue("Distributed system should not have disconnected",
          vm2.invokeBoolean(LocatorDUnitTest.class, "isSystemConnected"));
      
      assertEquals(sys.getDistributedMember(),
          MembershipManagerHelper.getCoordinator(sys));
      assertEquals(mem2, MembershipManagerHelper.getLeadMember(sys));
      
    }
    finally {
      vm2.invoke(disconnect);
      
      if (locator != null) {
        locator.stop();
      }
      locvm.invoke(getStopLocatorRunnable());
    }
  }

  /**
   * Tests that attempting to connect to a distributed system in which
   * no locator is defined throws an exception.
   */
  public void testNoLocator() {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    int port =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    deleteLocatorStateFile(port1);
    String locators = getServerHostName(host) + "[" + port + "]";
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", locators);

    final String expected = "java.net.ConnectException";
    final String addExpected = 
      "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected = 
      "<ExpectedException action=remove>" + expected + "</ExpectedException>";
      
    LogWriter bgexecLogger =
          new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
    bgexecLogger.info(addExpected);
    
    boolean exceptionOccurred = true;
    String oldValue = (String)System.getProperties().put("p2p.joinTimeout", "15000");
    try {
      DistributedSystem.connect(props);
      exceptionOccurred = false;

    } catch (DistributionException ex) {
      // I guess it can throw this too...

    } catch (GemFireConfigException ex) {
      String s = ex.getMessage();
      assertTrue(s.indexOf("Locator does not exist") >= 0);
      
    } catch (Exception ex) {
      // if you see this fail, determine if unexpected exception is expected
      // if expected then add in a catch block for it above this catch
      fail("Failed with unexpected exception", ex);
    }
    finally {
      if (oldValue == null) {
        System.getProperties().remove("p2p.joinTimeout");
      } else {
        System.getProperties().put("p2p.joinTimeout", oldValue);
      }
      bgexecLogger.info(removeExpected);
    }

    if (!exceptionOccurred) {
      fail("Should have thrown a GemFireConfigException");
    }
  }

  /**
   * Tests starting one locator in a remote VM and having multiple
   * members of the distributed system join it.  This ensures that
   * members start up okay, and that handling of a stopped locator
   * is correct.
   */
  public void testOneLocator() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final int port =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    deleteLocatorStateFile(port1);
    final String locators = getServerHostName(host) + "[" + port + "]";
    final String uniqueName = getUniqueName();
    
    vm0.invoke(new SerializableRunnable("Start locator " + locators) {
        public void run() {
          File logFile = new File("");
          try {
            Properties locProps = new Properties();
            locProps.setProperty("mcast-port", "0");
            locProps.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

            Locator.startLocatorAndDS(port, logFile, locProps);
          } catch (IOException ex) {
            fail("While starting locator on port " + port, ex);
          }
        }
      });
    try {

    SerializableRunnable connect =
      new SerializableRunnable("Connect to " + locators) {
          public void run() {
            //System.setProperty("p2p.joinTimeout", "5000");
            Properties props = new Properties();
            props.setProperty("mcast-port", "0");
            props.setProperty("locators", locators);
            DistributedSystem.connect(props);
          }
        };
    vm1.invoke(connect);
    vm2.invoke(connect);

    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", locators);

    system = (InternalDistributedSystem)DistributedSystem.connect(props);
    
    final DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
    getLogWriter().info("coordinator before termination of locator is " + coord);

    vm0.invoke(getStopLocatorRunnable());
    
    // now ensure that one of the remaining members became the coordinator
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return !coord.equals(MembershipManagerHelper.getCoordinator(system));
      }
      public String description() {
        return "expected the coordinator to be " + coord + " but it is " +
          MembershipManagerHelper.getCoordinator(system);
      }
    };
    DistributedTestCase.waitForCriterion(ev, 15 * 1000, 200, true);
    DistributedMember newCoord = MembershipManagerHelper.getCoordinator(system); 
    getLogWriter().info("coordinator after shutdown of locator was " +
        newCoord);
    if (coord.equals(newCoord)) {
      fail("another member should have become coordinator after the locator was stopped");
    }
      

    system.disconnect();

    SerializableRunnable disconnect =
      new SerializableRunnable("Disconnect from " + locators) {
          public void run() {
            DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
            if (sys != null && sys.isConnected()) {
              sys.disconnect();
            }
          }
        };
    vm1.invoke(disconnect);
    vm2.invoke(disconnect);

    } finally {
      vm0.invoke(getStopLocatorRunnable());
    }
  }

//  public void testRepeat() throws Exception {
//    for (int i=0; i<10; i++) {
//      System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> run #"+i);
//      testLocatorBecomesCoordinator();
//      tearDown();
//      setUp();
//    }
//  }
  /**
   * Tests starting one locator in a remote VM and having multiple
   * members of the distributed system join it.  This ensures that
   * members start up okay, and that handling of a stopped locator
   * is correct.  It then restarts the locator to demonstrate that
   * it can connect to and function as the group coordinator
   */
  public void testLocatorBecomesCoordinator() throws Exception {
    disconnectAllFromDS();
    final String expected = "java.net.ConnectException";
    final String addExpected = 
      "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected = 
      "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final int port =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    deleteLocatorStateFile(port1);
    final String locators = getServerHostName(host) + "[" + port + "]";
    
    vm0.invoke(getStartSBLocatorRunnable(port, getUniqueName()+"1"));
    try {

    final Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", locators);
    props.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    SerializableRunnable connect =
      new SerializableRunnable("Connect to " + locators) {
          public void run() {
            //System.setProperty("p2p.joinTimeout", "5000");
            DistributedSystem sys = getSystem(props);
            sys.getLogWriter().info(addExpected);
          }
        };
    vm1.invoke(connect);
    vm2.invoke(connect);

    system = (InternalDistributedSystem)getSystem(props);

    final DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
    getLogWriter().info("coordinator before termination of locator is " + coord);

    vm0.invoke(getStopLocatorRunnable());
    
    // now ensure that one of the remaining members became the coordinator
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return !coord.equals(MembershipManagerHelper.getCoordinator(system));
      }
      public String description() {
        return "expected the coordinator to be " + coord + " but it is " +
          MembershipManagerHelper.getCoordinator(system);
      }
    };
    DistributedTestCase.waitForCriterion(ev, 15000, 200, true);
    DistributedMember newCoord = MembershipManagerHelper.getCoordinator(system); 
    getLogWriter().info("coordinator after shutdown of locator was " +
        newCoord);
    if (newCoord == null || coord.equals(newCoord)) {
      fail("another member should have become coordinator after the locator was stopped: "
          + newCoord);
    }
      

    // restart the locator to demonstrate reconnection & make disconnects faster
    // it should also regain the role of coordinator, so we check to make sure
    // that the coordinator has changed
    vm0.invoke(getStartSBLocatorRunnable(port, getUniqueName()+"2"));
    
    final DistributedMember tempCoord = newCoord;
    ev = new WaitCriterion() {
      public boolean done() {
        return !tempCoord.equals(MembershipManagerHelper.getCoordinator(system));
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 5000, 200, true);
    
    system.disconnect();
    LogWriter bgexecLogger =
      new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
    bgexecLogger.info(removeExpected);

    SerializableRunnable disconnect =
      new SerializableRunnable("Disconnect from " + locators) {
          public void run() {
            DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
            if (sys != null && sys.isConnected()) {
              sys.disconnect();
            }
            // connectExceptions occur during disconnect, so we need the
            // expectedexception hint to be in effect until this point
            LogWriter bLogger =
              new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
            bLogger.info(removeExpected);
          }
        };
    vm1.invoke(disconnect);
    vm2.invoke(disconnect);
    vm0.invoke(getStopLocatorRunnable());
    } finally {
      vm0.invoke(getStopLocatorRunnable());
    }

  }

  
  /** set a short locator refresh rate */
  public static void setShortRefreshWait() {
    System.setProperty("p2p.gossipRefreshRate", "2000");
  }
  
  /** remove shortened locator refresh rate */
  public static void resetRefreshWait() {
    System.getProperties().remove("p2p.gossipRefreshRate");
  }
  
  public static boolean isSystemConnected() {
    DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys != null && sys.isConnected()) {
      return true;
    }
    return false;
  }
  

  static boolean beforeFailureNotificationReceived;
  static boolean afterFailureNotificationReceived;


  /**
   * Tests starting multiple locators in multiple VMs.
   */
  public void testMultipleLocators() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = freeTCPPorts[0];
    this.port1 = port1;
    final int port2 = freeTCPPorts[1];
    this.port2 = port2;
    deleteLocatorStateFile(port1, port2);
    final String host0 = getServerHostName(host); 
    final String locators = host0 + "[" + port1 + "]," +
                            host0 + "[" + port2 + "]";

    final Properties dsProps = new Properties();
    dsProps.setProperty("locators", locators);
    dsProps.setProperty("mcast-port", "0");
    dsProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
//    dsProps.setProperty("log-level", "finest");
    final String uniqueName = getUniqueName();

    vm0.invoke(new SerializableRunnable("Start locator on " + port1) {
      public void run() {
        File logFile = new File("");
        try {
          Locator.startLocatorAndDS(port1, logFile, dsProps);
        } catch (IOException ex) {
          fail("While starting locator on port " + port1, ex);
        }
      }
    });
    try {

      //try { Thread.currentThread().sleep(4000); } catch (InterruptedException ie) { }

      vm3.invoke(new SerializableRunnable("Start locator on " + port2) {
        public void run() {
          File logFile = new File("");
          try {
            Locator.startLocatorAndDS(port2, logFile, dsProps);

          } catch (IOException ex) {
            fail("While starting locator on port " + port2, ex);
          }
        }
      });
      try {

        SerializableRunnable connect =
          new SerializableRunnable("Connect to " + locators) {
          public void run() {
            Properties props = new Properties();
            props.setProperty("mcast-port", "0");
            props.setProperty("locators", locators);
            DistributedSystem.connect(props);
          }
        };
        vm1.invoke(connect);
        vm2.invoke(connect);

        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", locators);

        system = (InternalDistributedSystem)DistributedSystem.connect(props);

        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            try {
              return system.getDM().getViewMembers().size() >= 3;
            }
            catch (Exception e) {
              e.printStackTrace();
              fail("unexpected exception");
            }
            return false; // NOTREACHED
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);

        // three applications plus
        assertEquals(5, system.getDM().getViewMembers().size());

        system.disconnect();

        SerializableRunnable disconnect =
          new SerializableRunnable("Disconnect from " + locators) {
          public void run() {
            DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
            if (sys != null && sys.isConnected()) {
              sys.disconnect();
            }
          }
        };
        vm1.invoke(disconnect);
        vm2.invoke(disconnect);

      } finally {
        vm3.invoke(getStopLocatorRunnable());
      }
    } finally {
      vm0.invoke(getStopLocatorRunnable());
    }
  }
  
  /**
   * Tests starting multiple locators in multiple VMs.
   */
  public void testMultipleMcastLocators() throws Exception {
    disconnectAllFromDS();
    addExpectedException("Could not stop  Distribution Locator"); // shutdown timing issue in InternalLocator
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    final int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = freeTCPPorts[0];
    this.port1 = port1;
    final int port2 = freeTCPPorts[1];
    this.port2 = port2;
    deleteLocatorStateFile(port1, port2);
    final int mcastport = AvailablePort.getRandomAvailablePort(AvailablePort.MULTICAST);
    
    final String host0 = getServerHostName(host); 
    final String locators = host0 + "[" + port1 + "]," +
                            host0 + "[" + port2 + "]";
    final String uniqueName = getUniqueName();
    
    vm0.invoke(new SerializableRunnable("Start locator on " + port1) {
        public void run() {
          File logFile = new File("");
          try {
            Properties props = new Properties();
            props.setProperty("mcast-port", String.valueOf(mcastport));
            props.setProperty("locators", locators);
            props.setProperty("log-level", getDUnitLogLevel());
            props.setProperty("mcast-ttl", "0");
            props.setProperty("enable-network-partition-detection", "true");
            props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

            Locator.startLocatorAndDS(port1, logFile, null, props);
          }
          catch (IOException ex) {
            fail("While starting locator on port " + port1, ex);
          }
        }
      });
    vm3.invoke(new SerializableRunnable("Start locator on " + port2) {
        public void run() {
          File logFile = new File("");
          try {
            Properties props = new Properties();
            props.setProperty("mcast-port", String.valueOf(mcastport));
            props.setProperty("locators", locators);
            props.setProperty("log-level", getDUnitLogLevel());
            props.setProperty("mcast-ttl", "0");
            props.setProperty("enable-network-partition-detection", "true");
            props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
            Locator.startLocatorAndDS(port2, logFile, null, props);
          }
          catch (IOException ex) {
            fail("While starting locator on port " + port2, ex);
          }
        }
      });

    SerializableRunnable connect =
      new SerializableRunnable("Connect to " + locators) {
          public void run() {
            Properties props = new Properties();
            props.setProperty("mcast-port", String.valueOf(mcastport));
            props.setProperty("locators", locators);
            props.setProperty("log-level", getDUnitLogLevel());
            props.setProperty("mcast-ttl", "0");
            props.setProperty("enable-network-partition-detection", "true");
            DistributedSystem.connect(props);
          }
        };
    try {
      vm1.invoke(connect);
      vm2.invoke(connect);

      Properties props = new Properties();
      props.setProperty("mcast-port", String.valueOf(mcastport));
      props.setProperty("locators", locators);
      props.setProperty("log-level", getDUnitLogLevel());
      props.setProperty("mcast-ttl", "0");
      props.setProperty("enable-network-partition-detection", "true");

      system = (InternalDistributedSystem)DistributedSystem.connect(props);
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          try {
            return system.getDM().getViewMembers().size() == 5;
          }
          catch (Exception e) {
            fail("unexpected exception", e);
          }
          return false; // NOTREACHED
        }
        public String description() {
          return "waiting for 5 members - have " + system.getDM().getViewMembers().size();
        }
      };
      DistributedTestCase.waitForCriterion(ev, WAIT2_MS, 200, true);
      system.disconnect();

      SerializableRunnable disconnect =
        new SerializableRunnable("Disconnect from " + locators) {
            public void run() {
              DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
              if (sys != null && sys.isConnected()) {
                sys.disconnect();
              }
            }
          };
      vm1.invoke(disconnect);
      vm2.invoke(disconnect);
    }
    finally {
      SerializableRunnable stop = getStopLocatorRunnable();
      vm0.invoke(stop);
      vm3.invoke(stop);
      if (system != null) {
        system.disconnect();
      }
    }
  }


  /**
   * Tests that a VM can connect to a locator that is hosted in its
   * own VM.
   */
  public void testConnectToOwnLocator() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);

    port1 =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    deleteLocatorStateFile(port1);
    File logFile = new File("");
    Locator locator = Locator.startLocator(port1, logFile);
    try {

    final String locators = getServerHostName(host) + "[" + port1 + "]";

    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", locators);
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    system = (InternalDistributedSystem)DistributedSystem.connect(props);
    system.disconnect();
    } finally {
    locator.stop();
    }
  }

  /**
   * Tests that a VM cannot connect if the locator has a different
   * enable-network-partition-detection setting
   */
  public void testLocatorForcesDetection() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    Locator locator = null;
    
    try {
      port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      deleteLocatorStateFile(port1);
      final String locators = getServerHostName(host) + "[" + port1 + "]";
      final Properties properties = new Properties();
      properties.put("mcast-port", "0");
      properties.put("locators", locators);
      properties.put(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true");
      properties.put("disable-auto-reconnect", "true");
      properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
  
      final Properties properties2 = new Properties();
      properties2.put("mcast-port", "0");
      properties2.put("locators", locators);
      properties2.put(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "false");
      properties2.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
      properties2.put("disable-auto-reconnect", "true");
      
      vm1.invoke(new SerializableRunnable("try to connect") {
        public void run() {
          DistributedSystem s = null;
          try {
            s = DistributedSystem.connect(properties2);
            boolean enabled = ((InternalDistributedSystem)s).getConfig().getEnableNetworkPartitionDetection();
            s.disconnect();
            if (!enabled) {
              fail("should not have been able to connect with different enable-network-partition-detection settings");
            }
          }
          catch (GemFireConfigException e) {
            fail("should have been able to connect and have enable-network-partion-detection enabled");
          }
        }
      });
      
      locator.stop();
      
      // now start the locator with enable-network-partition-detection=false
      logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile , properties2);
  
      vm1.invoke(new SerializableRunnable("try to connect") {
        public void run() {
          DistributedSystem s = null;
          try {
            s = DistributedSystem.connect(properties);
            s.disconnect();
            fail("should not have been able to connect with different enable-network-partition-detection settings");
          }
          catch (GemFireConfigException e) {
            // passed
          }
        }
      });
      
      locator.stop();
      locator = null;
    }
    finally {
      if (locator != null) {
        locator.stop();
      }
    }
  }

  /**
   * Tests that a single VM can NOT host multiple locators
   */
  public void testHostingMultipleLocators() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    //VM vm = host.getVM(0);
    //VM vm1 = host.getVM(1);

    port1 =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    File logFile1 = new File("");
    deleteLocatorStateFile(port1);
    Locator locator1 = Locator.startLocator(port1, logFile1);
    
    try {

    int port2 =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    File logFile2 = new File("");

    deleteLocatorStateFile(port2);
    
    try {
      Locator locator2 = Locator.startLocator(port2, logFile2);
      fail("expected second locator start to fail.");
    } catch (IllegalStateException expected) {
    }

    final String host0 = getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," +
                            host0 + "[" + port2 + "]";

    SerializableRunnable connect =
      new SerializableRunnable("Connect to " + locators) {
          public void run() {
            Properties props = new Properties();
            props.setProperty("mcast-port", "0");
            props.setProperty("locators", locators);
            props.setProperty("log-level", getDUnitLogLevel());
            DistributedSystem.connect(props);
          }
        };
    connect.run();
    //vm1.invoke(connect);

    SerializableRunnable disconnect =
      new SerializableRunnable("Disconnect from " + locators) {
          public void run() {
            DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
            if (sys != null && sys.isConnected()) {
              sys.disconnect();
            }
          }
        };

    disconnect.run();
    //vm1.invoke(disconnect);

    } finally {
    locator1.stop();
    }
  }

  /**
   * Tests starting, stopping, and restarting a locator.  See bug
   * 32856.
   *
   * @since 4.1
   */
  public void testRestartLocator() throws Exception {
    disconnectAllFromDS();
    port1 =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    deleteLocatorStateFile(port1);
    File logFile = new File("");
    File stateFile = new File("locator"+port1+"state.dat");
    VM vm0 = Host.getHost(0).getVM(0);
    final Properties p = new Properties();
    p.setProperty(DistributionConfig.LOCATORS_NAME, Host.getHost(0).getHostName() + "["+port1+"]");
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    if (stateFile.exists()) {
      stateFile.delete();
    }

    getLogWriter().info("Starting locator");
    Locator locator = Locator.startLocatorAndDS(port1, logFile, p);
    try {
    
    SerializableRunnable connect =
      new SerializableRunnable("Connect to locator on port " + port1) {
          public void run() {
            DistributedSystem.connect(p);
          }
        };
    vm0.invoke(connect);
    
    getLogWriter().info("Stopping locator");
    locator.stop();
    
    getLogWriter().info("Starting locator");
    locator = Locator.startLocatorAndDS(port1, logFile, p);
    
    vm0.invoke(new SerializableRunnable("disconnect") {
      public void run() {
        DistributedSystem.connect(p).disconnect();
      }
    });
    
    } finally {
    locator.stop();
    }

  }
  
  /** return the distributed member id for the ds on this vm */
  public static DistributedMember getDistributedMember(Properties props) {
    props.put("name", "vm_"+VM.getCurrentVMNum());
    DistributedSystem sys = DistributedSystem.connect(props);
    sys.getLogWriter().info("<ExpectedException action=add>service failure</ExpectedException>");
    sys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ConnectException</ExpectedException>");
    sys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ForcedDisconnectException</ExpectedException>");
    return DistributedSystem.connect(props).getDistributedMember();
  }
  
  /** find a running locator and return its distributed member id */
  public static DistributedMember getLocatorDistributedMember() {
    return (Locator.getLocators().iterator().next())
      .getDistributedSystem().getDistributedMember();
  }
  
  /** find the lead member and return its id */
  public static DistributedMember getLeadMember() {
    DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    return MembershipManagerHelper.getLeadMember(sys);
  }

  private SerializableRunnable getStopLocatorRunnable() {
    return new SerializableRunnable("stop locator") {
      public void run() {
        MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
        Locator loc = Locator.getLocator();
        if (loc != null) {
          loc.stop();
          assertFalse(Locator.hasLocator());
        }
      }
    };
  }
  
  private SerializableRunnable getStartSBLocatorRunnable(final int port, final String name) {
    return new SerializableRunnable("Start locator on port " + port) {
      public void run() {
        File logFile = new File("");
        try {
          System.setProperty(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS, "true");
          System.setProperty("p2p.joinTimeout", "1000");
          Properties locProps = new Properties();
          locProps.put("mcast-port", "0");
          locProps.put("log-level", getDUnitLogLevel());
          Locator.startLocatorAndDS(port, logFile, locProps);
        } catch (IOException ex) {
          fail("While starting locator on port " + port, ex);
        }
        finally {
          System.getProperties().remove(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS);
          System.getProperties().remove("p2p.joinTimeout");
        }
      }
    };
  }
  
  protected void nukeJChannel(DistributedSystem sys) {
    sys.getLogWriter().info("<ExpectedException action=add>service failure</ExpectedException>");
    sys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ConnectException</ExpectedException>");
    sys.getLogWriter().info("<ExpectedException action=add>com.gemstone.gemfire.ForcedDisconnectException</ExpectedException>");
    try {
      MembershipManagerHelper.crashDistributedSystem(sys);
    }
    catch (DistributedSystemDisconnectedException se) {
      // it's okay for the system to already be shut down
    }
    sys.getLogWriter().info("<ExpectedException action=remove>service failure</ExpectedException>");
    sys.getLogWriter().info("<ExpectedException action=remove>com.gemstone.gemfire.ForcedDisconnectException</ExpectedException>");
  }

  
  //New test hook which blocks before closing channel.
  class TestHook implements MembershipTestHook {

    volatile boolean unboundedWait = true;
    @Override
    public void beforeMembershipFailure(String reason, Throwable cause) {
      System.out.println("Inside TestHook.beforeMembershipFailure with " + cause);
      long giveUp = System.currentTimeMillis() + 30000;
      if (cause instanceof ForcedDisconnectException) {
        while (unboundedWait && System.currentTimeMillis() < giveUp) {
          pause(1000);
        }
      } else {
        cause.printStackTrace();
      }
    }

    @Override
    public void afterMembershipFailure(String reason, Throwable cause) {
    }

    public void reset() {
      unboundedWait = false;
    }

  }
  class MyMembershipListener implements MembershipListener {
    boolean quorumLostInvoked;
    List<String> suspectReasons = new ArrayList<>(50);
    
    public void memberJoined(InternalDistributedMember id) {  }
    public void memberDeparted(InternalDistributedMember id, boolean crashed) { }
    public void memberSuspect(InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {
      suspectReasons.add(reason);
    }
    public void quorumLost(Set<InternalDistributedMember> failures,
        List<InternalDistributedMember> remaining) {
      quorumLostInvoked = true;
      getLogWriter().info("quorumLost invoked in test code");
    }
  }
  
  
}
