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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminDistributedSystemFactory;
import org.apache.geode.admin.Alert;
import org.apache.geode.admin.AlertLevel;
import org.apache.geode.admin.AlertListener;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This class tests the functionality of the {@link
 * DistributionManager} class.
 */
@Category(DistributedTest.class)
public class DistributionManagerDUnitTest extends JUnit4DistributedTestCase {
  private static final Logger logger = LogService.getLogger();

  public static DistributedSystem ds;

  /**
   * Clears the exceptionInThread flag in the given distribution
   * manager. 
   */
  public static void clearExceptionInThreads(DistributionManager dm) {
    dm.clearExceptionInThreads();
  }

  @Override
  public void preSetUp() throws Exception {
    disconnectAllFromDS();
  }

  protected static class ItsOkayForMyClassNotToBeFound
    extends SerialDistributionMessage {

    public int getDSFID() {
      return NO_FIXED_ID;
    }
    
    @Override
    protected void process(DistributionManager dm) {
      // We should never get here
    }
  };
  
  @Test
  public void testGetDistributionVMType() {
    DM dm = getSystem().getDistributionManager();
    InternalDistributedMember ipaddr = dm.getId();
    assertEquals(DistributionManager.NORMAL_DM_TYPE, ipaddr.getVmKind());
  }
  
  /**
   * Send the distribution manager a message it can't deserialize
   */
  @Ignore("TODO: use Awaitility and reenable assertions")
  @Test
  public void testExceptionInThreads() throws InterruptedException {
    DM dm = getSystem().getDistributionManager();
    String p1 = "ItsOkayForMyClassNotToBeFound";
    logger.info("<ExpectedException action=add>" + p1 + "</ExpectedException>");
    DistributionMessage m = new ItsOkayForMyClassNotToBeFound();
    dm.putOutgoing(m);
    Thread.sleep(1 * 1000);
    logger.info("<ExpectedException action=remove>" + p1 + "</ExpectedException>");
//    assertTrue(dm.exceptionInThreads());
//    dm.clearExceptionInThreads();
//    assertTrue(!dm.exceptionInThreads());
  }

  @Ignore("TODO: this passes when enabled")
  @Test
  public void testGetDistributionManagerIds() {
    int systemCount = 0;
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      systemCount += host.getSystemCount();
    }

    DM dm = getSystem().getDistributionManager();
    systemCount += 1;

    assertEquals(systemCount, dm.getNormalDistributionManagerIds().size());
  }

  /**
   * Demonstrate that a new UDP port is used when an attempt is made to
   * reconnect using a shunned port
   */
  @Test
  public void testConnectAfterBeingShunned() {
    InternalDistributedSystem sys = getSystem();
    MembershipManager mgr = MembershipManagerHelper.getMembershipManager(sys);
    InternalDistributedMember idm = mgr.getLocalMember();
    // TODO GMS needs to have a system property allowing the bind-port to be set
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "jg-bind-port", "" + idm.getPort());
    try {
      sys.disconnect();
      sys = getSystem();
      mgr = MembershipManagerHelper.getMembershipManager(sys);
      sys.disconnect();
      InternalDistributedMember idm2 = mgr.getLocalMember();
      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("original ID=" + idm +
          " and after connecting=" + idm2);
      assertTrue("should not have used a different udp port",
          idm.getPort() == idm2.getPort());
    } finally {
      System.getProperties().remove(DistributionConfig.GEMFIRE_PREFIX + "jg-bind-port");
    }
  }

  /**
   * Test the handling of "surprise members" in the membership manager.
   * Create a DistributedSystem in this VM and then add a fake member to
   * its surpriseMember set.  Then ensure that it stays in the set when
   * a new membership view arrives that doesn't contain it.  Then wait
   * until the member should be gone and force more view processing to have
   * it scrubbed from the set.
   **/ 
  @Test
  public void testSurpriseMemberHandling() {
    VM vm0 = Host.getHost(0).getVM(0);

    InternalDistributedSystem sys = getSystem();
    MembershipManager mgr = MembershipManagerHelper.getMembershipManager(sys);

    try {
      InternalDistributedMember mbr = new InternalDistributedMember(
        NetworkUtils.getIPLiteral(), 12345);

      // first make sure we can't add this as a surprise member (bug #44566)
      
      // if the view number isn't being recorded correctly the test will pass but the
      // functionality is broken
      Assert.assertTrue("expected view ID to be greater than zero", mgr.getView().getViewId() > 0);

      int oldViewId = mbr.getVmViewId();
      mbr.setVmViewId((int)mgr.getView().getViewId()-1);
      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("current membership view is " + mgr.getView());
      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("created ID " + mbr + " with view ID " + mbr.getVmViewId());
      sys.getLogWriter().info("<ExpectedException action=add>attempt to add old member</ExpectedException>");
      sys.getLogWriter().info("<ExpectedException action=add>Removing shunned GemFire node</ExpectedException>");
      try {
        boolean accepted = mgr.addSurpriseMember(mbr);
        Assert.assertTrue("member with old ID was not rejected (bug #44566)", !accepted);
      } finally {
        sys.getLogWriter().info("<ExpectedException action=remove>attempt to add old member</ExpectedException>");
        sys.getLogWriter().info("<ExpectedException action=remove>Removing shunned GemFire node</ExpectedException>");
      }
      mbr.setVmViewId(oldViewId);

      // now forcibly add it as a surprise member and show that it is reaped
      long gracePeriod = 5000;
      long startTime = System.currentTimeMillis();
      long timeout = ((GMSMembershipManager)mgr).getSurpriseMemberTimeout();
      long birthTime = startTime - timeout + gracePeriod;
      MembershipManagerHelper.addSurpriseMember(sys, mbr, birthTime);
      assertTrue("Member was not a surprise member", mgr.isSurpriseMember(mbr));
      
      // force a real view change
      SerializableRunnable connectDisconnect = new SerializableRunnable() {
        public void run() {
          getSystem().disconnect();
        }
      };
      vm0.invoke(connectDisconnect);
      
      if (birthTime < (System.currentTimeMillis() - timeout)) {
        return; // machine is too busy and we didn't get enough CPU to perform more assertions
      }
      assertTrue("Member was incorrectly removed from surprise member set",
          mgr.isSurpriseMember(mbr));
      
      try {
        Thread.sleep(gracePeriod);
      }
      catch (InterruptedException e) {
        fail("test was interrupted", e);
      }
      
      vm0.invoke(connectDisconnect);
      assertTrue("Member was not removed from surprise member set",
          !mgr.isSurpriseMember(mbr));
      
    }
    catch (UnknownHostException e) {
      fail("unable to resolve localhost - test needs some attention", e);
    }
    finally {
      if (sys != null && sys.isConnected()) {
        sys.disconnect();
      }
    }
  }

  /**
   * vm1 stores its cache in this static variable in
   * testAckSeverAllertThreshold
   */
  static Cache myCache;
  
  /**
   * Tests that a severe-level alert is generated if a member does not respond
   * with an ack quickly enough.  vm0 and vm1 create a region and set
   * ack-severe-alert-threshold.  vm1 has a cache listener in its
   * region that sleeps when notified, forcing the operation to take longer
   * than ack-wait-threshold + ack-severe-alert-threshold
   */
  @Test
  public void testAckSevereAlertThreshold() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
//    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    // in order to set a small ack-wait-threshold, we have to remove the
    // system property established by the dunit harness
    String oldAckWait = (String)System.getProperties()
        .remove(DistributionConfig.GEMFIRE_PREFIX + ACK_WAIT_THRESHOLD);

    try {
      final Properties props = getDistributedSystemProperties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(ACK_WAIT_THRESHOLD, "3");
      props.setProperty(ACK_SEVERE_ALERT_THRESHOLD, "3");
      props.setProperty(NAME, "putter");
  
      getSystem(props);
      Region rgn = (new RegionFactory())
                        .setScope(Scope.DISTRIBUTED_ACK)
                        .setEarlyAck(false)
                        .setDataPolicy(DataPolicy.REPLICATE)
                        .create("testRegion");

      vm1.invoke(new SerializableRunnable("Connect to distributed system") {
        public void run() {
          props.setProperty(NAME, "sleeper");
          getSystem(props);
          IgnoredException.addIgnoredException("elapsed while waiting for replies");
          RegionFactory rf = new RegionFactory();
          Region r = rf.setScope(Scope.DISTRIBUTED_ACK)
            .setDataPolicy(DataPolicy.REPLICATE)
            .setEarlyAck(false)
            .addCacheListener(getSleepingListener(false))
            .create("testRegion");
          myCache = r.getCache();
          try {
            createAlertListener();
          }
          catch (Exception e) {
            throw new RuntimeException("failed to create alert listener", e);
          }
        }
      });
      
      // now we have two caches set up.  vm1 has a listener that will sleep
      // and cause the severe-alert threshold to be crossed
      
      rgn.put("bomb", "pow!"); // this will hang until vm1 responds

      rgn.getCache().close();
      basicGetSystem().disconnect();

      vm1.invoke(new SerializableRunnable("disconnect from ds") {
        public void run() {
          if (!myCache.isClosed()) {
            if (basicGetSystem().isConnected()) {
              basicGetSystem().disconnect();
            }
            myCache = null;
          }
          if (basicGetSystem().isConnected()) {
            basicGetSystem().disconnect();
          }
          synchronized(alertGuard) {
            assertTrue(alertReceived);
          }
        }
      });

    }
    finally {
      if (oldAckWait != null) {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + ACK_WAIT_THRESHOLD, oldAckWait);
      }
    }
  }
  
  static volatile boolean regionDestroyedInvoked;

  static CacheListener getSleepingListener(final boolean playDead) {
    regionDestroyedInvoked = false;
    
    return new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        try {
          if (playDead) {
            MembershipManagerHelper.beSickMember(getSystemStatic());
            MembershipManagerHelper.playDead(getSystemStatic());
          }
          Thread.sleep(15000);
        }
        catch (InterruptedException ie) {
          fail("interrupted", ie);
        }
      }
      @Override
      public void afterRegionDestroy(RegionEvent event) {
        LogWriter logger = myCache.getLogger();
        logger.info("afterRegionDestroyed invoked in sleeping listener");
        logger.info("<ExpectedException action=remove>service failure</ExpectedException>");
        logger.info("<ExpectedException action=remove>org.apache.geode.ForcedDisconnectException</ExpectedException>");
        regionDestroyedInvoked = true;
        }
    };
  }
  
  static AdminDistributedSystem adminSystem;
  static Object alertGuard = new Object();
  static boolean alertReceived;
  
  static void createAlertListener() throws Exception {
    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(getSystemStatic(), null);
    adminSystem =
      AdminDistributedSystemFactory.getDistributedSystem(config);
    adminSystem.setAlertLevel(AlertLevel.SEVERE);
    adminSystem.addAlertListener(new AlertListener() {
      public void alert(Alert alert) {
        try {          
          logger.info("alert listener invoked for alert originating in " + alert.getConnectionName());
          logger.info("  alert text = " + alert.getMessage());
          logger.info("  systemMember = " + alert.getSystemMember());
        }
        catch (Exception e) {
          logger.fatal("exception trying to use alert object", e);
        }
        synchronized(alertGuard) {
          alertReceived = true;
        }
      }
    });
    adminSystem.connect();
    assertTrue(adminSystem.waitToBeConnected(5 * 1000));
  }

  /**
   * Tests that a sick member is kicked out
   */
  @Test
  public void testKickOutSickMember() throws Exception {
    disconnectAllFromDS();
    IgnoredException.addIgnoredException("10 seconds have elapsed while waiting");
    Host host = Host.getHost(0);
//    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    // in order to set a small ack-wait-threshold, we have to remove the
    // system property established by the dunit harness
    String oldAckWait = (String)System.getProperties()
        .remove(DistributionConfig.GEMFIRE_PREFIX + ACK_WAIT_THRESHOLD);

    try {
      final Properties props = getDistributedSystemProperties();
      props.setProperty(MCAST_PORT, "0"); // loner
      props.setProperty(ACK_WAIT_THRESHOLD, "5");
      props.setProperty(ACK_SEVERE_ALERT_THRESHOLD, "5");
      props.setProperty(NAME, "putter");
  
      getSystem(props);
      Region rgn = (new RegionFactory())
                        .setScope(Scope.DISTRIBUTED_ACK)
                        .setDataPolicy(DataPolicy.REPLICATE)
                        .create("testRegion");
      basicGetSystem().getLogWriter().info("<ExpectedException action=add>sec have elapsed while waiting for replies</ExpectedException>");
      
      vm1.invoke(new SerializableRunnable("Connect to distributed system") {
        public void run() {
          props.setProperty(NAME, "sleeper");
          getSystem(props);
          LogWriter log = basicGetSystem().getLogWriter();
          log.info("<ExpectedException action=add>service failure</ExpectedException>");
          log.info("<ExpectedException action=add>org.apache.geode.ForcedDisconnectException</ExpectedException>");
          RegionFactory rf = new RegionFactory();
          Region r = rf.setScope(Scope.DISTRIBUTED_ACK)
            .setDataPolicy(DataPolicy.REPLICATE)
            .addCacheListener(getSleepingListener(true))
            .create("testRegion");
          myCache = r.getCache();
        }
      });
      
      // now we have two caches set up, each having an alert listener.  Vm1
      // also has a cache listener that will turn off its ability to respond
      // to "are you dead" messages and then sleep
      
      rgn.put("bomb", "pow!");
      
      
      rgn.getCache().close();
      basicGetSystem().getLogWriter().info("<ExpectedException action=remove>sec have elapsed while waiting for replies</ExpectedException>");
      basicGetSystem().disconnect();
      
      vm1.invoke(new SerializableRunnable("wait for forced disconnect") {
        public void run() {
          // wait a while for the DS to finish disconnecting
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return !basicGetSystem().isConnected();
            }
            public String description() {
              return null;
            }
          };
          // if this fails it means the sick member wasn't kicked out and something is wrong
          Wait.waitForCriterion(ev, 60 * 1000, 200, true);
          
          ev = new WaitCriterion() {
            public boolean done() {
              return myCache.isClosed();
            }
            public String description() {
              return null;
            }
          };
          Wait.waitForCriterion(ev, 20 * 1000, 200, false);
          
          if (!myCache.isClosed()) {
            if (basicGetSystem().isConnected()) {
              basicGetSystem().disconnect();
            }
            myCache = null;
            throw new RuntimeException("Test Failed - vm1's cache is not closed");
          }
          if (basicGetSystem().isConnected()) {
             basicGetSystem().disconnect();
             throw new RuntimeException("Test Failed - vm1's system should have been disconnected");
          }
          
          WaitCriterion wc = new WaitCriterion() {
            public boolean done() {
              return regionDestroyedInvoked;
            }
            public String description() {
              return "vm1's listener should have received afterRegionDestroyed notification";
            }
          };
          Wait.waitForCriterion(wc, 30 * 1000, 1000, true);
          
        }
      });

    }
    finally {
      if (oldAckWait != null) {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + ACK_WAIT_THRESHOLD, oldAckWait);
      }
    }
  }

  /**
   * test use of a bad bind-address for bug #32565
   */
  @Test
  public void testBadBindAddress() throws Exception {
    disconnectAllFromDS();

    final Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0"); // loner
    // use a valid address that's not proper for this machine
    props.setProperty(BIND_ADDRESS, "www.yahoo.com");
    props.setProperty(ACK_WAIT_THRESHOLD, "5");
    props.setProperty(ACK_SEVERE_ALERT_THRESHOLD, "5");
    try {
      getSystem(props);
    } catch (IllegalArgumentException e) {
      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("caught expected exception (1)", e);
    }
    // use an invalid address
    props.setProperty(BIND_ADDRESS, "bruce.schuchardt");
    try {
      getSystem(props);
    } catch (IllegalArgumentException e) {
      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("caught expected exception (2_", e);
    }
    // use a valid bind address
    props.setProperty(BIND_ADDRESS, InetAddress.getLocalHost().getCanonicalHostName());
    getSystem().disconnect();
  }
  
  /**
   * install a new view and show that waitForViewInstallation works as expected
   */
  @Test
  public void testWaitForViewInstallation() {
    getSystem(new Properties());
    
    MembershipManager mgr = basicGetSystem().getDM().getMembershipManager();

    final NetView v = mgr.getView();
    
    final boolean[] passed = new boolean[1];
    Thread t = new Thread("wait for view installation") {
      public void run() {
        try {
          ((DistributionManager)basicGetSystem().getDM()).waitForViewInstallation(v.getViewId()+1);
          synchronized(passed) {
            passed[0] = true;
          }
        } catch (InterruptedException e) {
          // failed
        }
      }
    };
    t.setDaemon(true);
    t.start();
    
    Wait.pause(2000);

    NetView newView = new NetView(v, v.getViewId()+1);
    ((Manager)mgr).installView(newView);

    Wait.pause(2000);
    
    synchronized(passed) {
      Assert.assertTrue(passed[0]);
    }
  }
}
