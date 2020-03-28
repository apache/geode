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
package org.apache.geode.cache30;

import static java.lang.System.out;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.DataPolicy.REPLICATE;
import static org.apache.geode.cache.LossAction.RECONNECT;
import static org.apache.geode.cache.ResumptionAction.NONE;
import static org.apache.geode.cache.Scope.DISTRIBUTED_ACK;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_NUM_RECONNECT_TRIES;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.ROLES;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.Locator.getLocator;
import static org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper.getDistribution;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.ThreadUtils.join;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.ResumptionAction;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem.ReconnectListener;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;

@SuppressWarnings("serial")
@Category({MembershipTest.class})
public class ReconnectDUnitTest extends JUnit4CacheTestCase {
  static int locatorPort;
  static Locator locator;
  static DistributedSystem savedSystem;
  static GemFireCacheImpl savedCache;
  static int locatorVMNumber = 3;
  static Thread gfshThread;

  static Properties dsProperties;
  static String fileSeparator = File.separator;

  public ReconnectDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final int locPort = locatorPort;
    Host.getHost(0).getVM(locatorVMNumber).invoke(new SerializableRunnable("start locator") {
      @Override
      public void run() {
        try {
          disconnectFromDS();
          dsProperties = null;
          locatorPort = locPort;
          Properties props = getDistributedSystemProperties();
          locator = Locator.startLocatorAndDS(locatorPort, new File(""), props);
          system = (InternalDistributedSystem) locator.getDistributedSystem();
          cache = ((InternalLocator) locator).getCache();
          ReconnectDUnitTest.savedSystem = locator.getDistributedSystem();
          IgnoredException.addIgnoredException(
              "org.apache.geode.ForcedDisconnectException||Possible loss of quorum");
          // MembershipManagerHelper.getMembershipManager(InternalDistributedSystem.getConnectedInstance()).setDebugJGroups(true);
        } catch (IOException e) {
          Assert.fail("unable to start locator", e);
        }
      }
    });

    SerializableRunnable setDistributedSystemProperties =
        new SerializableRunnable("set distributed system properties") {
          @Override
          public void run() {
            dsProperties = null;
            locatorPort = locPort;
            getDistributedSystemProperties();
          }
        };
    setDistributedSystemProperties.run();
    Invoke.invokeInEveryVM(setDistributedSystemProperties);

    beginCacheXml();
    createRegion("myRegion", createAtts());
    finishCacheXml("MyDisconnect");
    // Cache cache = getCache();
    closeCache();
    basicGetSystem().disconnect();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    if (dsProperties == null) {
      dsProperties = new Properties();
      dsProperties.put(MAX_WAIT_TIME_RECONNECT, "20000");
      dsProperties.put(ENABLE_NETWORK_PARTITION_DETECTION, "true");
      dsProperties.put(DISABLE_AUTO_RECONNECT, "false");
      dsProperties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
      dsProperties.put(LOCATORS, "localHost[" + locatorPort + "]");
      dsProperties.put(MCAST_PORT, "0");
      dsProperties.put(MEMBER_TIMEOUT, "2000");
      dsProperties.put(LOG_LEVEL, "info");
      dsProperties.put(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
      dsProperties.put("security-username", "clusterManage");
      dsProperties.put("security-password", "clusterManage");
      addDSProps(dsProperties);
    }
    return dsProperties;
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    System.out.println("entering postTearDownCacheTestCase");
    SerializableRunnable disconnect = new SerializableRunnable("disconnect and clean up") {
      @Override
      public void run() {
        if (savedSystem != null && savedSystem.isReconnecting()) {
          savedSystem.stopReconnecting();
        }
        savedSystem = null;
        savedCache = null;
        dsProperties = null;
        locator = null;
        locatorPort = 0;
      }
    };
    Invoke.invokeInEveryVM(disconnect);
    disconnect.run();
    disconnectAllFromDS();
  }

  /**
   * Creates some region attributes for the regions being created.
   */
  private RegionAttributes createAtts() {
    AttributesFactory factory = new AttributesFactory();

    {
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setScope(Scope.DISTRIBUTED_ACK);
    }

    return factory.create();
  }


  @Test
  public void testReconnectWithQuorum() throws Exception {
    // quorum check fails, then succeeds
    IgnoredException.addIgnoredException("killing member's ds");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM locatorVm = host.getVM(locatorVMNumber);

    final int locPort = locatorPort;

    final String xmlFileLoc = (new File(".")).getAbsolutePath();

    // disable disconnects in the locator so we have some stability
    locatorVm.invoke(new SerializableRunnable("disable force-disconnect") {
      @Override
      public void run() {
        MembershipManagerHelper
            .disableDisconnectOnQuorumLossForTesting(Locator.getLocator().getDistributedSystem());
      }
    });

    SerializableCallable create =
        new SerializableCallable("Create Cache and Regions from cache.xml") {
          @Override
          public Object call() throws CacheException {
            locatorPort = locPort;
            Properties props = getDistributedSystemProperties();
            props.put(CACHE_XML_FILE, xmlFileLoc + fileSeparator + "MyDisconnect-cache.xml");
            props.put(MAX_NUM_RECONNECT_TRIES, "2");
            // props.put("log-file", "autoReconnectVM"+VM.getCurrentVMNum()+"_"+getPID()+".log");
            cache = (InternalCache) new CacheFactory(props).create();
            IgnoredException.addIgnoredException(
                "org.apache.geode.ForcedDisconnectException||Possible loss of quorum");
            Region myRegion = cache.getRegion("root/myRegion");
            ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
            myRegion.put("MyKey1", "MyValue1");
            return savedSystem.getDistributedMember();
          }
        };

    System.out.println("creating caches in vm0 and vm1");
    vm0.invoke(create);
    vm1.invoke(create);

    // view is [locator(3), vm0(15), vm1(10), vm2(10)]

    /*
     * now we want to kick out the locator and observe that it reconnects using its rebooted
     * location service
     */
    System.out.println("disconnecting locator");
    forceDisconnect(locatorVm);
    waitForReconnect(locatorVm);

    // if the locator reconnected it did so with its own location
    // service since it doesn't know about any other locators
    ensureLocationServiceRunning(locatorVm);

  }

  @Test
  public void testReconnectOnForcedDisconnect() throws Exception {
    doTestReconnectOnForcedDisconnect(false);
  }

  /** bug #51335 - customer is also trying to recreate the cache */
  // this test is disabled due to a high failure rate during CI test runs.
  // see bug #52160
  @Test
  public void testReconnectCollidesWithApplication() throws Exception {
    doTestReconnectOnForcedDisconnect(true);
  }

  public void doTestReconnectOnForcedDisconnect(final boolean createInAppToo) throws Exception {

    IgnoredException.addIgnoredException("killing member's ds");

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final int locPort = locatorPort;
    final int secondLocPort = AvailablePortHelper.getRandomAvailableTCPPort();

    Invoke
        .invokeInEveryVM(() -> DistributedTestUtils.deleteLocatorStateFile(locPort, secondLocPort));


    final String xmlFileLoc = (new File(".")).getAbsolutePath();

    SerializableCallable create1 =
        new SerializableCallable("Create Cache and Regions from cache.xml") {
          @Override
          public Object call() throws CacheException {
            locatorPort = locPort;
            Properties props = getDistributedSystemProperties();
            props.put(CACHE_XML_FILE, xmlFileLoc + fileSeparator + "MyDisconnect-cache.xml");
            props.put(MAX_WAIT_TIME_RECONNECT, "1000");
            cache = (InternalCache) new CacheFactory(props).create();
            Region myRegion = cache.getRegion("root/myRegion");
            ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
            myRegion.put("MyKey1", "MyValue1");
            return savedSystem.getDistributedMember();
          }
        };

    SerializableCallable create2 =
        new SerializableCallable("Create Cache and Regions from cache.xml") {
          @Override
          public Object call() throws CacheException {
            locatorPort = locPort;
            final Properties props = getDistributedSystemProperties();
            props.put(CACHE_XML_FILE, xmlFileLoc + fileSeparator + "MyDisconnect-cache.xml");
            props.put(MAX_WAIT_TIME_RECONNECT, "5000");
            props.put(START_LOCATOR, "localhost[" + secondLocPort + "]");
            props.put(LOCATORS, props.get(LOCATORS) + ",localhost[" + secondLocPort + "]");
            getSystem(props);
            cache = getCache();
            ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
            Region myRegion = cache.getRegion("root/myRegion");
            myRegion.put("Mykey2", "MyValue2");
            assertNotNull(myRegion.get("MyKey1"));
            if (createInAppToo) {
              Thread recreateCacheThread = new Thread("ReconnectDUnitTest.createInAppThread") {
                @Override
                public void run() {
                  while (!cache.isClosed()) {
                    Wait.pause(100);
                  }
                  try {
                    cache = (InternalCache) new CacheFactory(props).create();
                    System.err.println(
                        "testReconnectCollidesWithApplication failed - application thread was able to create a cache");
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
    final InternalDistributedMember dm = (InternalDistributedMember) vm1.invoke(create2);

    IgnoredException.addIgnoredException("ForcedDisconnectException");
    forceDisconnect(vm1);

    DistributedMember newdm =
        (DistributedMember) vm1.invoke(new SerializableCallable("wait for reconnect(1)") {
          @Override
          public Object call() {
            final DistributedSystem ds = savedSystem;
            savedSystem = null;
            GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
              @Override
              public boolean done() {
                return ds.isReconnecting();
              }

              @Override
              public String description() {
                return "waiting for ds to begin reconnecting";
              }
            });
            System.out.println("entering reconnect wait for " + ds);
            System.out.println("ds.isReconnecting() = " + ds.isReconnecting());
            boolean failure = true;
            try {
              ds.waitUntilReconnected(getTimeout().toMillis(), MILLISECONDS);
              savedSystem = ds.getReconnectedSystem();
              locator = (InternalLocator) getLocator();
              assertTrue("Expected system to be restarted", ds.getReconnectedSystem() != null);
              assertTrue("Expected system to be running", ds.getReconnectedSystem().isConnected());
              assertTrue("Expected there to be a locator", locator != null);
              assertTrue("Expected locator to be restarted",
                  !((InternalLocator) locator).isStopped());
              failure = false;
              cache = ((InternalLocator) locator).getCache();
              system = cache.getInternalDistributedSystem();
              return ds.getReconnectedSystem().getDistributedMember();
            } catch (InterruptedException e) {
              System.err.println("interrupted while waiting for reconnect");
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
    boolean stopped = (Boolean) vm1.invoke(new SerializableCallable("wait for reconnect and stop") {
      @Override
      public Object call() {
        final DistributedSystem ds = savedSystem;
        savedSystem = null;
        GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
          @Override
          public boolean done() {
            return ds.isReconnecting() || ds.getReconnectedSystem() != null;
          }

          @Override
          public String description() {
            return "waiting for reconnect to commence in " + ds;
          }

        });
        ds.stopReconnecting();
        assertFalse(ds.isReconnecting());
        DistributedSystem newDs = ds.getReconnectedSystem();
        if (newDs != null) {
          System.err.println("expected distributed system to be disconnected: " + newDs);
          newDs.disconnect();
          return false;
        }
        return true;
      }
    });
    assertTrue("expected DistributedSystem to disconnect", stopped);

    // recreate the system in vm1 without a locator and crash it
    DistributedMember evenNewerdm = (DistributedMember) vm1.invoke(create1);
    forceDisconnect(vm1);
    newdm = waitForReconnect(vm1);
    assertNotSame("expected a reconnect to occur in member", evenNewerdm, newdm);
    Invoke
        .invokeInEveryVM(() -> DistributedTestUtils.deleteLocatorStateFile(locPort, secondLocPort));
  }

  private DistributedMember getDMID(VM vm) {
    return (DistributedMember) vm.invoke(new SerializableCallable("get ID") {
      @Override
      public Object call() {
        ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
        return ReconnectDUnitTest.savedSystem.getDistributedMember();
      }
    });
  }

  /** this will throw an exception if location services aren't running */
  private void ensureLocationServiceRunning(VM vm) {
    vm.invoke("ensureLocationServiceRunning", () -> {
      await().untilAsserted(() -> {
        InternalLocator intloc = (InternalLocator) locator;
        ServerLocator serverLocator = intloc.getServerLocatorAdvisee();
        // the initialization flag in the locator's ControllerAdvisor will
        // be set if a handshake has been performed
        assertTrue(serverLocator.getDistributionAdvisor().isInitialized());
      });
    });
  }

  private DistributedMember waitForReconnect(VM vm) {
    return (DistributedMember) vm
        .invoke(new SerializableCallable("wait for Reconnect and return ID") {
          @Override
          public Object call() {
            out.println("waitForReconnect invoked");
            final DistributedSystem ds = savedSystem;
            savedSystem = null;
            GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
              @Override
              public boolean done() {
                return ds.isReconnecting();
              }

              @Override
              public String description() {
                return "waiting for ds to begin reconnecting";
              }
            });
            long waitTime = 600;
            System.out.println("VM" + VM.getCurrentVMNum() + " waiting up to "
                + waitTime + " seconds for reconnect to complete");
            try {
              ds.waitUntilReconnected(waitTime, SECONDS);
            } catch (InterruptedException e) {
              fail("interrupted while waiting for reconnect");
            }
            assertTrue("expected system to be reconnected", ds.getReconnectedSystem() != null);
            int oldViewId =
                getDistribution(ds).getLocalMember().getVmViewId();
            int newViewId =
                ((InternalDistributedMember) ds.getReconnectedSystem().getDistributedMember())
                    .getVmViewId();
            if (!(newViewId > oldViewId)) {
              fail("expected a new ID to be assigned.  oldViewId=" + oldViewId + "; newViewId="
                  + newViewId);
            }
            return ds.getReconnectedSystem().getDistributedMember();
          }
        });
  }

  @Test
  public void testReconnectALocator() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM locatorVm = host.getVM(3);
    DistributedMember dm, newdm;

    final int locPort = locatorPort;
    final int secondLocPort = AvailablePortHelper.getRandomAvailableTCPPort();

    Invoke
        .invokeInEveryVM(() -> DistributedTestUtils.deleteLocatorStateFile(locPort, secondLocPort));

    final String xmlFileLoc = (new File(".")).getAbsolutePath();

    // This locator was started in setUp.
    File locatorViewLog =
        new File(locatorVm.getWorkingDirectory(), "locator" + locatorPort + "views.log");
    assertTrue("Expected to find " + locatorViewLog.getPath() + " file", locatorViewLog.exists());
    long logSize = locatorViewLog.length();

    vm0.invoke("Create a second locator", () -> {
      locatorPort = locPort;
      Properties props = getDistributedSystemProperties();
      props.put(MAX_WAIT_TIME_RECONNECT, "1000");
      props.put(LOCATORS, props.get(LOCATORS) + ",localhost[" + locPort + "]");
      props.put(ENABLE_CLUSTER_CONFIGURATION, "false");
      try {
        InternalLocator locator =
            (InternalLocator) Locator.startLocatorAndDS(secondLocPort, null, props);
        system = (InternalDistributedSystem) locator.getDistributedSystem();
        cache = locator.getCache();
      } catch (IOException e) {
        Assert.fail("exception starting locator", e);
      }
    });

    File locator2ViewLog =
        new File(vm0.getWorkingDirectory(), "locator" + secondLocPort + "views.log");
    assertTrue("Expected to find " + locator2ViewLog.getPath() + " file", locator2ViewLog.exists());
    long log2Size = locator2ViewLog.length();

    // create a cache in vm1 so there is more weight in the system
    vm1.invoke("Create Cache and Regions from cache.xml", () -> {
      locatorPort = locPort;
      Properties props = getDistributedSystemProperties();
      props.put(CACHE_XML_FILE, xmlFileLoc + fileSeparator + "MyDisconnect-cache.xml");
      props.put(MAX_WAIT_TIME_RECONNECT, "1000");
      ReconnectDUnitTest.savedSystem = getSystem(props);
      cache = getCache();
      Region myRegion = cache.getRegion("root/myRegion");
      myRegion.put("MyKey1", "MyValue1");
      return savedSystem.getDistributedMember();
    });

    try {
      dm = getDMID(vm0);
      createGfshWaitingThread(vm0);
      forceDisconnect(vm0);
      newdm = waitForReconnect(vm0);
      assertGfshWaitingThreadAlive(vm0);

      assertTrue("Expected the restarted member to be hosting a running locator",
          vm0.invoke("check for running locator", () -> {
            await("waiting for locator to restart")
                .until(Locator::getLocator, notNullValue());
            if (((InternalLocator) Locator.getLocator()).isStopped()) {
              System.err.println("found a stopped locator");
              return false;
            }
            return true;
          }));

      assertNotSame("expected a reconnect to occur in the locator", dm, newdm);

      // the log should have been opened and appended with a new view
      assertTrue("expected " + locator2ViewLog.getPath() + " to grow in size",
          locator2ViewLog.length() > log2Size);
      // the other locator should have logged a new view
      assertTrue("expected " + locatorViewLog.getPath() + " to grow in size",
          locatorViewLog.length() > logSize);

    } finally {
      vm0.invoke(new SerializableRunnable("stop locator") {
        @Override
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
      Invoke.invokeInEveryVM(
          () -> DistributedTestUtils.deleteLocatorStateFile(locPort, secondLocPort));
    }
  }

  @SuppressWarnings("serial")
  private void createGfshWaitingThread(VM vm) {
    vm.invoke(new SerializableRunnable("create Gfsh-like waiting thread") {
      @Override
      public void run() {
        final Locator loc = Locator.getLocator();
        assertNotNull(loc);
        gfshThread = new Thread("ReconnectDUnitTest_Gfsh_thread") {
          @Override
          public void run() {
            try {
              ((InternalLocator) loc).waitToStop();
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
      @Override
      public void run() {
        assertTrue(gfshThread.isAlive());
      }
    });
  }

  /**
   * Test the reconnect behavior when the required roles are missing. Reconnect is triggered as a
   * Reliability policy. The test is to see if the reconnect is triggered for the configured number
   * of times
   */

  @Test
  public void testReconnectWithRoleLoss() throws TimeoutException, RegionExistsException {

    final String rr1 = "RoleA";
    final String rr2 = "RoleB";
    final String[] requiredRoles = {rr1, rr2};
    final int locPort = locatorPort;
    final String xmlFileLoc = (new File(".")).getAbsolutePath();


    beginCacheXml();

    locatorPort = locPort;
    Properties config = getDistributedSystemProperties();
    config.put(ROLES, "");
    config.put(LOG_LEVEL, "info");
    // config.put("log-file", "roleLossController.log");
    // creating the DS
    getSystem(config);

    MembershipAttributes ra =
        new MembershipAttributes(requiredRoles, LossAction.RECONNECT, ResumptionAction.NONE);

    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);

    RegionAttributes attr = fac.create();
    createRootRegion("MyRegion", attr);

    // writing the cachexml file.

    File file = new File("RoleReconnect-cache.xml");
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(getCache(), pw);
      pw.close();
    } catch (IOException ex) {
      Assert.fail("IOException during cache.xml generation to " + file, ex);
    }
    closeCache();
    basicGetSystem().disconnect();

    System.out.println("disconnected from the system...");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);

    // Recreating from the cachexml.

    SerializableRunnable roleLoss = new CacheSerializableRunnable("ROLERECONNECTTESTS") {
      @Override
      public void run2() throws RuntimeException {
        System.out.println("####### STARTING THE REAL TEST ##########");

        locatorPort = locPort;
        dsProperties = null;
        Properties props = getDistributedSystemProperties();
        props.put(CACHE_XML_FILE, xmlFileLoc + fileSeparator + "RoleReconnect-cache.xml");
        props.put(MAX_WAIT_TIME_RECONNECT, "200");
        final int timeReconnect = 3;
        props.put(MAX_NUM_RECONNECT_TRIES, "3");
        props.put(LOG_LEVEL, "info");
        // props.put("log-file", "roleLossVM0.log");

        getSystem(props);

        addReconnectListener();

        IgnoredException.addIgnoredException("CacheClosedException");
        try {
          getCache(props);
          throw new RuntimeException("The test should throw a CancelException ");
        } catch (CancelException ignor) { // can be caused by role loss during intialization.
          System.out.println("Got Expected CancelException ");
        }

        WaitCriterion ev = new WaitCriterion() {
          @Override
          public boolean done() {
            return reconnectTries >= timeReconnect;
          }

          @Override
          public String description() {
            return "Waiting for reconnect count " + timeReconnect + " currently " + reconnectTries;
          }
        };
        Wait.waitForCriterion(ev, 60 * 1000, 200, true);
        assertEquals(timeReconnect, reconnectTries);
      }

    };

    vm0.invoke(roleLoss);


  }



  public static volatile int reconnectTries;

  public static volatile boolean initialized = false;

  public static volatile boolean initialRolePlayerStarted = false;

  // public static boolean rPut;
  public static int reconnectTries() {
    return reconnectTries;
  }

  public static boolean isInitialized() {
    return initialized;
  }

  public static boolean isInitialRolePlayerStarted() {
    return initialRolePlayerStarted;
  }

  @Before
  public void initStatics() {
    Invoke.invokeInEveryVM(() -> {
      reconnectTries = 0;
      initialized = false;
      initialRolePlayerStarted = false;
    });
  }

  // See #50944 before enabling the test. This ticket has been closed with wontFix
  // for the 2014 8.0 release.
  @Test
  public void testReconnectWithRequiredRoleRegained() throws Throwable {

    final String rr1 = "RoleA";
    // final String rr2 = "RoleB";
    final String[] requiredRoles = {rr1};
    // final boolean receivedPut[] = new boolean[1];

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
    config.put(ROLES, "");
    config.put(LOG_LEVEL, "info");
    // creating the DS
    getSystem(config);

    MembershipAttributes ra =
        new MembershipAttributes(requiredRoles, RECONNECT, NONE);

    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(DISTRIBUTED_ACK);
    fac.setDataPolicy(REPLICATE);

    RegionAttributes attr = fac.create();
    createRootRegion(regionName, attr);

    // writing the cachexml file.

    File file = new File("RoleRegained.xml");
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(getCache(), pw);
      pw.close();
    } catch (IOException ex) {
      Assert.fail("IOException during cache.xml generation to " + file, ex);
    }
    closeCache();
    // disconnectFromDS();
    getSystem().disconnect();

    // ################################################################### //
    //
    Host host = getHost(0);
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

    CacheSerializableRunnable roleLoss =
        getRoleLossRunnable(vm1, locPort, regionName, myKey, myValue,
            "starting role loss vm.  When the role is lost it will start" + " trying to reconnect",
            file.getAbsolutePath());
    final AsyncInvocation roleLossAsync = vm0.invokeAsync(roleLoss);

    System.out.println("waiting for role loss vm to start reconnect attempts");

    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        if (!roleLossAsync.isAlive()) {
          return true;
        }
        int tries = vm0.invoke(() -> ReconnectDUnitTest.reconnectTries());
        return tries != 0;
      }

      @Override
      public String description() {
        return "waiting for event";
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);

    VM vm2 = host.getVM(2);
    if (roleLossAsync.isAlive()) {

      SerializableRunnable roleAPlayer = getRoleAPlayerRunnable(locPort, regionName, myKey, myValue,
          "starting roleAPlayer in a different vm."
              + "  After this reconnect should succeed in vm0");

      vm2.invoke(roleAPlayer);

      // long startTime = System.currentTimeMillis();
      /*
       * while (numReconnect[0].intValue() > 0){ if((System.currentTimeMillis()-startTime )> 120000)
       * fail("The test failed because the required role not satisfied" +
       * "and the number of reconnected tried is not set to zero for " + "more than 2 mins"); try{
       * Thread.sleep(15); }catch(Exception ee){ getLogWriter().severe("Exception : "+ee); } }
       */
      System.out.println("waiting for vm0 to finish reconnecting");
      ThreadUtils.join(roleLossAsync, 120 * 1000);
    }

    if (roleLossAsync.getException() != null) {
      throw roleLossAsync.getException();
    }

    join(avkVm1, 30 * 1000);
    if (avkVm1.getException() != null) {
      Assert.fail("Exception in Vm1", avkVm1.getException());
    }

  }

  private CacheSerializableRunnable getRoleLossRunnable(final VM otherVM, final int locPort,
      final String regionName, final String myKey, final Object myValue,
      final String startupMessage, final String xmlFilePath) {

    return new CacheSerializableRunnable("roleloss runnable") {
      @Override
      public void run2() {
        System.out.println(startupMessage);
        WaitCriterion ev = new WaitCriterion() {
          @Override
          public boolean done() {
            return otherVM.invoke(() -> ReconnectDUnitTest.isInitialRolePlayerStarted())
                .booleanValue();
          }

          @Override
          public String description() {
            return null;
          }
        };
        await().untilAsserted(ev);

        System.out.println(
            "Starting the test and creating the cache and regions etc ..." + System.getenv("PWD"));
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put(CACHE_XML_FILE, xmlFilePath);
        props.put(MAX_WAIT_TIME_RECONNECT, "3000");
        props.put(MAX_NUM_RECONNECT_TRIES, "8");
        props.put(LOG_LEVEL, "info");

        getSystem(props);
        IgnoredException.addIgnoredException("CacheClosedException");

        try {
          getCache();
        } catch (CancelException e) {
          // can happen if RoleA goes away during initialization
          System.out.println("cache threw CancelException while creating the cache");
        }

        initialized = true;

        addReconnectListener();

        await().until(() -> reconnectTries != 0);

        await().until(() -> {
          String excuse = "none";
          if (InternalDistributedSystem.getReconnectAttemptCounter() != 0) {
            System.out.println("reconnectAttemptCounter is "
                + InternalDistributedSystem.getReconnectAttemptCounter()
                + " waiting for it to be zero");
            return false;
          }
          Object key = null;
          Object value = null;
          Region.Entry keyValue = null;
          try {
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
              keyValue = (Region.Entry) it.next();
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
            System.out.println("All assertions passed");
            System.out.println("MyKey : " + key + " and myvalue : " + value);
            return true;
          } catch (CancelException ecc) {
            // ignor the exception because the cache can be closed/null some times
            // while in reconnect.
          } catch (RegionDestroyedException rex) {

          } finally {
            System.out.println("waiting for reconnect.  Current status is '" + excuse + "'");
          }
          return false;
        });

        if (cache != null) {
          cache.getDistributedSystem().disconnect();
        }
      }
    }; // roleloss runnable
  }

  /**
   * auto-reconnect was found to stop attempting to reconnect and rebuild the cache if another
   * forced-disconnect was triggered after reconnect but before cache creation was completed. This
   * test uses a region listener to crash the reconnecting distributed system during cache creation
   * and asserts that it then reconnects and rebuilds the cache.
   */
  @Test
  public void testReconnectFailsInCacheCreation() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final int locPort = locatorPort;

    SerializableRunnable createCache = new SerializableRunnable("Create Cache and Regions") {
      @Override
      public void run() {
        locatorPort = locPort;
        final Properties props = getDistributedSystemProperties();
        props.put(MAX_WAIT_TIME_RECONNECT, "1000");
        dsProperties = props;
        ReconnectDUnitTest.savedSystem = getSystem(props);
        ReconnectDUnitTest.savedCache = (GemFireCacheImpl) getCache();
        Region myRegion = createRegion("myRegion", createAtts());
        myRegion.put("MyKey", "MyValue");
        myRegion.getAttributesMutator()
            .addCacheListener(new CacheListenerTriggeringForcedDisconnect());
      }
    };

    vm0.invoke(createCache); // vm0 keeps the locator from losing quorum when vm1 crashes

    vm1.invoke(createCache);
    IgnoredException.addIgnoredException(
        "DistributedSystemDisconnectedException|ForcedDisconnectException", vm1);
    forceDisconnect(vm1);

    vm1.invoke(new SerializableRunnable("wait for reconnect") {
      @Override
      public void run() {
        final GemFireCacheImpl cache = savedCache;
        GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
          @Override
          public boolean done() {
            return cache.isReconnecting();
          }

          @Override
          public String description() {
            return "waiting for cache to begin reconnecting";
          }
        });
        out.println("entering reconnect wait for " + cache);
        try {
          cache.waitUntilReconnected(5, MINUTES);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
        assertNotNull(cache.getReconnectedCache());
      }
    });
  }

  /**
   * GEODE-2155 Auto-reconnect fails with NPE due to a cache listener Declarable.init method
   * throwing an exception.
   */
  @Test
  public void testReconnectFailsDueToBadCacheXML() throws Exception {

    Host host = getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final int locPort = locatorPort;

    SerializableRunnable createCache = new SerializableRunnable("Create Cache and Regions") {
      @Override
      public void run() {
        locatorPort = locPort;
        final Properties props = getDistributedSystemProperties();
        props.put(MAX_WAIT_TIME_RECONNECT, "1000");
        dsProperties = props;
        savedSystem = getSystem(props);
        savedCache = (GemFireCacheImpl) getCache();
        Region myRegion = createRegion("myRegion", createAtts());
        myRegion.put("MyKey", "MyValue");
        myRegion.getAttributesMutator().addCacheListener(new ListenerWhoseInitMethodAlwaysThrows());
      }
    };

    vm0.invoke(createCache); // vm0 keeps the locator from losing quorum when vm1 crashes

    createCache.run();
    addIgnoredException(
        "DistributedSystemDisconnectedException|ForcedDisconnectException", vm1);
    forceDisconnect(null);

    final GemFireCacheImpl cache = savedCache;
    GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
      @Override
      public boolean done() {
        return cache.isReconnecting()
            || cache.getInternalDistributedSystem().isReconnectCancelled();
      }

      @Override
      public String description() {
        return "waiting for cache to begin reconnecting";
      }
    });
    assertThatThrownBy(() -> cache.waitUntilReconnected(getTimeout().toMillis(), MILLISECONDS))
        .isInstanceOf(CacheClosedException.class)
        .hasMessageContaining("Cache could not be recreated")
        .hasCauseExactlyInstanceOf(DistributedSystemDisconnectedException.class);
    assertTrue(cache.getInternalDistributedSystem().isReconnectCancelled());
    assertNull(cache.getReconnectedCache());
  }

  private CacheSerializableRunnable getRoleAPlayerRunnable(final int locPort,
      final String regionName, final String myKey, final String myValue,
      final String startupMessage) {
    return new CacheSerializableRunnable("second RoleA player") {
      @Override
      public void run2() throws CacheException {
        System.out.println(startupMessage);
        // closeCache();
        // getSystem().disconnect();
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put(LOG_LEVEL, "info");
        props.put(ROLES, "RoleA");

        getSystem(props);
        getCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.create();
        Region region = createRootRegion(regionName, attr);
        System.out.println("STARTED THE REQUIREDROLES CACHE");
        try {
          Thread.sleep(120);
        } catch (Exception ee) {
          fail("interrupted");
        }

        region.put(myKey, myValue);
        try {
          Thread.sleep(5000); // why are we sleeping for 5 seconds here?
          // if it is to give time to avkVm0 to notice us we should have
          // avkVm0 signal us that it has seen us and then we can exit.
        } catch (InterruptedException ee) {
          fail("interrupted");
        }
        System.out.println("RolePlayer is done...");


      }


    };
  }


  private CacheSerializableRunnable getRoleAPlayerForCacheInitializationRunnable(final VM otherVM,
      final int locPort, final String regionName, final String startupMessage) {
    return new CacheSerializableRunnable("first RoleA player") {
      @Override
      public void run2() throws CacheException {
        // closeCache();
        // getSystem().disconnect();
        System.out.println(startupMessage);
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put(LOG_LEVEL, "info");
        props.put(ROLES, "RoleA");

        getSystem(props);
        getCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.create();
        createRootRegion(regionName, attr);
        System.out.println("STARTED THE REQUIREDROLES CACHE");
        initialRolePlayerStarted = true;

        while (!otherVM.invoke(() -> ReconnectDUnitTest.isInitialized()).booleanValue()) {
          try {
            Thread.sleep(15);
          } catch (InterruptedException ignor) {
            fail("interrupted");
          }
        }
        System.out.println("RoleAPlayerInitializer is done...");
        closeCache();

      }
    };
  }


  void addReconnectListener() {
    reconnectTries = 0; // reset the count for this listener
    System.out.println("adding reconnect listener");
    ReconnectListener reconlis = new ReconnectListener() {
      @Override
      public void reconnecting(InternalDistributedSystem oldSys) {
        System.out.println("reconnect listener invoked");
        reconnectTries++;
      }

      @Override
      public void onReconnect(InternalDistributedSystem system1,
          InternalDistributedSystem system2) {
        System.out.println("reconnect listener onReconnect invoked " + system2);
        cache = system2.getCache();
      }
    };
    InternalDistributedSystem.addReconnectListener(reconlis);
  }

  private void waitTimeout() throws InterruptedException {
    Thread.sleep(500);

  }

  public boolean forceDisconnect(VM vm) throws Exception {
    SerializableCallable fd = new SerializableCallable("crash distributed system") {
      @Override
      public Object call() throws Exception {
        // since the system will disconnect and attempt to reconnect
        // a new system the old reference to DTC.system can cause
        // trouble, so we first null it out.
        nullSystem();
        final Locator oldLocator = Locator.getLocator();
        final DistributedSystem msys = cache.getDistributedSystem();
        MembershipManagerHelper.crashDistributedSystem(msys);
        if (oldLocator != null) {
          WaitCriterion wc = new WaitCriterion() {
            @Override
            public boolean done() {
              return msys.isReconnecting() || msys.getReconnectedSystem() != null;
            }

            @Override
            public String description() {
              return "waiting for locator to start reconnecting: " + oldLocator;
            }
          };
          GeodeAwaitility.await().untilAsserted(wc);
        }
        return true;
      }
    };
    if (vm != null) {
      return (Boolean) vm.invoke(fd);
    } else {
      return (Boolean) fd.call();
    }
  }

  /**
   * A listener whose init always throws an exception.
   * Since init is always called during cache.xml parsing
   * this listener is not able to be used from cache.xml.
   */
  public static class ListenerWhoseInitMethodAlwaysThrows extends CacheListenerAdapter {
    @Override
    public void init(Properties props) {
      throw new RuntimeException("Cause parsing to fail");
    };
  }

  /**
   * CacheListenerTriggeringForcedDisconnect crashes the distributed system when it is invoked for
   * the first time.
   * After that it ignores any notifications.
   */
  public static class CacheListenerTriggeringForcedDisconnect extends CacheListenerAdapter
      implements Declarable {
    public static int crashCount = 0;

    @Override
    public void afterRegionCreate(final RegionEvent event) {
      if (crashCount == 0) {
        crashCount += 1;
        // we crash the system in a different thread than the ReconnectThread
        // to simulate receiving a ForcedDisconnect from the membership manager
        // in the UDP reader thread
        Thread t = new Thread("crash reconnecting system (ReconnectDUnitTest)") {
          @Override
          public void run() {
            System.out.println("crashing distributed system");
            GemFireCacheImpl cache = (GemFireCacheImpl) event.getRegion().getCache();
            MembershipManagerHelper.crashDistributedSystem(cache.getDistributedSystem());
          }
        };
        t.setDaemon(true);
        t.start();
      }
    }

    @Override
    public void init(Properties props) {}

  }

  protected void addDSProps(Properties p) {}
}
