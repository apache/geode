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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.ACK_SEVERE_ALERT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.ACK_WAIT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.NetworkUtils.getIPLiteral;
import static org.apache.geode.test.dunit.Wait.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.LogWriter;
import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminDistributedSystemFactory;
import org.apache.geode.admin.AlertLevel;
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
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * This class tests the functionality of the {@link ClusterDistributionManager} class.
 */
@Category({MembershipTest.class})
public class ClusterDistributionManagerDUnitTest extends DistributedTestCase {
  private static final Logger logger = LogService.getLogger();

  private static volatile boolean regionDestroyedInvoked;
  private static volatile Cache myCache;
  private static volatile boolean alertReceived;

  private transient ExecutorService executorService;

  private VM vm1;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() throws Exception {
    executorService = Executors.newSingleThreadExecutor();
    vm1 = Host.getHost(0).getVM(1);
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
    invokeInEveryVM(() -> {
      regionDestroyedInvoked = false;
      myCache = null;
      alertReceived = false;
    });
    assertThat(executorService.shutdownNow()).isEmpty();
  }

  @Test
  public void testGetDistributionVMType() {
    DistributionManager dm = getSystem().getDistributionManager();
    InternalDistributedMember member = dm.getId();

    assertEquals(ClusterDistributionManager.NORMAL_DM_TYPE, member.getVmKind());
  }

  /**
   * Demonstrate that a new UDP port is used when an attempt is made to reconnect using a shunned
   * port
   */
  @Test
  public void testConnectAfterBeingShunned() {
    InternalDistributedSystem system = getSystem();
    MembershipManager membershipManager = MembershipManagerHelper.getMembershipManager(system);
    InternalDistributedMember memberBefore = membershipManager.getLocalMember();

    // TODO GMS needs to have a system property allowing the bind-port to be set
    System.setProperty(GEMFIRE_PREFIX + "jg-bind-port", "" + memberBefore.getPort());
    system.disconnect();
    system = getSystem();
    membershipManager = MembershipManagerHelper.getMembershipManager(system);
    system.disconnect();
    InternalDistributedMember memberAfter = membershipManager.getLocalMember();

    assertThat(memberAfter.getPort()).isEqualTo(memberBefore.getPort());
  }

  /**
   * Test the handling of "surprise members" in the membership manager. Create a DistributedSystem
   * in this VM and then add a fake member to its surpriseMember set. Then ensure that it stays in
   * the set when a new membership view arrives that doesn't contain it. Then wait until the member
   * should be gone and force more view processing to have it scrubbed from the set.
   **/
  @Test
  public void testSurpriseMemberHandling() throws Exception {
    System.setProperty(GEMFIRE_PREFIX + "surprise-member-timeout", "3000");
    InternalDistributedSystem system = getSystem();
    GMSMembershipManager membershipManager =
        (GMSMembershipManager) MembershipManagerHelper.getMembershipManager(system);
    assertThat(membershipManager.isCleanupTimerStarted()).isTrue();

    InternalDistributedMember member = new InternalDistributedMember(getIPLiteral(), 12345);

    // first make sure we can't add this as a surprise member (bug #44566)
    // if the view number isn't being recorded correctly the test will pass but the
    // functionality is broken
    assertThat(membershipManager.getView().getViewId()).isGreaterThan(0);

    int oldViewId = member.getVmViewId();
    member.setVmViewId(membershipManager.getView().getViewId() - 1);

    addIgnoredException("attempt to add old member");
    addIgnoredException("Removing shunned GemFire node");

    boolean accepted = membershipManager.addSurpriseMember(member);
    assertThat(accepted).as("member with old ID was not rejected (bug #44566)").isFalse();

    member.setVmViewId(oldViewId);

    // now forcibly add it as a surprise member and show that it is reaped
    long gracePeriod = 5000;
    long startTime = System.currentTimeMillis();
    long timeout = membershipManager.getSurpriseMemberTimeout();
    long birthTime = startTime - timeout + gracePeriod;
    MembershipManagerHelper.addSurpriseMember(system, member, birthTime);
    assertThat(membershipManager.isSurpriseMember(member)).as("Member was not a surprise member")
        .isTrue();

    await("waiting for member to be removed")
        .until(() -> !membershipManager.isSurpriseMember(member));
  }

  /**
   * Tests that a severe-level alert is generated if a member does not respond with an ack quickly
   * enough. vm0 and vm1 create a region and set ack-severe-alert-threshold. vm1 has a cache
   * listener in its region that sleeps when notified, forcing the operation to take longer than
   * ack-wait-threshold + ack-severe-alert-threshold
   */
  @Test
  public void testAckSevereAlertThreshold() throws Exception {
    // in order to set a small ack-wait-threshold, we have to remove the
    // system property established by the dunit harness
    System.clearProperty(GEMFIRE_PREFIX + ACK_WAIT_THRESHOLD);

    Properties config = getDistributedSystemProperties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(ACK_WAIT_THRESHOLD, "3");
    config.setProperty(ACK_SEVERE_ALERT_THRESHOLD, "3");
    config.setProperty(NAME, "putter");

    getSystem(config);
    Region<String, String> region =
        new RegionFactory<String, String>().setScope(Scope.DISTRIBUTED_ACK).setEarlyAck(false)
            .setDataPolicy(DataPolicy.REPLICATE).create("testRegion");

    vm1.invoke("Connect to distributed system", () -> {
      config.setProperty(NAME, "sleeper");
      getSystem(config);
      addIgnoredException("elapsed while waiting for replies");

      RegionFactory<String, String> regionFactory = new RegionFactory<>();
      Region<String, String> sameRegion =
          regionFactory.setScope(Scope.DISTRIBUTED_ACK).setDataPolicy(DataPolicy.REPLICATE)
              .setEarlyAck(false).addCacheListener(getSleepingListener(false)).create("testRegion");
      myCache = sameRegion.getCache();

      createAlertListener();
    });

    // now we have two caches set up. vm1 has a listener that will sleep
    // and cause the severe-alert threshold to be crossed

    region.put("bomb", "pow!"); // this will hang until vm1 responds
    disconnectAllFromDS();

    vm1.invoke(() -> {
      assertThat(alertReceived).isTrue();
    });
  }

  /**
   * Tests that a sick member is kicked out
   */
  @Test
  public void testKickOutSickMember() throws Exception {
    addIgnoredException("10 seconds have elapsed while waiting");

    // in order to set a small ack-wait-threshold, we have to remove the
    // system property established by the dunit harness
    System.clearProperty(GEMFIRE_PREFIX + ACK_WAIT_THRESHOLD);

    Properties config = getDistributedSystemProperties();
    config.setProperty(MCAST_PORT, "0"); // loner
    config.setProperty(ACK_WAIT_THRESHOLD, "5");
    config.setProperty(ACK_SEVERE_ALERT_THRESHOLD, "5");
    config.setProperty(NAME, "putter");

    getSystem(config);
    Region<String, String> region = new RegionFactory<String, String>()
        .setScope(Scope.DISTRIBUTED_ACK).setDataPolicy(DataPolicy.REPLICATE).create("testRegion");

    addIgnoredException("sec have elapsed while waiting for replies");

    vm1.invoke(new SerializableRunnable("Connect to distributed system") {
      @Override
      public void run() {
        config.setProperty(NAME, "sleeper");
        getSystem(config);

        addIgnoredException("service failure");
        addIgnoredException(ForcedDisconnectException.class.getName());

        RegionFactory<String, String> regionFactory = new RegionFactory<>();
        Region sameRegion =
            regionFactory.setScope(Scope.DISTRIBUTED_ACK).setDataPolicy(DataPolicy.REPLICATE)
                .addCacheListener(getSleepingListener(true)).create("testRegion");
        myCache = sameRegion.getCache();
      }
    });

    // now we have two caches set up, each having an alert listener. Vm1
    // also has a cache listener that will turn off its ability to respond
    // to "are you dead" messages and then sleep

    region.put("bomb", "pow!");
    disconnectFromDS();

    vm1.invoke("wait for forced disconnect", () -> {
      await("vm1's system should have been disconnected")
          .untilAsserted(() -> assertThat(basicGetSystem().isConnected()).isFalse());

      await("vm1's cache is not closed")
          .untilAsserted(() -> assertThat(myCache.isClosed()).isTrue());

      await("vm1's listener should have received afterRegionDestroyed notification")
          .untilAsserted(() -> assertThat(regionDestroyedInvoked).isTrue());
    });
  }

  /**
   * test use of a bad bind-address for bug #32565
   */
  @Test
  public void testBadBindAddress() throws Exception {
    Properties config = getDistributedSystemProperties();
    config.setProperty(MCAST_PORT, "0"); // loner
    config.setProperty(ACK_WAIT_THRESHOLD, "5");
    config.setProperty(ACK_SEVERE_ALERT_THRESHOLD, "5");

    // use a valid address that's not proper for this machine
    config.setProperty(BIND_ADDRESS, "www.yahoo.com");
    assertThatThrownBy(() -> getSystem(config)).isInstanceOf(IllegalArgumentException.class);

    // use an invalid address
    config.setProperty(BIND_ADDRESS, "bruce.schuchardt");
    assertThatThrownBy(() -> getSystem(config)).isInstanceOf(IllegalArgumentException.class);

    // use a valid bind address
    config.setProperty(BIND_ADDRESS, InetAddress.getLocalHost().getCanonicalHostName());
    assertThatCode(() -> getSystem()).doesNotThrowAnyException();
  }

  /**
   * install a new view and show that waitForViewInstallation works as expected
   */
  @Test
  public void testWaitForViewInstallation() {
    InternalDistributedSystem system = getSystem(new Properties());
    ClusterDistributionManager dm = (ClusterDistributionManager) system.getDM();
    GMSMembershipManager membershipManager = (GMSMembershipManager) dm.getMembershipManager();
    NetView view = membershipManager.getView();

    AtomicBoolean waitForViewInstallationDone = new AtomicBoolean();
    executorService.submit(() -> {
      try {
        dm.waitForViewInstallation(view.getViewId() + 1);
        waitForViewInstallationDone.set(true);
      } catch (InterruptedException e) {
        errorCollector.addError(e);
      }
    });

    pause(2000);

    NetView newView = new NetView(view, view.getViewId() + 1);
    membershipManager.installView(newView);

    await()
        .untilAsserted(() -> assertThat(waitForViewInstallationDone.get()).isTrue());
  }

  private CacheListener<String, String> getSleepingListener(final boolean playDead) {
    regionDestroyedInvoked = false;

    return new CacheListenerAdapter<String, String>() {
      @Override
      public void afterCreate(EntryEvent event) {
        try {
          if (playDead) {
            MembershipManagerHelper.beSickMember(getSystemStatic());
            MembershipManagerHelper.playDead(getSystemStatic());
          }
          Thread.sleep(15 * 1000);
        } catch (InterruptedException ie) {
          errorCollector.addError(ie);
        }
      }

      @Override
      public void afterRegionDestroy(RegionEvent event) {
        LogWriter logWriter = myCache.getLogger();
        logWriter.info("afterRegionDestroyed invoked in sleeping listener");
        logWriter.info("<ExpectedException action=remove>service failure</ExpectedException>");
        logWriter.info(
            "<ExpectedException action=remove>org.apache.geode.ForcedDisconnectException</ExpectedException>");
        regionDestroyedInvoked = true;
      }
    };
  }

  private void createAlertListener() throws Exception {
    DistributedSystemConfig config =
        AdminDistributedSystemFactory.defineDistributedSystem(getSystemStatic(), null);
    AdminDistributedSystem adminSystem = AdminDistributedSystemFactory.getDistributedSystem(config);
    adminSystem.setAlertLevel(AlertLevel.SEVERE);
    adminSystem.addAlertListener(alert -> {
      try {
        logger.info("alert listener invoked for alert originating in " + alert.getConnectionName());
        logger.info("  alert text = " + alert.getMessage());
        logger.info("  systemMember = " + alert.getSystemMember());
      } catch (Exception e) {
        errorCollector.addError(e);
      }
      alertReceived = true;
    });
    adminSystem.connect();
    assertTrue(adminSystem.waitToBeConnected(5 * 1000));
  }
}
