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
package org.apache.geode.internal.cache.tier.sockets;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

@SuppressWarnings({"rawtypes", "serial"})
@Category(DistributedTest.class)
public class HAInterestPart2DUnitTest extends HAInterestTestCase {

  public HAInterestPart2DUnitTest() {
    super();
  }

  /**
   * Tests if Primary fails during interest un registration should initiate failover should pick new
   * primary
   */
  @Test
  public void testPrimaryFailureInUNregisterInterest() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server2.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server3.invoke(() -> HAInterestTestCase.createEntriesK1andK2());

    registerK1AndK2();

    VM oldPrimary = getPrimaryVM();
    stopPrimaryAndUnregisterRegisterK1();

    verifyDeadAndLiveServers(1, 2);

    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(() -> HAInterestTestCase.verifyDispatcherIsAlive());
    // primary
    newPrimary.invoke(() -> HAInterestTestCase.verifyInterestUNRegistration());
    // secondary
    getBackupVM().invoke(() -> HAInterestTestCase.verifyInterestUNRegistration());
  }

  /**
   * Tests if Secondary fails during interest un registration should add to dead Ep list
   */
  @Test
  public void testSecondaryFailureInUNRegisterInterest() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server2.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server3.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    registerK1AndK2();
    VM stoppedBackup = stopSecondaryAndUNregisterK1();
    verifyDeadAndLiveServers(1, 2);
    // still primary
    getPrimaryVM().invoke(() -> HAInterestTestCase.verifyDispatcherIsAlive());
    // primary
    getPrimaryVM().invoke(() -> HAInterestTestCase.verifyInterestUNRegistration());
    // secondary
    getBackupVM(stoppedBackup).invoke(() -> HAInterestTestCase.verifyInterestUNRegistration());
  }

  /**
   * Tests a scenario in which Dead Server Monitor detects Server Live Just before interest
   * registration then interest should be registered on the newly detected live server as well
   */
  @Test
  public void testDSMDetectsServerLiveJustBeforeInterestRegistration() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server2.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server3.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    VM backup = getBackupVM();
    backup.invoke(() -> HAInterestTestCase.stopServer());
    verifyDeadAndLiveServers(1, 2);
    setClientServerObserverForBeforeRegistration(backup);
    try {
      registerK1AndK2();
      waitForBeforeRegistrationCallback();
    } finally {
      unSetClientServerObserverForRegistrationCallback();
    }
    server1.invoke(() -> HAInterestTestCase.verifyInterestRegistration());
    server2.invoke(() -> HAInterestTestCase.verifyInterestRegistration());
    server3.invoke(() -> HAInterestTestCase.verifyInterestRegistration());
  }

  /**
   * Tests a scenario in which Dead Server Monitor detects Server Live Just After interest
   * registration then interest should be registered on the newly detected live server as well
   */
  @Test
  public void testDSMDetectsServerLiveJustAfterInterestRegistration() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));

    createEntriesK1andK2();
    server1.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server2.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server3.invoke(() -> HAInterestTestCase.createEntriesK1andK2());

    VM backup = getBackupVM();
    backup.invoke(() -> HAInterestTestCase.stopServer());
    verifyDeadAndLiveServers(1, 2);

    setClientServerObserverForAfterRegistration(backup);
    try {
      registerK1AndK2();
      waitForAfterRegistrationCallback();
    } finally {
      unSetClientServerObserverForRegistrationCallback();
    }

    server1.invoke(() -> HAInterestTestCase.verifyInterestRegistration());
    server2.invoke(() -> HAInterestTestCase.verifyInterestRegistration());
    server3.invoke(() -> HAInterestTestCase.verifyInterestRegistration());
  }

  /**
   * Tests a Scenario: Only one server, register interest on the server stop server , and update the
   * registered entries on the server start the server , DSM will recover interest list on this live
   * server and verify that as a part of recovery it refreshes registered entries from the server,
   * because it is primary
   */
  @Test
  public void testRefreshEntriesFromPrimaryWhenDSMDetectsServerLive() throws Exception {
    IgnoredException.addIgnoredException(ServerConnectivityException.class.getName());

    PORT1 = ((Integer) server1.invoke(() -> HAInterestTestCase.createServerCache())).intValue();
    server1.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    createClientPoolCacheConnectionToSingleServer(this.getName(),
        NetworkUtils.getServerHostName(server1.getHost()));
    registerK1AndK2();
    verifyRefreshedEntriesFromServer();

    server1.invoke(() -> HAInterestTestCase.stopServer());
    verifyDeadAndLiveServers(1, 0);
    server1.invoke(() -> HAInterestTestCase.putK1andK2());
    server1.invoke(() -> HAInterestTestCase.startServer());
    verifyDeadAndLiveServers(0, 1);
    final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    // Verify for interest registration after cache-server is started.
    server1.invoke(() -> HAInterestTestCase.verifyInterestRegistration());

    WaitCriterion wc = new WaitCriterion() {
      private String excuse;

      @Override
      public boolean done() {
        Region.Entry e1;
        Region.Entry e2;

        try {
          e1 = r1.getEntry(k1);
          if (e1 == null) {
            excuse = "Entry for k1 is null";
            return false;
          }
        } catch (EntryDestroyedException e) {
          excuse = "Entry destroyed";
          return false;
        }
        if (!server_k1.equals(e1.getValue())) {
          excuse = "e1 value is not server_k1";
          return false;
        }
        try {
          e2 = r1.getEntry(k2);
          if (e2 == null) {
            excuse = "Entry for k2 is null";
            return false;
          }
        } catch (EntryDestroyedException e) {
          excuse = "Entry destroyed";
          return false;
        }
        if (!server_k2.equals(e2.getValue())) {
          excuse = "e2 value is not server_k2";
          return false;
        }
        return true;
      }

      @Override
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, TIMEOUT_MILLIS, INTERVAL_MILLIS, true);
  }

  /**
   * Tests a Scenario: stop a secondary server and update the registered entries on the stopped
   * server start the server , DSM will recover interest list on this live server and verify that as
   * a part of recovery it does not refreshes registered entries from the server, because it is
   * secondary
   */
  @Test
  public void testGIIFromSecondaryWhenDSMDetectsServerLive() throws Exception {
    server1.invoke(() -> HAInterestTestCase.closeCache());
    server2.invoke(() -> HAInterestTestCase.closeCache());
    server3.invoke(() -> HAInterestTestCase.closeCache());

    PORT1 = ((Integer) server1.invoke(() -> HAInterestTestCase.createServerCacheWithLocalRegion()))
        .intValue();
    PORT2 = ((Integer) server2.invoke(() -> HAInterestTestCase.createServerCacheWithLocalRegion()))
        .intValue();
    PORT3 = ((Integer) server3.invoke(() -> HAInterestTestCase.createServerCacheWithLocalRegion()))
        .intValue();

    server1.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server2.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    server3.invoke(() -> HAInterestTestCase.createEntriesK1andK2());

    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));

    VM backup1 = getBackupVM();
    VM backup2 = getBackupVM(backup1);
    backup1.invoke(() -> HAInterestTestCase.stopServer());
    backup2.invoke(() -> HAInterestTestCase.stopServer());
    verifyDeadAndLiveServers(2, 1);
    registerK1AndK2();
    verifyRefreshedEntriesFromServer();
    backup1.invoke(() -> HAInterestTestCase.putK1andK2());
    backup1.invoke(() -> HAInterestTestCase.startServer());
    verifyDeadAndLiveServers(1, 2);
    verifyRefreshedEntriesFromServer();
  }

  /**
   * Bug Test for Bug # 35945 A java level Deadlock between acquireConnection and RegionEntry during
   * processRecoveredEndpoint by Dead Server Monitor Thread.
   *
   * @throws Exception
   */
  @Test
  public void testBug35945() throws Exception {
    PORT1 = ((Integer) server1.invoke(() -> HAInterestTestCase.createServerCache())).intValue();
    server1.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    createClientPoolCacheConnectionToSingleServer(this.getName(),
        NetworkUtils.getServerHostName(server1.getHost()));
    registerK1AndK2();
    verifyRefreshedEntriesFromServer();

    server1.invoke(() -> HAInterestTestCase.stopServer());
    verifyDeadAndLiveServers(1, 0);
    // put on stopped server
    server1.invoke(() -> HAInterestTestCase.putK1andK2());
    // spawn a thread to put on server , which will acquire a lock on entry
    setClientServerObserverForBeforeInterestRecovery();
    server1.invoke(() -> HAInterestTestCase.startServer());
    verifyDeadAndLiveServers(0, 1);
    waitForBeforeInterestRecoveryCallBack();
    // verify updated value of k1 as a refreshEntriesFromServer
    final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);

    WaitCriterion wc = new WaitCriterion() {
      private String excuse;

      @Override
      public boolean done() {
        Region.Entry e1 = r1.getEntry(k1);
        Region.Entry e2 = r1.getEntry(k2);
        Object v1 = null;
        if (e1 != null) {
          try {
            v1 = e1.getValue();
          } catch (EntryDestroyedException ignore) {
            // handled to fix GEODE-296
          }
        }
        if (e1 == null || !server_k1_updated.equals(v1)) {
          excuse = "v1=" + v1;
          return false;
        }
        Object v2 = null;
        if (e2 != null) {
          try {
            v2 = e2.getValue();
          } catch (EntryDestroyedException ignore) {
            // handled to fix GEODE-296
          }
        }
        if (e2 == null || !server_k2.equals(v2)) {
          excuse = "v2=" + v2;
          return false;
        }
        return true;
      }

      @Override
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, TIMEOUT_MILLIS, INTERVAL_MILLIS, true);
  }

  /**
   * Tests if failure occurred in Interest recovery thread, then it should select new endpoint to
   * register interest
   */
  @Category(FlakyTest.class) // GEODE-108
  @Test
  public void testInterestRecoveryFailure() throws Exception {
    IgnoredException.addIgnoredException("Server unreachable");

    PORT1 = ((Integer) server1.invoke(() -> HAInterestTestCase.createServerCache())).intValue();
    server1.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    PORT2 = ((Integer) server2.invoke(() -> HAInterestTestCase.createServerCache())).intValue();
    server2.invoke(() -> HAInterestTestCase.createEntriesK1andK2());
    createClientPoolCacheWithSmallRetryInterval(this.getName(),
        NetworkUtils.getServerHostName(server1.getHost()));
    registerK1AndK2();
    verifyRefreshedEntriesFromServer();
    VM backup = getBackupVM();
    VM primary = getPrimaryVM();

    backup.invoke(() -> HAInterestTestCase.stopServer());
    primary.invoke(() -> HAInterestTestCase.stopServer());
    verifyDeadAndLiveServers(2, 0);

    primary.invoke(() -> HAInterestTestCase.putK1andK2());
    setClientServerObserverForBeforeInterestRecoveryFailure();
    primary.invoke(() -> HAInterestTestCase.startServer());
    waitForBeforeInterestRecoveryCallBack();
    if (exceptionOccured) {
      fail("The DSM could not ensure that server 1 is started & serevr 2 is stopped");
    }
    final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);

    WaitCriterion wc = new WaitCriterion() {
      private String excuse;

      @Override
      public boolean done() {
        Region.Entry e1 = r1.getEntry(k1);
        Region.Entry e2 = r1.getEntry(k2);
        if (e1 == null) {
          excuse = "Entry for k1 still null";
          return false;
        }
        if (e2 == null) {
          excuse = "Entry for k2 still null";
          return false;
        }
        if (!(server_k1.equals(e1.getValue()))) {
          excuse = "Value for k1 wrong";
          return false;
        }
        if (!(server_k2.equals(e2.getValue()))) {
          excuse = "Value for k2 wrong";
          return false;
        }
        return true;
      }

      @Override
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, TIMEOUT_MILLIS, INTERVAL_MILLIS, true);
  }
}
