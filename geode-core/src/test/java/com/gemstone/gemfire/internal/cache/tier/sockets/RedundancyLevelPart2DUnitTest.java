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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class RedundancyLevelPart2DUnitTest extends RedundancyLevelTestBase {

  @BeforeClass
  public static void caseSetUp() throws Exception {
    disconnectAllFromDS();
  }

  private void waitConnectedServers(final int expected) {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return expected == pool.getConnectedServerCount();
      }
      public String description() {
        return "Connected server count (" + pool.getConnectedServerCount() 
        + ") never became " + expected;
      }
    };
    Wait.waitForCriterion(wc, 2 * 60 * 1000, 1000, true);
  }
  
  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the live server
   * map to compensate for the failure. The EP failed also happened to be a
   * Primary EP so a new EP from fail over set should become the primary and
   * make sure that CCP is created on the server with relevant interest
   * registartion.
   * Failure Detection by LSM
   */
  @Test
  public void testRedundancySpecifiedPrimaryEPFails() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1);
      waitConnectedServers(4);
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));

      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      //pause(5000);
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      //assertTrue(pool.getRedundantNames()
      //    .contains(SERVER2));
      assertTrue(pool.getPrimaryName().equals(SERVER2));
      //assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedPrimaryEPFails ",
          ex);
    }
  }
  
  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the live server
   * map to compensate for the failure. The EP failed also happened to be a
   * Primary EP so a new EP from fail over set should become the primary and
   * make sure that CCP is created on the server with relevant interest
   * registartion.
   * Failure Detection by CCU
   */
  @Test
  public void testRedundancySpecifiedPrimaryEPFailsDetectionByCCU() {
    try {
      
      FailOverDetectionByCCU = true;
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,3000,100);
      waitConnectedServers(4);
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      verifyOrderOfEndpoints();
      // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER2));
      assertTrue(pool.getPrimaryName().equals(SERVER2));
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedPrimaryEPFailsDetectionByCCU ",
          ex);
    }
  }

  /*
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the live server
   * map to compensate for the failure. The EP failed also happened to be a
   * Primary EP so a new EP from fail over set should become the primary and
   * make sure that CCP is created on the server with relevant interest
   * registartion.
   * Failure Detection by Register Interest
   */
  @Test
  public void testRedundancySpecifiedPrimaryEPFailsDetectionByRegisterInterest() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,3000, 100);
      waitConnectedServers(4);
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      createEntriesK1andK2();
      registerK1AndK2();
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      verifyOrderOfEndpoints();
      // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER2));
      assertTrue(pool.getPrimaryName().equals(SERVER2));
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedPrimaryEPFailsDetectionByRegisterInterest ",
          ex);
    }
  }
  
  /*
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the live server
   * map to compensate for the failure. The EP failed also happened to be a
   * Primary EP so a new EP from fail over set should become the primary and
   * make sure that CCP is created on the server with relevant interest
   * registartion.
   * Failure Detection by Unregister Interest
   */  
  @Test
  public void testRedundancySpecifiedPrimaryEPFailsDetectionByUnregisterInterest() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,3000,100);
      waitConnectedServers(4);
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      unregisterInterest();
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      verifyOrderOfEndpoints();
      // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER2));
      assertTrue(pool.getPrimaryName().equals(SERVER2));
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedPrimaryEPFailsDetectionByUnregisterInterest ",
          ex);
    }
  }
  
  /*
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the live server
   * map to compensate for the failure. The EP failed also happened to be a
   * Primary EP so a new EP from fail over set should become the primary and
   * make sure that CCP is created on the server with relevant interest
   * registartion.
   * Failure Detection by put operation
   */
  @Test
  public void testRedundancySpecifiedPrimaryEPFailsDetectionByPut() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,3000, 100);
      waitConnectedServers(4);
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      doPuts();
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      verifyOrderOfEndpoints();
      // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER2));
      assertTrue(pool.getPrimaryName().equals(SERVER2));
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedPrimaryEPFailsDetectionByPut ",
          ex);
    }
  }
  
  /*
   * If there are 4 servers in Live Server Map with redundancy level as 1. Kill
   * the primary & secondary. Two Eps should be added from the live server & the
   * existing EP in the set should be the new primary and make sure that CCP is
   * created on both the server with relevant interest registartion.
   */
  @Test
  public void testRedundancySpecifiedPrimarySecondaryEPFails() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1);
      waitConnectedServers(4);
      assertEquals(1, pool.getRedundantNames().size());
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      assertFalse(pool.getRedundantNames().contains(SERVER3));
      assertFalse(pool.getRedundantNames().contains(SERVER4));
      verifyOrderOfEndpoints();
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      //pause(5000);
      verifyLiveAndRedundantServers(2, 1);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      //Not Sure
      //assertTrue(pool.getPrimaryName().equals(SERVER2));
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      //assertTrue(pool.getRedundantNames().contains(SERVER3));
      server3.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedPrimarySecondaryEPFails ",
          ex);
    }
  }

  /**
   * There are 4 Eps in Live serevr Map with redundancy level as 2. Kill two Eps
   * (excluding the primary). As a result live server map will contain 2 ,
   * active list will contain two & dead server map will contain 2. Redundancy
   * is unsatisfied by 1. Bring up one EP . The DSM should add the alive EP to
   * the active end point & live server map with primary unchnaged. Also make
   * sure that CCP is created on the server with relevant interest registartion.
   * Bringing the 4th EP alive should simply add it to Live server map.
   */
  @Test
  public void testRedundancySpecifiedEPFails() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 2);
      waitConnectedServers(4);
      assertEquals(2, pool.getRedundantNames().size());
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      assertTrue(pool.getRedundantNames().contains(SERVER3));
      assertFalse(pool.getRedundantNames().contains(SERVER4));
      // kill non primary EPs
      verifyOrderOfEndpoints();
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      server2.invoke(() -> RedundancyLevelTestBase.stopServer());
      verifyDeadServers(2);
      //assertIndexDetailsEquals(2, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(2, proxy.getDeadServers().size());
      //pause(10000);
      verifyLiveAndRedundantServers(2, 1);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // bring up one server.
      server1.invoke(() -> RedundancyLevelTestBase.startServer());
      //pause(10000);
      verifyLiveAndRedundantServers(3, 2);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(3, pool.getRedundantNames().size());
      //assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      verifyRedundantServersContain(SERVER2);
      verifyRedundantServersContain(SERVER4);
      server1.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      // bring up another server should get added to live server map only and
      // not to the active server as redundancy level is satisfied.
      server2.invoke(() -> RedundancyLevelTestBase.startServer());
      //pause(10000);
      Wait.pause(1000);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(3, pool.getRedundantNames().size());
      //assertIndexDetailsEquals(4, pool.getConnectedServerCount());
      server2.invoke(() -> RedundancyLevelTestBase.verifyNoCCP());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedEPFails ",
          ex);
    }
  }

  /**
   * Redundancy level specified but not satisfied, new EP is added then it
   * should be added in Live server map as well as failover set and make sure
   * that CCP is created on the server with relevant interest registartion.
   */
  @Test
  public void testRedundancyLevelSpecifiedButNotSatisfied() {
    try {
      // stop two secondaries
      server2.invoke(() -> RedundancyLevelTestBase.stopServer());
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      // make sure that the client connects to only two servers and
      // redundancyLevel
      // unsatisfied with one
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 2);
      // let the client connect to servers
      //pause(10000);      
      verifyLiveAndRedundantServers(2, 1);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      //assertIndexDetailsEquals(2, pool.getConnectedServerCount());
      assertTrue(pool.getRedundantNames().contains(SERVER4));
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertFalse(pool.getRedundantNames().contains(SERVER3));
      assertFalse(pool.getRedundantNames().contains(SERVER2));
      // start server
      server2.invoke(() -> RedundancyLevelTestBase.startServer());
      //pause(10000);
      verifyLiveAndRedundantServers(3, 2);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(3, pool.getRedundantNames().size());
      assertTrue(pool.getRedundantNames().contains(SERVER4));
      assertTrue(pool.getRedundantNames().contains(SERVER3));
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertFalse(pool.getRedundantNames().contains(SERVER2));
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      // verify that redundancy level is satisfied
      server1.invoke(() -> RedundancyLevelTestBase.startServer());
      //pause(10000);
      Wait.pause(1000);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(3, pool.getRedundantNames().size());
      //assertIndexDetailsEquals(4, pool.getConnectedServerCount());
      assertTrue(pool.getRedundantNames().contains(SERVER4));
      assertTrue(pool.getRedundantNames().contains(SERVER3));
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertFalse(pool.getRedundantNames().contains(SERVER2));
      server1.invoke(() -> RedundancyLevelTestBase.verifyNoCCP());

    }
    catch (Exception ex) {
      Assert.fail("test failed due to exception in test noRedundancyLevelServerFail ",
          ex);
    }

  }

  /**
   * Redundancy level specified and satisfied, new EP is added then it should be
   * added only in Live server map and make sure that no CCP is created on the
   * server.
   */
  @Test
  public void testRedundancyLevelSpecifiedAndSatisfied() {
    try {
      // TODO: Yogesh
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 2);
      // let the client connect to servers
      //pause(10000);      
      verifyLiveAndRedundantServers(3, 2);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(3, pool.getRedundantNames().size());
      //assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      assertTrue(pool.getRedundantNames().contains(SERVER3));
      assertTrue(pool.getRedundantNames().contains(SERVER4));
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertFalse(pool.getRedundantNames().contains(SERVER2));
      // start server
      server1.invoke(() -> RedundancyLevelTestBase.startServer());
      Wait.pause(1000);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(3, pool.getRedundantNames().size());
      //assertIndexDetailsEquals(4, pool.getConnectedServerCount());
      assertTrue(pool.getRedundantNames().contains(SERVER3));
      assertTrue(pool.getRedundantNames().contains(SERVER4));
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertFalse(pool.getRedundantNames().contains(SERVER2));
      server1.invoke(() -> RedundancyLevelTestBase.verifyNoCCP());

    }
    catch (Exception ex) {
      Assert.fail("test failed due to exception in test noRedundancyLevelServerFail ",
          ex);
    }
  }

  /**
   * Redundancy level not specified, new EP is added then it should be added in
   * live server map as well as failover set and make sure that CCP is created
   * on the server with relevant interest registartion.
   */
  @Test
  public void testRedundancyLevelNotSpecified() {
    try {
      // TODO: Yogesh
      server2.invoke(() -> RedundancyLevelTestBase.stopServer());
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, -1/* not specified */);
      // let the client connect to servers
      //pause(10000);
      verifyLiveAndRedundantServers(3, 2);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(1, pool.getRedundantNames().size());
      //assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      assertFalse(pool.getRedundantNames().contains(SERVER1));
      assertFalse(pool.getRedundantNames().contains(SERVER3));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER4));
      // start server
      server2.invoke(() -> RedundancyLevelTestBase.startServer());
      //pause(10000);
      verifyLiveAndRedundantServers(4, 3);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(1, pool.getRedundantNames().size());
      //assertIndexDetailsEquals(4, pool.getConnectedServerCount());
      //assertTrue(pool.getRedundantNames()
      //    .contains(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      assertTrue(pool.getRedundantNames().contains(SERVER3));
      assertTrue(pool.getRedundantNames().contains(SERVER4));
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
    }
    catch (Exception ex) {
      Assert.fail("test failed due to exception in test noRedundancyLevelServerFail ",
          ex);
    }
  }

  /**
   * Redundancy level specified is more than the total EndPoints. In such situation there should
   * not be any exception & all the EPs should has CacheClientProxy created.
   */
  @Test
  public void testRedundancySpecifiedMoreThanEPs() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 5);
      assertEquals(3, pool.getRedundantNames().size());
      server0.invoke(() -> RedundancyLevelTestBase.verifyCCP());
      server1.invoke(() -> RedundancyLevelTestBase.verifyCCP());
      server2.invoke(() -> RedundancyLevelTestBase.verifyCCP());
      server3.invoke(() -> RedundancyLevelTestBase.verifyCCP());
    }
    catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedMoreThanEPs ",
          ex);
    }
  }

}
