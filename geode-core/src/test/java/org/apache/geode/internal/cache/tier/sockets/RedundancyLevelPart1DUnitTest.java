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
package org.apache.geode.internal.cache.tier.sockets;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests Redundancy Level Functionality
 */
@Category(DistributedTest.class)
public class RedundancyLevelPart1DUnitTest extends RedundancyLevelTestBase {

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
    Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
  }
  
  /**
   * Redundancy level not specifed, an EP which dies of should be removed from
   * the fail over set as well as the live server map
   */
  @Test
  public void testRedundancyNotSpecifiedNonPrimaryServerFail() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 0);
      verifyOrderOfEndpoints();
      server2.invoke(() -> RedundancyLevelTestBase.stopServer());
      //pause(5000);      
      verifyLiveAndRedundantServers(3, 0);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(1, pool.getRedundantNames().size());
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
//      pause(10 * 1000);
//      assertFalse(pool.getCurrentServerNames().contains(SERVER3));
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          return !pool.getCurrentServerNames().contains(SERVER3);
        }
        public String description() {
          return "pool still contains " + SERVER3;
        }
      };
      Wait.waitForCriterion(wc, 30 * 1000, 1000, true);
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test RedundancyNotSpecifiedNonPrimaryServerFail ",
          ex);
    }
  }

  /**
   * Redundancy level not specified. If an EP which dies of is a Primary EP ,
   * then the EP should be removed from the live server map, added to dead
   * server map. 
   */
  @Test
  public void testRedundancyNotSpecifiedPrimaryServerFails() {
    /*ClientServerObserver oldBo = ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void beforeFailoverByCacheClientUpdater(Endpoint epFailed)
      {
        try{
          Thread.currentThread().sleep(300000);
        }catch(InterruptedException ie) {            
          Thread.currentThread().interrupt();
        }
      }
    });*/
    try {
      //Asif: Increased the socket read timeout to 3000 sec becoz the registering 
      // of keys was timing out sometimes causing fail over to EP4 cozing 
      // below assertion to fail
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 0, 3000, 100);
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      verifyOrderOfEndpoints();
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      verifyLiveAndRedundantServers(3, 0);
      verifyOrderOfEndpoints();
      //assertIndexDetailsEquals(1, pool.getRedundantNames().size());
      //assertIndexDetailsEquals(3, pool.getConnectedServerCount());
//      pause(10 * 1000);
//      assertFalse(pool.getCurrentServerNames().contains(SERVER1));
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          return !pool.getCurrentServerNames().contains(SERVER1); 
        }
        public String description() {
          return "pool still contains " + SERVER1;
        }
      };
      Wait.waitForCriterion(wc, 30 * 1000, 1000, true);
      assertFalse(pool.getPrimaryName().equals(SERVER1));
      assertEquals(SERVER2, pool.getPrimaryName());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test RedundancyNotSpecifiedPrimaryServerFails ",
          ex);
    }/*finally {
      ClientServerObserverHolder.setInstance(oldBo);
    }*/
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not
   * part of the fail over list , then it should be removed from Live Server Map &
   * added to dead server map. It should not change the current failover set.
   * Failover detection by LSM
   */
  @Test
  public void testRedundancySpecifiedNonFailoverEPFails() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1);
      waitConnectedServers(4);
      assertEquals(1, pool.getRedundantNames().size());
      //assertTrue(pool.getRedundantNames().contains(SERVER1));      
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server2.invoke(() -> RedundancyLevelTestBase.stopServer());
      //pause(5000);
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER2);
      verifyLiveAndRedundantServers(3, 1);
      verifyOrderOfEndpoints();
      // assertTrue(proxy.getDeadServers().contains(SERVER3));
      //assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER1));
      //assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedNonFailoverEPFails ",
          ex);
    }
  }
  
  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not
   * part of the fail over list , then it should be removed from Live Server Map &
   * added to dead server map. It should not change the current failover set.
   * Failover detection by CCU
   */
  @Ignore("TODO")
  @Test
  public void testRedundancySpecifiedNonFailoverEPFailsDetectionByCCU() throws Exception {
    FailOverDetectionByCCU = true;
    createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1, 250, 500);
    waitConnectedServers(4);
    assertEquals(1, pool.getRedundantNames().size());
    // assertTrue(pool.getRedundantNames()
    // .contains(SERVER1));
    assertTrue(pool.getRedundantNames().contains(SERVER4));
    //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
    verifyOrderOfEndpoints();
    server2.invoke(() -> RedundancyLevelTestBase.stopServer());
    // pause(5000);
    verifyDeadServers(1);
    verifyRedundantServersContain(SERVER4);
    verifyLiveAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    // assertTrue(proxy.getDeadServers().contains(SERVER3));
    // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
    // assertTrue(pool.getRedundantNames()
    // .contains(SERVER1));
    // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
    //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not
   * part of the fail over list , then it should be removed from Live Server Map &
   * added to dead server map. It should not change the current failover set.
   * Failover detection by Register Interest
   */
  @Ignore("TODO")
  @Test
  public void testRedundancySpecifiedNonFailoverEPFailsDetectionByRegisterInterest() throws Exception {
    createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,250, 500);
    waitConnectedServers(4);
    assertEquals(1, pool.getRedundantNames().size());
    // assertTrue(pool.getRedundantNames()
    // .contains(SERVER1));
    assertTrue(pool.getRedundantNames().contains(SERVER4));
    //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
    verifyOrderOfEndpoints();
    server2.invoke(() -> RedundancyLevelTestBase.stopServer());
    // pause(5000);
    createEntriesK1andK2();
    registerK1AndK2();
    verifyDeadServers(1);
    verifyRedundantServersContain(SERVER4);
    verifyLiveAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    // assertTrue(proxy.getDeadServers().contains(SERVER3));
    // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
    // assertTrue(pool.getRedundantNames()
    // .contains(SERVER1));
    // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
    //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
  }
  
  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not
   * part of the fail over list , then it should be removed from Live Server Map &
   * added to dead server map. It should not change the current failover set.
   * Failover detection by Unregister Interest
   */
  @Ignore("TODO")
  @Test
  public void testRedundancySpecifiedNonFailoverEPFailsDetectionByUnregisterInterest() throws Exception {
    createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,250,500);
    waitConnectedServers(4);
    assertEquals(1, pool.getRedundantNames().size());
    // assertTrue(pool.getRedundantNames()
    // .contains(SERVER1));
    assertTrue(pool.getRedundantNames().contains(SERVER4));
    //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
    verifyOrderOfEndpoints();
    server2.invoke(() -> RedundancyLevelTestBase.stopServer());
    // pause(5000);
    unregisterInterest();
    verifyDeadServers(1);
    verifyRedundantServersContain(SERVER4);
    verifyLiveAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    // assertTrue(proxy.getDeadServers().contains(SERVER3));
    // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
    // assertTrue(pool.getRedundantNames()
    // .contains(SERVER1));
    // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
    //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
  }
  
  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not
   * part of the fail over list , then it should be removed from Live Server Map &
   * added to dead server map. It should not change the current failover set.
   * Failover detection by Put operation.
   */
  @Test
  public void testRedundancySpecifiedNonFailoverEPFailsDetectionByPut() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,500,1000);
      waitConnectedServers(4);
      assertEquals(1, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server2.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      doPuts();
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER2);
      verifyLiveAndRedundantServers(3, 1);
      verifyOrderOfEndpoints();
      // assertTrue(proxy.getDeadServers().contains(SERVER3));
      // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER1));
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
     // assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedNonFailoverEPFailsDetectionByPut ",
          ex);
    }
  }  
  
  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the Live Server
   * Map to compensate for the failure.
   * Failure Detection by LSM.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFails() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1);
      waitConnectedServers(4);
      assertEquals(1, pool.getRedundantNames().size());
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      //pause(5000);
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      //assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      //      assertTrue(pool.getRedundantNames()
      //          .contains(SERVER1));
      verifyOrderOfEndpoints();
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      //assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedNonFailoverEPFails ",
          ex);
    }
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the Live Server
   * Map to compensate for the failure.
   * Failure Detection by CCU.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByCCU() {
    try {
      
      FailOverDetectionByCCU = true;
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1, 250, 500);
      waitConnectedServers(4);
      assertEquals(1, pool.getRedundantNames().size());
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER1));
      verifyOrderOfEndpoints();
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedNonPrimaryEPFailsDetectionByCCU ",
          ex);
    }
  }
  
  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the Live Server
   * Map to compensate for the failure.
   * Failure Detection by Register Interest.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByRegisterInterest() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,250, 500);
      waitConnectedServers(4);
      assertEquals(1, pool.getRedundantNames().size());
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      createEntriesK1andK2();
      registerK1AndK2();
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER1));
      verifyOrderOfEndpoints();
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedNonPrimaryEPFailsDetectionByRegisterInterest ",
          ex);
    }
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the Live Server
   * Map to compensate for the failure.
   * Failure Detection by Unregister Interest.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByUnregisterInterest() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,250,500);
      waitConnectedServers(4);
      assertEquals(1, pool.getRedundantNames().size());
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      unregisterInterest();
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER1));
      verifyOrderOfEndpoints();
      //server1.invoke(RedundancyLevelTestBase.class,
      //    "verifyInterestRegistration");
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedNonPrimaryEPFailsDetectionByUnregisterInterest ",
          ex);
    }
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part
   * of the fail over list , then it should be removed from live server map &
   * added to dead server map. A new EP should be picked from the Live Server
   * Map to compensate for the failure.
   * Failure Detection by Put operation.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByPut() {
    try {
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1,250,500);
      waitConnectedServers(4);
      assertEquals(1, pool.getRedundantNames().size());
      assertTrue(pool.getPrimaryName().equals(SERVER1));
      assertTrue(pool.getRedundantNames().contains(SERVER2));
      //assertIndexDetailsEquals(0, proxy.getDeadServers().size());
      verifyOrderOfEndpoints();
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      // pause(5000);
      doPuts();
      System.out.println("server1="+SERVER1);
      System.out.println("server2="+SERVER2);
      System.out.println("server3="+SERVER3);
      System.out.println("server4="+SERVER4);
      verifyDeadServers(1);
      verifyRedundantServersContain(SERVER3);
      verifyLiveAndRedundantServers(3, 1);
      // assertIndexDetailsEquals(2, pool.getRedundantNames().size());
      // assertTrue(pool.getRedundantNames()
      // .contains(SERVER1));
      verifyOrderOfEndpoints();
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      // assertIndexDetailsEquals(3, pool.getConnectedServerCount());
      //assertIndexDetailsEquals(1, proxy.getDeadServers().size());
    }
    catch (Exception ex) {
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedNonPrimaryEPFailsDetectionByPut ",
          ex);
    }
  }  
  

  
}
