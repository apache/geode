/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;

import dunit.DistributedTestCase;
import dunit.VM;

public class HAInterestPart2DUnitTest extends HAInterestBaseTest
{
  
  /** constructor */
  public HAInterestPart2DUnitTest(String name) {
    super(name);
  }

  /**
   * Tests if Primary fails during interest un registration should initiate failover
   * should pick new primary
   */
  public void testPrimaryFailureInUNregisterInterest()throws Exception{
    createClientPoolCache(this.getName(),getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");

    registerK1AndK2();

    VM oldPrimary = getPrimaryVM();
    stopPrimaryAndUnregisterRegisterK1();

    verifyDeadAndLiveServers(1,2);

    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestBaseTest.class, "verifyDispatcherIsAlive");
    //primary
    newPrimary.invoke(HAInterestBaseTest.class, "verifyInterestUNRegistration");
    //secondary
    getBackupVM().invoke(HAInterestBaseTest.class, "verifyInterestUNRegistration");
  }

  /**
   * Tests if Secondary fails during interest un registration should add to dead Ep list
   */
  public void testSecondaryFailureInUNRegisterInterest()throws Exception{

    createClientPoolCache(this.getName(),getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    registerK1AndK2();
    VM stoppedBackup = stopSecondaryAndUNregisterK1();
    verifyDeadAndLiveServers(1,2);
    //still primary
    getPrimaryVM().invoke(HAInterestBaseTest.class, "verifyDispatcherIsAlive");
    //primary
    getPrimaryVM().invoke(HAInterestBaseTest.class, "verifyInterestUNRegistration");
    //secondary
    getBackupVM(stoppedBackup).invoke(HAInterestBaseTest.class, "verifyInterestUNRegistration");
  }

  /**
   * Tests a scenario in which Dead Server Monitor detects Server Live Just before interest registration
   * then interst should be registerd on the newly detcted live server as well
   * @throws Exception
   */
  public void testDSMDetectsServerLiveJustBeforeInterestRegistration()throws Exception{
    createClientPoolCache(this.getName(),getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    VM backup = getBackupVM();
    backup.invoke(HAInterestBaseTest.class, "stopServer");
    verifyDeadAndLiveServers(1, 2);
    setBridgeObserverForBeforeRegistration(backup);
    try {
      registerK1AndK2();
      waitForBeforeRegistrationCallback();
    } finally {
      unSetBridgeObserverForRegistrationCallback();
    }
    server1.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
    server2.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
    server3.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");

  }
  /**
   * Tests a scenario in which Dead Server Monitor detects Server Live Just After interest registration
   * then interst should be registerd on the newly detcted live server as well
   *
   * @throws Exception
   */
  public void testDSMDetectsServerLiveJustAfterInterestRegistration()throws Exception{

    createClientPoolCache(this.getName(),getServerHostName(server1.getHost()));

    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");

    VM backup = getBackupVM();
    backup.invoke(HAInterestBaseTest.class, "stopServer");
    verifyDeadAndLiveServers(1,2);

    setBridgeObserverForAfterRegistration(backup);
    try {
      registerK1AndK2();
      waitForAfterRegistrationCallback();
    } finally {
      unSetBridgeObserverForRegistrationCallback();
    }

    server1.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
    server2.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
    server3.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");

  }
   /**
    * Tests a Scenario:
    * Only one server, register interst on the server
    * stop server , and update the registered entries on the server
    * start the server , DSM will recover interest list on this live server
    * and verify that as a part of recovery it refreshes
    * registerd entries from the server, because it is primary
    *
    * @throws Exception
    */
 public void testRefreshEntriesFromPrimaryWhenDSMDetectsServerLive()throws Exception{

 PORT1 =  ((Integer) server1.invoke(HAInterestBaseTest.class, "createServerCache")).intValue();
 server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
 createClientPoolCacheConnectionToSingleServer(this.getName(),getServerHostName(server1.getHost()));
 registerK1AndK2();
 verifyRefreshedEntriesFromServer();
    // expected ServerConnectivityException
    final ExpectedException expectedEx = addExpectedException(
        ServerConnectivityException.class.getName());
 server1.invoke(HAInterestBaseTest.class, "stopServer");
 verifyDeadAndLiveServers(1, 0);
 server1.invoke(HAInterestBaseTest.class, "putK1andK2");
 server1.invoke(HAInterestBaseTest.class, "startServer");
 verifyDeadAndLiveServers(0, 1);
 final Region r1 = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
 assertNotNull(r1);   
 
 WaitCriterion wc = new WaitCriterion() {
   String excuse;
   public boolean done() {
     Region.Entry e1;
     Region.Entry e2;

     try {
       e1 = r1.getEntry(k1);
       if (e1 == null) {
         excuse = "Entry for k1 is null";
         return false;
       }
     }
     catch (EntryDestroyedException e) {
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
     }
     catch (EntryDestroyedException e) {
       excuse = "Entry destroyed";
       return false;
     }
     if (!server_k2.equals(e2.getValue())) {
       excuse = "e2 value is not server_k2";
       return false;
     }
     return true;
   }
   public String description() {
     return excuse;
   }
 };
 DistributedTestCase.waitForCriterion(wc, 120 * 1000, 1000, true);

 expectedEx.remove();

 //assertEquals(server_k2, r1.getEntry(k2).getValue());
 //assertEquals(server_k1 ,r1.getEntry(k1).getValue());
}

 /**
    * Tests a Scenario:
    * stop a secondary server and update the registered entries on the stopped server
    * start the server , DSM will recover interest list on this live server
    * and verify that as a part of recovery it does not refreshes
    * registerd entries from the server, because it is secondary
    *
    * @throws Exception
    */
 public void testGIIFromSecondaryWhenDSMDetectsServerLive()throws Exception{

   server1.invoke(HAInterestBaseTest.class, "closeCache");
   server2.invoke(HAInterestBaseTest.class, "closeCache");
   server3.invoke(HAInterestBaseTest.class, "closeCache");

   PORT1 =  ((Integer) server1.invoke(HAInterestBaseTest.class, "createServerCacheWithLocalRegion")).intValue();
   PORT2 =  ((Integer) server2.invoke(HAInterestBaseTest.class, "createServerCacheWithLocalRegion")).intValue();
   PORT3 =  ((Integer) server3.invoke(HAInterestBaseTest.class, "createServerCacheWithLocalRegion")).intValue();

   server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
   server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
   server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");

   createClientPoolCache(this.getName(),getServerHostName(server1.getHost()));

   VM backup1 = getBackupVM();
   VM backup2 = getBackupVM(backup1);
   backup1.invoke(HAInterestBaseTest.class, "stopServer");
   backup2.invoke(HAInterestBaseTest.class, "stopServer");
   verifyDeadAndLiveServers(2, 1);
   registerK1AndK2();
   verifyRefreshedEntriesFromServer();
   backup1.invoke(HAInterestBaseTest.class, "putK1andK2");
   backup1.invoke(HAInterestBaseTest.class, "startServer");
   verifyDeadAndLiveServers(1, 2);
   verifyRefreshedEntriesFromServer();
 }
 /**
  * Bug Test for Bug # 35945
  * A java level Deadlock between acquireConnection and RegionEntry
  * during processRecoveredEndpoint by Dead Server Monitor Thread.
  *
  * @throws Exception
  */
 public void testBug35945()throws Exception{

   PORT1 =  ((Integer) server1.invoke(HAInterestBaseTest.class, "createServerCache")).intValue();
   server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
   createClientPoolCacheConnectionToSingleServer(this.getName(),getServerHostName(server1.getHost()));
   registerK1AndK2();
   verifyRefreshedEntriesFromServer();

   server1.invoke(HAInterestBaseTest.class, "stopServer");
   verifyDeadAndLiveServers(1, 0);
   // put on stopped server
   server1.invoke(HAInterestBaseTest.class, "putK1andK2");
   // spawn a thread to put on server , which will acquire a lock on entry 
   setBridgeObserverForBeforeInterestRecovery();
   server1.invoke(HAInterestBaseTest.class, "startServer");
   verifyDeadAndLiveServers(0, 1);
   waitForBeforeInterestRecoveryCallBack();
   // verify updated value of k1 as a refreshEntriesFromServer
   final Region r1 = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
   assertNotNull(r1);
   
   WaitCriterion wc = new WaitCriterion() {
     String excuse;
     public boolean done() {
       Region.Entry e1 = r1.getEntry(k1);
       Region.Entry e2 = r1.getEntry(k2);
       if (e1 == null || !server_k1_updated.equals(e1.getValue())) {
         excuse="k1="+(e1==null?"null":e1.getValue());
         return false;
       }
       if (e2 == null || !server_k2.equals(e2.getValue())) {
         excuse="k2="+(e2==null?"null":e2.getValue());
         return false;
       }
       return true;
     }
     public String description() {
       return excuse;
     }
   };
   DistributedTestCase.waitForCriterion(wc, 30 * 1000, 1000, true);
  // assertEquals(server_k2, r1.getEntry(k2).getValue());
  // assertEquals(server_k1_updated ,r1.getEntry(k1).getValue());
 }
  /**
   * Tests if failure occured in Interest recovery thread, then it should select new endpoint to
   * register interest
   *
   * @throws Exception
   */
  public void testInterestRecoveryFailure()throws Exception{

   PORT1 =  ((Integer) server1.invoke(HAInterestBaseTest.class, "createServerCache")).intValue();
   server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
   PORT2 =  ((Integer) server2.invoke(HAInterestBaseTest.class, "createServerCache")).intValue();
   server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
   createClientPoolCacheWithSmallRetryInterval(this.getName(),getServerHostName(server1.getHost()));
   registerK1AndK2();
   verifyRefreshedEntriesFromServer();
   VM backup = getBackupVM();
   VM primary = getPrimaryVM();
   cache.getLogger().info("<ExpectedException action=add>" + "Server unreachable" +
       "</ExpectedException>");
   try {
     backup.invoke(HAInterestBaseTest.class, "stopServer");
     primary.invoke(HAInterestBaseTest.class, "stopServer");
     verifyDeadAndLiveServers(2, 0);
   } finally {
     cache.getLogger().info("<ExpectedException action=remove>" + "Server unreachable" +
         "</ExpectedException>");
   }
   primary.invoke(HAInterestBaseTest.class, "putK1andK2");
   setBridgeObserverForBeforeInterestRecoveryFailure();
   primary.invoke(HAInterestBaseTest.class, "startServer");
   waitForBeforeInterestRecoveryCallBack();
   if(exceptionOccured) {
     fail("The DSM could not ensure that server 1 is started & serevr 2 is stopped");
   }
   final Region r1 = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
   assertNotNull(r1);
   //pause(10000);
   WaitCriterion wc = new WaitCriterion() {
     String excuse;
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
     public String description() {
       return excuse;
     }
   };
   DistributedTestCase.waitForCriterion(wc, 2 * 60 * 1000, 1000, true);
 }

}
