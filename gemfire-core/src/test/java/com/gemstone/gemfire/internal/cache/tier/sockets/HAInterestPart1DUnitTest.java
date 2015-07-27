/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import dunit.VM;

@SuppressWarnings("serial")
public class HAInterestPart1DUnitTest extends HAInterestBaseTest {

  public HAInterestPart1DUnitTest(String name) {
    super(name);
  }

  /**
   * Tests whether interest is registered or not on both primary and secondaries
   */
  public void testInterestRegistrationOnBothPrimaryAndSecondary() throws Exception {
    createClientPoolCache(this.getName(), getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    // register K1 and K2
    registerK1AndK2();
    server1.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
    server2.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
    server3.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
  }

  /**
   * Tests whether interest is registered on both primary and secondaries and
   * verify their responses
   */
  public void testInterestRegistrationResponseOnBothPrimaryAndSecondary() throws Exception {
    createClientPoolCache(this.getName(), getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    // register interest and verify response
    registerK1AndK2OnPrimaryAndSecondaryAndVerifyResponse();
  }

  /**
   * Tests whether re-registration of interest causes duplicates on server side
   * interest map
   */
  public void testRERegistrationWillNotCreateDuplicateKeysOnServerInterstMaps() throws Exception {
    createClientPoolCache(this.getName(), getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    // register multiple times
    reRegisterK1AndK2();

    server1.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
    server2.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
    server3.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
  }

  /**
   * Tests if Primary fails during interest registration should initiate
   * failover and should pick new primary and get server keys in response of
   * registerInterest
   */
  public void testPrimaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(this.getName(), getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    // stop primary
    VM oldPrimary = getPrimaryVM();
    stopPrimaryAndRegisterK1AndK2AndVerifyResponse();
    // DSM
    verifyDeadAndLiveServers(1, 2);
    // new primary
    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestBaseTest.class, "verifyDispatcherIsAlive");
    newPrimary.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
  }

  /**
   * Tests if Secondary fails during interest registration should add to dead Ep
   * list
   */
  public void testSecondaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(this.getName(), getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");

    VM primary = getPrimaryVM();
    stopSecondaryAndRegisterK1AndK2AndVerifyResponse();

    verifyDeadAndLiveServers(1, 2);
    // still primary
    primary.invoke(HAInterestBaseTest.class, "verifyDispatcherIsAlive");
    primary.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
  }

  /**
   * Tests if Primary and next primary candidate fails during interest
   * registration it should pick new primary from ep list and add these two
   * server to dead ep list and expect serverKeys as a response from
   * registration on newly selected primary
   */
  public void testBothPrimaryAndSecondaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(this.getName(), getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    // stop server1 and server2
    VM oldPrimary = getPrimaryVM();
    stopBothPrimaryAndSecondaryAndRegisterK1AndK2AndVerifyResponse();

    verifyDeadAndLiveServers(2, 1);
    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestBaseTest.class, "verifyDispatcherIsAlive");
    newPrimary.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
  }

  /**
   * Tests if Primary fails during interest registration , it selects new
   * primary from the ep list after making this ep as primary it fails , so
   * interest registration will initiate failover on this ep as well it should
   * pick new primary from ep list and these two server to dead ep list and
   * expect serverKeys as a response from registration on newly selected primary
   *
   */
  public void testProbablePrimaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(this.getName(), getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");

    VM oldPrimary = getPrimaryVM();
    stopPrimaryAndRegisterK1AndK2AndVerifyResponse();

    verifyDeadAndLiveServers(1, 2);
    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestBaseTest.class, "verifyDispatcherIsAlive");
    newPrimary.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
  }

  /**
   * Tests if DeadServerMonitor on detecting an EP as alive should register
   * client ( create CCP) as welll as register IL
   */
  public void testInterstRegistrationOnRecoveredEPbyDSM() throws Exception {
    addExpectedException("SocketException");

    createClientPoolCache(this.getName(), getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    registerK1AndK2();
    server1.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server2.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");
    server3.invoke(HAInterestBaseTest.class, "createEntriesK1andK2");

    server1.invoke(HAInterestBaseTest.class, "stopServer");
    server2.invoke(HAInterestBaseTest.class, "stopServer");
    server3.invoke(HAInterestBaseTest.class, "stopServer");
    // All servers are dead at this point , no primary in the system.
    verifyDeadAndLiveServers(3, 0);

    // now start one of the servers
    server2.invoke(HAInterestBaseTest.class, "startServer");
    verifyDeadAndLiveServers(2, 1);
    // verify that is it primary , and dispatcher is running
    server2.invoke(HAInterestBaseTest.class, "verifyDispatcherIsAlive");
    // verify that interest is registered on this recovered EP
    server2.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");

    // now start one more server ; this should be now secondary
    server1.invoke(HAInterestBaseTest.class, "startServer");
    verifyDeadAndLiveServers(1, 2);

    // verify that is it secondary , dispatcher should not be runnig
    server1.invoke(HAInterestBaseTest.class, "verifyDispatcherIsNotAlive");
    // verify that interest is registered on this recovered EP as well
    server1.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");

    // now start one more server ; this should be now secondary
    server3.invoke(HAInterestBaseTest.class, "startServer");
    verifyDeadAndLiveServers(0, 3);

    // verify that is it secondary , dispatcher should not be runnig
    server3.invoke(HAInterestBaseTest.class, "verifyDispatcherIsNotAlive");
    // verify that interest is registered on this recovered EP as well
    server3.invoke(HAInterestBaseTest.class, "verifyInterestRegistration");
  }
}
