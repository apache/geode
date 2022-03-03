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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@SuppressWarnings("serial")
@Category({ClientSubscriptionTest.class})
public class HAInterestPart1DUnitTest extends HAInterestTestCase {

  public HAInterestPart1DUnitTest() {
    super();
  }

  /**
   * Tests whether interest is registered or not on both primary and secondaries
   */
  @Test
  public void testInterestRegistrationOnBothPrimaryAndSecondary() throws Exception {
    createClientPoolCache(getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase::createEntriesK1andK2);
    server2.invoke(HAInterestTestCase::createEntriesK1andK2);
    server3.invoke(HAInterestTestCase::createEntriesK1andK2);
    // register K1 and K2
    registerK1AndK2();
    server1.invoke(HAInterestTestCase::verifyInterestRegistration);
    server2.invoke(HAInterestTestCase::verifyInterestRegistration);
    server3.invoke(HAInterestTestCase::verifyInterestRegistration);
  }

  /**
   * Tests whether interest is registered on both primary and secondaries and verify their responses
   */
  @Test
  public void testInterestRegistrationResponseOnBothPrimaryAndSecondary() throws Exception {
    createClientPoolCache(getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase::createEntriesK1andK2);
    server2.invoke(HAInterestTestCase::createEntriesK1andK2);
    server3.invoke(HAInterestTestCase::createEntriesK1andK2);
    // register interest and verify response
    registerK1AndK2OnPrimaryAndSecondaryAndVerifyResponse();
  }

  /**
   * Tests whether re-registration of interest causes duplicates on server side interest map
   */
  @Test
  public void testRERegistrationWillNotCreateDuplicateKeysOnServerInterstMaps() throws Exception {
    createClientPoolCache(getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase::createEntriesK1andK2);
    server2.invoke(HAInterestTestCase::createEntriesK1andK2);
    server3.invoke(HAInterestTestCase::createEntriesK1andK2);
    // register multiple times
    reRegisterK1AndK2();

    server1.invoke(HAInterestTestCase::verifyInterestRegistration);
    server2.invoke(HAInterestTestCase::verifyInterestRegistration);
    server3.invoke(HAInterestTestCase::verifyInterestRegistration);
  }

  /**
   * Tests if Primary fails during interest registration should initiate failover and should pick
   * new primary and get server keys in response of registerInterest
   */
  @Test
  public void testPrimaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase::createEntriesK1andK2);
    server2.invoke(HAInterestTestCase::createEntriesK1andK2);
    server3.invoke(HAInterestTestCase::createEntriesK1andK2);
    // stop primary
    VM oldPrimary = getPrimaryVM();
    stopPrimaryAndRegisterK1AndK2AndVerifyResponse();
    // DSM
    verifyDeadAndLiveServers(1, 2);
    // new primary
    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestTestCase::verifyDispatcherIsAlive);
    newPrimary.invoke(HAInterestTestCase::verifyInterestRegistration);
  }

  /**
   * Tests if Secondary fails during interest registration should add to dead Ep list
   */
  @Test
  public void testSecondaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase::createEntriesK1andK2);
    server2.invoke(HAInterestTestCase::createEntriesK1andK2);
    server3.invoke(HAInterestTestCase::createEntriesK1andK2);

    VM primary = getPrimaryVM();
    stopSecondaryAndRegisterK1AndK2AndVerifyResponse();

    verifyDeadAndLiveServers(1, 2);
    // still primary
    primary.invoke(HAInterestTestCase::verifyDispatcherIsAlive);
    primary.invoke(HAInterestTestCase::verifyInterestRegistration);
  }

  /**
   * Tests if Primary and next primary candidate fails during interest registration it should pick
   * new primary from ep list and add these two server to dead ep list and expect serverKeys as a
   * response from registration on newly selected primary
   */
  @Test
  public void testBothPrimaryAndSecondaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase::createEntriesK1andK2);
    server2.invoke(HAInterestTestCase::createEntriesK1andK2);
    server3.invoke(HAInterestTestCase::createEntriesK1andK2);
    // stop server1 and server2
    VM oldPrimary = getPrimaryVM();
    stopBothPrimaryAndSecondaryAndRegisterK1AndK2AndVerifyResponse();

    verifyDeadAndLiveServers(2, 1);
    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestTestCase::verifyDispatcherIsAlive);
    newPrimary.invoke(HAInterestTestCase::verifyInterestRegistration);
  }

  /**
   * Tests if Primary fails during interest registration , it selects new primary from the ep list
   * after making this ep as primary it fails , so interest registration will initiate failover on
   * this ep as well it should pick new primary from ep list and these two server to dead ep list
   * and expect serverKeys as a response from registration on newly selected primary
   *
   */
  @Test
  public void testProbablePrimaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase::createEntriesK1andK2);
    server2.invoke(HAInterestTestCase::createEntriesK1andK2);
    server3.invoke(HAInterestTestCase::createEntriesK1andK2);

    VM oldPrimary = getPrimaryVM();
    stopPrimaryAndRegisterK1AndK2AndVerifyResponse();

    verifyDeadAndLiveServers(1, 2);
    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestTestCase::verifyDispatcherIsAlive);
    newPrimary.invoke(HAInterestTestCase::verifyInterestRegistration);
  }

  /**
   * Tests if DeadServerMonitor on detecting an EP as alive should register client ( create CCP) as
   * welll as register IL
   */
  @Test
  public void testInterstRegistrationOnRecoveredEPbyDSM() throws Exception {
    IgnoredException.addIgnoredException("SocketException");
    IgnoredException.addIgnoredException("Unexpected IOException");

    createClientPoolCache(getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    registerK1AndK2();
    server1.invoke(HAInterestTestCase::createEntriesK1andK2);
    server2.invoke(HAInterestTestCase::createEntriesK1andK2);
    server3.invoke(HAInterestTestCase::createEntriesK1andK2);

    server1.invoke(HAInterestTestCase::stopServer);
    server2.invoke(HAInterestTestCase::stopServer);
    server3.invoke(HAInterestTestCase::stopServer);
    // All servers are dead at this point , no primary in the system.
    verifyDeadAndLiveServers(3, 0);

    // now start one of the servers
    server2.invoke(HAInterestTestCase::startServer);
    verifyDeadAndLiveServers(2, 1);
    // verify that is it primary , and dispatcher is running
    server2.invoke(HAInterestTestCase::verifyDispatcherIsAlive);
    // verify that interest is registered on this recovered EP
    server2.invoke(HAInterestTestCase::verifyInterestRegistration);

    // now start one more server ; this should be now secondary
    server1.invoke(HAInterestTestCase::startServer);
    verifyDeadAndLiveServers(1, 2);

    // verify that is it secondary , dispatcher should not be runnig
    server1.invoke(HAInterestTestCase::verifyDispatcherIsNotAlive);
    // verify that interest is registered on this recovered EP as well
    server1.invoke(HAInterestTestCase::verifyInterestRegistration);

    // now start one more server ; this should be now secondary
    server3.invoke(HAInterestTestCase::startServer);
    verifyDeadAndLiveServers(0, 3);

    // verify that is it secondary , dispatcher should not be runnig
    server3.invoke(HAInterestTestCase::verifyDispatcherIsNotAlive);
    // verify that interest is registered on this recovered EP as well
    server3.invoke(HAInterestTestCase::verifyInterestRegistration);
  }
}
