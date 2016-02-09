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

import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;

@SuppressWarnings("serial")
public class HAInterestPart1DUnitTest extends HAInterestTestCase {

  public HAInterestPart1DUnitTest(String name) {
    super(name);
  }

  /**
   * Tests whether interest is registered or not on both primary and secondaries
   */
  public void testInterestRegistrationOnBothPrimaryAndSecondary() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server2.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server3.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    // register K1 and K2
    registerK1AndK2();
    server1.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
    server2.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
    server3.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
  }

  /**
   * Tests whether interest is registered on both primary and secondaries and
   * verify their responses
   */
  public void testInterestRegistrationResponseOnBothPrimaryAndSecondary() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server2.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server3.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    // register interest and verify response
    registerK1AndK2OnPrimaryAndSecondaryAndVerifyResponse();
  }

  /**
   * Tests whether re-registration of interest causes duplicates on server side
   * interest map
   */
  public void testRERegistrationWillNotCreateDuplicateKeysOnServerInterstMaps() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server2.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server3.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    // register multiple times
    reRegisterK1AndK2();

    server1.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
    server2.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
    server3.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
  }

  /**
   * Tests if Primary fails during interest registration should initiate
   * failover and should pick new primary and get server keys in response of
   * registerInterest
   */
  public void testPrimaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server2.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server3.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    // stop primary
    VM oldPrimary = getPrimaryVM();
    stopPrimaryAndRegisterK1AndK2AndVerifyResponse();
    // DSM
    verifyDeadAndLiveServers(1, 2);
    // new primary
    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestTestCase.class, "verifyDispatcherIsAlive");
    newPrimary.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
  }

  /**
   * Tests if Secondary fails during interest registration should add to dead Ep
   * list
   */
  public void testSecondaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server2.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server3.invoke(HAInterestTestCase.class, "createEntriesK1andK2");

    VM primary = getPrimaryVM();
    stopSecondaryAndRegisterK1AndK2AndVerifyResponse();

    verifyDeadAndLiveServers(1, 2);
    // still primary
    primary.invoke(HAInterestTestCase.class, "verifyDispatcherIsAlive");
    primary.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
  }

  /**
   * Tests if Primary and next primary candidate fails during interest
   * registration it should pick new primary from ep list and add these two
   * server to dead ep list and expect serverKeys as a response from
   * registration on newly selected primary
   */
  public void testBothPrimaryAndSecondaryFailureInRegisterInterest() throws Exception {
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server2.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server3.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    // stop server1 and server2
    VM oldPrimary = getPrimaryVM();
    stopBothPrimaryAndSecondaryAndRegisterK1AndK2AndVerifyResponse();

    verifyDeadAndLiveServers(2, 1);
    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestTestCase.class, "verifyDispatcherIsAlive");
    newPrimary.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
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
    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    server1.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server2.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server3.invoke(HAInterestTestCase.class, "createEntriesK1andK2");

    VM oldPrimary = getPrimaryVM();
    stopPrimaryAndRegisterK1AndK2AndVerifyResponse();

    verifyDeadAndLiveServers(1, 2);
    VM newPrimary = getPrimaryVM(oldPrimary);
    newPrimary.invoke(HAInterestTestCase.class, "verifyDispatcherIsAlive");
    newPrimary.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
  }

  /**
   * Tests if DeadServerMonitor on detecting an EP as alive should register
   * client ( create CCP) as welll as register IL
   */
  public void testInterstRegistrationOnRecoveredEPbyDSM() throws Exception {
    IgnoredException.addIgnoredException("SocketException");
    IgnoredException.addIgnoredException("Unexpected IOException");

    createClientPoolCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    createEntriesK1andK2();
    registerK1AndK2();
    server1.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server2.invoke(HAInterestTestCase.class, "createEntriesK1andK2");
    server3.invoke(HAInterestTestCase.class, "createEntriesK1andK2");

    server1.invoke(HAInterestTestCase.class, "stopServer");
    server2.invoke(HAInterestTestCase.class, "stopServer");
    server3.invoke(HAInterestTestCase.class, "stopServer");
    // All servers are dead at this point , no primary in the system.
    verifyDeadAndLiveServers(3, 0);

    // now start one of the servers
    server2.invoke(HAInterestTestCase.class, "startServer");
    verifyDeadAndLiveServers(2, 1);
    // verify that is it primary , and dispatcher is running
    server2.invoke(HAInterestTestCase.class, "verifyDispatcherIsAlive");
    // verify that interest is registered on this recovered EP
    server2.invoke(HAInterestTestCase.class, "verifyInterestRegistration");

    // now start one more server ; this should be now secondary
    server1.invoke(HAInterestTestCase.class, "startServer");
    verifyDeadAndLiveServers(1, 2);

    // verify that is it secondary , dispatcher should not be runnig
    server1.invoke(HAInterestTestCase.class, "verifyDispatcherIsNotAlive");
    // verify that interest is registered on this recovered EP as well
    server1.invoke(HAInterestTestCase.class, "verifyInterestRegistration");

    // now start one more server ; this should be now secondary
    server3.invoke(HAInterestTestCase.class, "startServer");
    verifyDeadAndLiveServers(0, 3);

    // verify that is it secondary , dispatcher should not be runnig
    server3.invoke(HAInterestTestCase.class, "verifyDispatcherIsNotAlive");
    // verify that interest is registered on this recovered EP as well
    server3.invoke(HAInterestTestCase.class, "verifyInterestRegistration");
  }
}
