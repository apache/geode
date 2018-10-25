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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class RedundancyLevelPart2DUnitTest extends RedundancyLevelTestBase {

  @BeforeClass
  public static void caseSetUp() {
    disconnectAllFromDS();
  }

  private void waitConnectedServers(final int expected) {
    await("Connected server count (" + pool.getConnectedServerCount() + ") never became "
        + expected)
            .until(() -> pool.getConnectedServerCount(), equalTo(expected));
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the live server map to compensate for the failure. The EP failed also happened to
   * be a Primary EP so a new EP from fail over set should become the primary and make sure that CCP
   * is created on the server with relevant interest registration. Failure Detection by LSM
   */
  @Test
  public void testRedundancySpecifiedPrimaryEPFails() throws Exception {

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1);
    waitConnectedServers(4);
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();

    verifyOrderOfEndpoints();
    server0.invoke(RedundancyLevelTestBase::stopServer);
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();

    assertThat(pool.getPrimaryName().equals(SERVER2)).isTrue();
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the live server map to compensate for the failure. The EP failed also happened to
   * be a Primary EP so a new EP from fail over set should become the primary and make sure that CCP
   * is created on the server with relevant interest registration. Failure Detection by CCU
   */
  @Test
  public void testRedundancySpecifiedPrimaryEPFailsDetectionByCCU() throws Exception {

    FailOverDetectionByCCU = true;
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 3000, 100);
    waitConnectedServers(4);
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server0.invoke(RedundancyLevelTestBase::stopServer);
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    assertThat(pool.getPrimaryName().equals(SERVER2)).isTrue();

  }

  /*
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the live server map to compensate for the failure. The EP failed also happened to
   * be a Primary EP so a new EP from fail over set should become the primary and make sure that CCP
   * is created on the server with relevant interest registration. Failure Detection by Register
   * Interest
   */
  @Test
  public void testRedundancySpecifiedPrimaryEPFailsDetectionByRegisterInterest() throws Exception {

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 3000, 100);
    waitConnectedServers(4);
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server0.invoke(RedundancyLevelTestBase::stopServer);
    createEntriesK1andK2();
    registerK1AndK2();
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    assertThat(pool.getPrimaryName().equals(SERVER2)).isTrue();

  }

  /*
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the live server map to compensate for the failure. The EP failed also happened to
   * be a Primary EP so a new EP from fail over set should become the primary and make sure that CCP
   * is created on the server with relevant interest registration. Failure Detection by Unregister
   * Interest
   */
  @Test
  public void testRedundancySpecifiedPrimaryEPFailsDetectionByUnregisterInterest()
      throws Exception {

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 3000, 100);
    waitConnectedServers(4);
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server0.invoke(RedundancyLevelTestBase::stopServer);

    unregisterInterest();
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();

    assertThat(pool.getPrimaryName().equals(SERVER2)).isTrue();

  }

  /*
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the live server map to compensate for the failure. The EP failed also happened to
   * be a Primary EP so a new EP from fail over set should become the primary and make sure that CCP
   * is created on the server with relevant interest registration. Failure Detection by put
   * operation
   */
  @Test
  public void testRedundancySpecifiedPrimaryEPFailsDetectionByPut() throws Exception {

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 3000, 100);
    waitConnectedServers(4);
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();

    verifyOrderOfEndpoints();
    server0.invoke(RedundancyLevelTestBase::stopServer);

    doPuts();
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    assertThat(pool.getPrimaryName().equals(SERVER2)).isTrue();
  }

  /*
   * If there are 4 servers in Live Server Map with redundancy level as 1. Kill the primary &
   * secondary. Two Eps should be added from the live server & the existing EP in the set should be
   * the new primary and make sure that CCP is created on both the server with relevant interest
   * registration.
   */
  @Test
  public void testRedundancySpecifiedPrimarySecondaryEPFails() throws Exception {
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER3)).isFalse();
    assertThat(pool.getRedundantNames().contains(SERVER4)).isFalse();
    verifyOrderOfEndpoints();
    server0.invoke(RedundancyLevelTestBase::stopServer);
    server1.invoke(RedundancyLevelTestBase::stopServer);
    verifyConnectedAndRedundantServers(2, 1);
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    server3.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
  }

  /**
   * There are 4 Eps in Live server Map with redundancy level as 2. Kill two Eps (excluding the
   * primary). As a result live server map will contain 2 , active list will contain two & dead
   * server map will contain 2. Redundancy is unsatisfied by 1. Bring up one EP . The DSM should add
   * the alive EP to the active end point & live server map with primary unchanged. Also make sure
   * that CCP is created on the server with relevant interest registration. Bringing the 4th EP
   * alive should simply add it to Live server map.
   */
  @Test
  public void testRedundancySpecifiedEPFails() throws Exception {

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        2);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(2);
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER3)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER4)).isFalse();
    // kill non primary EPs
    verifyOrderOfEndpoints();
    server1.invoke(RedundancyLevelTestBase::stopServer);
    server2.invoke(RedundancyLevelTestBase::stopServer);
    verifyConnectedAndRedundantServers(2, 1);
    verifyOrderOfEndpoints();
    server1.invoke(RedundancyLevelTestBase::startServer);
    verifyConnectedAndRedundantServers(3, 2);
    verifyOrderOfEndpoints();
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    verifyRedundantServersContain(SERVER2);
    verifyRedundantServersContain(SERVER4);
    server1.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    // bring up another server should get added to live server map only and
    // not to the active server as redundancy level is satisfied.
    server2.invoke(RedundancyLevelTestBase::startServer);
    Wait.pause(1000);
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::verifyNoCCP);

  }

  /**
   * Redundancy level specified but not satisfied, new EP is added then it should be added in Live
   * server map as well as failover set and make sure that CCP is created on the server with
   * relevant interest registration.
   */
  @Test
  public void testRedundancyLevelSpecifiedButNotSatisfied() throws Exception {
    // stop two secondaries
    server2.invoke(RedundancyLevelTestBase::stopServer);
    server1.invoke(RedundancyLevelTestBase::stopServer);

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        2);

    verifyConnectedAndRedundantServers(2, 1);
    verifyOrderOfEndpoints();

    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER3)).isFalse();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isFalse();
    // start server
    server2.invoke(RedundancyLevelTestBase::startServer);

    verifyConnectedAndRedundantServers(3, 2);
    verifyOrderOfEndpoints();

    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER3)).isTrue();
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isFalse();
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    // verify that redundancy level is satisfied
    server1.invoke(RedundancyLevelTestBase::startServer);
    Wait.pause(1000);
    verifyOrderOfEndpoints();

    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER3)).isTrue();
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isFalse();
    server1.invoke(RedundancyLevelTestBase::verifyNoCCP);


  }

  /**
   * Redundancy level specified and satisfied, new EP is added then it should be added only in Live
   * server map and make sure that no CCP is created on the server.
   */
  @Test
  public void testRedundancyLevelSpecifiedAndSatisfied() throws Exception {
    server1.invoke(RedundancyLevelTestBase::stopServer);
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        2);

    verifyConnectedAndRedundantServers(3, 2);
    verifyOrderOfEndpoints();
    assertThat(pool.getRedundantNames().contains(SERVER3)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isFalse();
    // start server
    server1.invoke(RedundancyLevelTestBase::startServer);
    Wait.pause(1000);
    verifyOrderOfEndpoints();
    assertThat(pool.getRedundantNames().contains(SERVER3)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isFalse();
    server1.invoke(RedundancyLevelTestBase::verifyNoCCP);

  }

  /**
   * Redundancy level not specified, new EP is added then it should be added in live server map as
   * well as failover set and make sure that CCP is created on the server with relevant interest
   * registration.
   */
  @Test
  public void testRedundancyLevelNotSpecified() throws Exception {

    server2.invoke(RedundancyLevelTestBase::stopServer);
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        -1/* not specified */);

    verifyConnectedAndRedundantServers(3, 2);
    verifyOrderOfEndpoints();

    assertThat(pool.getRedundantNames().contains(SERVER1)).isFalse();
    assertThat(pool.getRedundantNames().contains(SERVER3)).isFalse();
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    // start server
    server2.invoke(RedundancyLevelTestBase::startServer);
    verifyConnectedAndRedundantServers(4, 3);
    verifyOrderOfEndpoints();

    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER3)).isTrue();
    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    assertThat(pool.getPrimaryName().equals(SERVER1)).isTrue();
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);

  }

  /**
   * Redundancy level specified is more than the total EndPoints. In such situation there should not
   * be any exception & all the EPs should has CacheClientProxy created.
   */
  @Test
  public void testRedundancySpecifiedMoreThanEPs() throws Exception {
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        5);
    assertThat(pool.getRedundantNames().size()).isEqualTo(3);
    server0.invoke(RedundancyLevelTestBase::verifyCCP);
    server1.invoke(RedundancyLevelTestBase::verifyCCP);
    server2.invoke(RedundancyLevelTestBase::verifyCCP);
    server3.invoke(RedundancyLevelTestBase::verifyCCP);

  }

}
