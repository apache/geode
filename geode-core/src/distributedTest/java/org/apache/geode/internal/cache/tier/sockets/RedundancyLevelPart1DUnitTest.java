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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Tests Redundancy Level Functionality
 */
@Category({ClientSubscriptionTest.class})
public class RedundancyLevelPart1DUnitTest extends RedundancyLevelTestBase {

  @BeforeClass
  public static void caseSetUp() throws Exception {
    disconnectAllFromDS();
  }

  private void waitConnectedServers(final int expected) {
    await("Connected server count (" + pool.getConnectedServerCount() + ") never became "
        + expected)
            .until(() -> pool.getConnectedServerCount(), equalTo(expected));
  }

  /**
   * Redundancy level not specified, an EP which dies of should be removed from the fail over set as
   * well as the live server map
   */
  @Test
  public void testRedundancyNotSpecifiedNonPrimaryServerFail() throws Exception {

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        0);
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::stopServer);
    verifyConnectedAndRedundantServers(3, 0);
    verifyOrderOfEndpoints();

    await("pool still contains " + SERVER3)
        .until(() -> !pool.getCurrentServerNames().contains(SERVER3));

  }

  /**
   * Redundancy level not specified. If an EP which dies of is a Primary EP , then the EP should be
   * removed from the live server map, added to dead server map.
   */
  @Test
  public void testRedundancyNotSpecifiedPrimaryServerFails() throws Exception {

    // Asif: Increased the socket read timeout to 3000 sec because the registering
    // of keys was timing out sometimes causing fail over to EP4 causing
    // below assertion to fail
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        0, 3000, 100);
    assertThat(SERVER1).isEqualTo(pool.getPrimaryName());
    verifyOrderOfEndpoints();
    server0.invoke(RedundancyLevelTestBase::stopServer);
    verifyConnectedAndRedundantServers(3, 0);
    verifyOrderOfEndpoints();

    await("pool still contains " + SERVER1)
        .until(() -> !pool.getCurrentServerNames().contains(SERVER1));

    assertThat(pool.getPrimaryName()).isNotEqualTo(SERVER1);
    assertThat(pool.getPrimaryName()).isEqualTo(SERVER2);

  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not part of the fail over
   * list , then it should be removed from Live Server Map & added to dead server map. It should not
   * change the current failover set. Failover detection by LSM
   */
  @Test
  public void testRedundancySpecifiedNonFailoverEPFails() throws Exception {
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::stopServer);
    verifyRedundantServersContain(SERVER2);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not part of the fail over
   * list , then it should be removed from Live Server Map & added to dead server map. It should not
   * change the current failover set. Failover detection by CCU
   */
  @Ignore("TODO")
  @Test
  public void testRedundancySpecifiedNonFailoverEPFailsDetectionByCCU() throws Exception {
    FailOverDetectionByCCU = true;
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 250, 500);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::stopServer);
    verifyRedundantServersContain(SERVER4);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not part of the fail over
   * list , then it should be removed from Live Server Map & added to dead server map. It should not
   * change the current failover set. Failover detection by Register Interest
   */
  @Ignore("TODO")
  @Test
  public void testRedundancySpecifiedNonFailoverEPFailsDetectionByRegisterInterest()
      throws Exception {
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 250, 500);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::stopServer);
    createEntriesK1andK2();
    registerK1AndK2();
    verifyRedundantServersContain(SERVER4);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not part of the fail over
   * list , then it should be removed from Live Server Map & added to dead server map. It should not
   * change the current failover set. Failover detection by Unregister Interest
   */
  @Ignore("TODO")
  @Test
  public void testRedundancySpecifiedNonFailoverEPFailsDetectionByUnregisterInterest()
      throws Exception {
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 250, 500);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(pool.getRedundantNames().contains(SERVER4)).isTrue();
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::stopServer);
    unregisterInterest();
    verifyRedundantServersContain(SERVER4);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not part of the fail over
   * list , then it should be removed from Live Server Map & added to dead server map. It should not
   * change the current failover set. Failover detection by Put operation.
   */
  @Test
  public void testRedundancySpecifiedNonFailoverEPFailsDetectionByPut() throws Exception {
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 500, 1000);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::stopServer);
    doPuts();
    verifyRedundantServersContain(SERVER2);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by LSM.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFails() throws Exception {
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(SERVER1).isEqualTo(pool.getPrimaryName());
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server1.invoke(RedundancyLevelTestBase::stopServer);
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);

  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by CCU.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByCCU() throws Exception {
    FailOverDetectionByCCU = true;
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 250, 500);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(SERVER1).isEqualTo(pool.getPrimaryName());
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server1.invoke(RedundancyLevelTestBase::stopServer);
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);

  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by Register
   * Interest.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByRegisterInterest()
      throws Exception {

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 250, 500);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(SERVER1).isEqualTo(pool.getPrimaryName());
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server1.invoke(RedundancyLevelTestBase::stopServer);
    createEntriesK1andK2();
    registerK1AndK2();
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);

  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by Unregister
   * Interest.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByUnregisterInterest()
      throws Exception {

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 250, 500);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(SERVER1).isEqualTo(pool.getPrimaryName());
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server1.invoke(RedundancyLevelTestBase::stopServer);
    unregisterInterest();
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by Put
   * operation.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByPut() throws Exception {

    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1, 250, 500);
    waitConnectedServers(4);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    assertThat(SERVER1).isEqualTo(pool.getPrimaryName());
    assertThat(pool.getRedundantNames().contains(SERVER2)).isTrue();
    verifyOrderOfEndpoints();
    server1.invoke(RedundancyLevelTestBase::stopServer);
    doPuts();
    System.out.println("server1=" + SERVER1);
    System.out.println("server2=" + SERVER2);
    System.out.println("server3=" + SERVER3);
    System.out.println("server4=" + SERVER4);
    verifyRedundantServersContain(SERVER3);
    verifyConnectedAndRedundantServers(3, 1);
    verifyOrderOfEndpoints();
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);

  }
}
