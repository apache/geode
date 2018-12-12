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

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Tests Redundancy Level Functionality
 */
@Category({ClientSubscriptionTest.class})
public class RedundancyLevelPart3DUnitTest extends RedundancyLevelTestBase {

  @BeforeClass
  public static void caseSetUp() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * This tests failing of a primary server in a situation where the rest of the server are all
   * redundant. After every failure, the order, the dispatcher, the interest registration and the
   * makePrimary calls are verified. The failure detection in these tests could be either through
   * CCU or cache operation, whichever occurs first
   */
  @Test
  public void testRegisterInterestAndMakePrimaryWithFullRedundancy() throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        3);
    createEntriesK1andK2();
    registerK1AndK2();
    assertThat(pool.getRedundantNames().size()).isEqualTo(3);
    server0.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server1.invoke(RedundancyLevelTestBase::verifyDispatcherIsNotAlive);
    server2.invoke(RedundancyLevelTestBase::verifyDispatcherIsNotAlive);
    server3.invoke(RedundancyLevelTestBase::verifyDispatcherIsNotAlive);
    server0.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    server1.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    server3.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = true;
    PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
    PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = true;
    registerInterestCalled = false;
    makePrimaryCalled = false;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void beforeInterestRegistration() {
        registerInterestCalled = true;
      }

      public void beforeInterestRecovery() {
        registerInterestCalled = true;

      }

      public void beforePrimaryIdentificationFromBackup() {
        makePrimaryCalled = true;
      }
    });
    server0.invoke(RedundancyLevelTestBase::stopServer);
    doPuts();
    server1.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server2.invoke(RedundancyLevelTestBase::verifyDispatcherIsNotAlive);
    server3.invoke(RedundancyLevelTestBase::verifyDispatcherIsNotAlive);
    verifyConnectedAndRedundantServers(3, 2);
    assertThat(registerInterestCalled).describedAs(
        "register interest should not have been called since we failed to a redundant server !")
        .isFalse();
    assertThat(makePrimaryCalled).describedAs(
        "make primary should have been called since primary did fail and a new primary was to be chosen ")
        .isTrue();

    assertThat(pool.getRedundantNames().size()).isEqualTo(2);
    makePrimaryCalled = false;
    server1.invoke(RedundancyLevelTestBase::stopServer);
    doPuts();
    server2.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server3.invoke(RedundancyLevelTestBase::verifyDispatcherIsNotAlive);
    verifyConnectedAndRedundantServers(2, 1);
    assertThat(registerInterestCalled).describedAs(
        "register interest should not have been called since we failed to a redundant server !")
        .isFalse();
    assertThat(makePrimaryCalled).describedAs(
        "make primary should have been called since primary did fail and a new primary was to be chosen ")
        .isTrue();

    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    makePrimaryCalled = false;
    server2.invoke(RedundancyLevelTestBase::stopServer);
    doPuts();
    server3.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    verifyConnectedAndRedundantServers(1, 0);

    assertThat(registerInterestCalled).describedAs(
        "register interest should not have been called since we failed to a redundant server !")
        .isFalse();
    assertThat(makePrimaryCalled).describedAs(
        "make primary should have been called since primary did fail and a new primary was to be chosen ")
        .isTrue();

    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    server3.invoke(RedundancyLevelTestBase::stopServer);
    server0.invoke(RedundancyLevelTestBase::startServer);
    verifyConnectedAndRedundantServers(1, 0);
    doPuts();
    server0.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server0.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    if (!registerInterestCalled) {
      Assertions
          .fail("register interest should have been called since a recovered server came up!");
    }
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
    PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
    PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;
  }

  /**
   * This tests failing of a primary server in a situation where the rest of the server are all non
   * redundant. After every failure, the order, the dispatcher, the interest registration and the
   * makePrimary calls are verified. The failure detection in these tests could be either through
   * CCU or cache operation, whichever occurs first
   */
  @Test
  public void testRegisterInterestAndMakePrimaryWithZeroRedundancy() throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        0);
    createEntriesK1andK2();
    registerK1AndK2();
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    server0.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server0.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    server0.invoke(RedundancyLevelTestBase::stopServer);
    verifyConnectedAndRedundantServers(3, 0);
    doPuts();
    server1.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server1.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    server1.invoke(RedundancyLevelTestBase::stopServer);
    verifyConnectedAndRedundantServers(2, 0);
    doPuts();
    server2.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    server2.invoke(RedundancyLevelTestBase::stopServer);
    verifyConnectedAndRedundantServers(1, 0);
    doPuts();
    server3.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server3.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    server3.invoke(RedundancyLevelTestBase::stopServer);
    server0.invoke(RedundancyLevelTestBase::startServer);
    verifyConnectedAndRedundantServers(1, 0);
    doPuts();
    server0.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server0.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);

  }

  /**
   * This tests failing of a primary server in a situation where only one of the rest of the servers
   * is redundant. After every failure, the order, the dispatcher, the interest registration and the
   * makePrimary calls are verified. The failure detection in these tests could be either through
   * CCU or cache operation, whichever occurs first
   */
  @Test
  public void testRegisterInterestAndMakePrimaryWithRedundancyOne() throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    createClientCache(NetworkUtils.getServerHostName(), PORT1, PORT2, PORT3, PORT4,
        1);
    createEntriesK1andK2();
    registerK1AndK2();
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    server0.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server0.invoke(RedundancyLevelTestBase::stopServer);
    doPuts();
    server1.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server2.invoke(RedundancyLevelTestBase::verifyCCP);
    server2.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    verifyConnectedAndRedundantServers(3, 1);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    server1.invoke(RedundancyLevelTestBase::stopServer);
    doPuts();
    server2.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server2.invoke(RedundancyLevelTestBase::stopServer);
    doPuts();
    server3.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server3.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    verifyConnectedAndRedundantServers(1, 0);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    server3.invoke(RedundancyLevelTestBase::stopServer);
    server0.invoke(RedundancyLevelTestBase::startServer);
    verifyConnectedAndRedundantServers(1, 0);
    doPuts();
    server0.invoke(RedundancyLevelTestBase::verifyDispatcherIsAlive);
    server0.invoke(RedundancyLevelTestBase::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
  }
}
