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

import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * Tests Redundancy Level Functionality
 */
@Category(DistributedTest.class)
public class RedundancyLevelPart3DUnitTest extends RedundancyLevelTestBase {

  @BeforeClass
  public static void caseSetUp() throws Exception {
    disconnectAllFromDS();
  }
  
  /**
   * This tests failing of a primary server in a situation where teh rest of the server are all redundant.
   * After every failure, the order, the dispatcher, the interest registration and the makePrimary calls
   * are verified. The failure detection in these tests could be either through CCU or cache operation,
   * whichever occurs first
   */
  @Test
  public void testRegisterInterestAndMakePrimaryWithFullRedundancy() {
    try {
      CacheServerTestUtil.disableShufflingOfEndpoints();
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 3);
      createEntriesK1andK2();
      registerK1AndK2();
      assertEquals(3, pool.getRedundantNames().size());
      server0.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server1.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsNotAlive());
      server2.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsNotAlive());
      server3.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsNotAlive());
      server0.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      server1.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      server3.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = true;
      PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
      PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = true;
      registerInterestCalled = false;
      makePrimaryCalled = false;
      ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
        public void beforeInterestRegistration()
        {
          registerInterestCalled = true;
        }
        public void beforeInterestRecovery()
        {
          registerInterestCalled = true;
          
        }
        public void beforePrimaryIdentificationFromBackup()
        {
          makePrimaryCalled = true;
        }
      });
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      doPuts();
      server1.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server2.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsNotAlive());
      server3.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsNotAlive());
      verifyLiveAndRedundantServers(3, 2);
      if(registerInterestCalled){
        fail("register interest should not have been called since we failed to a redundant server !");
      }
      if(!makePrimaryCalled){
        fail("make primary should have been called since primary did fail and a new primary was to be chosen ");
      }
      assertEquals(2, pool.getRedundantNames().size());
      makePrimaryCalled = false;
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      doPuts();
      server2.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server3.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsNotAlive());
      verifyLiveAndRedundantServers(2, 1);
      if(registerInterestCalled){
        fail("register interest should not have been called since we failed to a redundant server !");
      }
      if(!makePrimaryCalled){
        fail("make primary should have been called since primary did fail and a new primary was to be chosen ");
      }
      assertEquals(1, pool.getRedundantNames().size());
      makePrimaryCalled = false;
      server2.invoke(() -> RedundancyLevelTestBase.stopServer());
      doPuts();
      server3.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      verifyLiveAndRedundantServers(1, 0);
      if(registerInterestCalled){
        fail("register interest should not have been called since we failed to a redundant server !");
      }
      if(!makePrimaryCalled){
        fail("make primary should have been called since primary did fail and a new primary was to be chosen ");
      }
      assertEquals(0, pool.getRedundantNames().size());
      server3.invoke(() -> RedundancyLevelTestBase.stopServer());
      server0.invoke(() -> RedundancyLevelTestBase.startServer());
      doPuts();
      server0.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server0.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      verifyLiveAndRedundantServers(1, 0);
      if(!registerInterestCalled){
        fail("register interest should have been called since a recovered server came up!");
      }
      assertEquals(0, pool.getRedundantNames().size());
      PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
      PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
      PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;
    }
     catch (Exception ex) {
        ex.printStackTrace();
        Assert.fail(
            "test failed due to exception in test testRedundancySpecifiedMoreThanEPs ",
            ex);
     }
   }
  
  /**
   * This tests failing of a primary server in a situation where the rest of the server are all non redundant.
   * After every failure, the order, the dispatcher, the interest registration and the makePrimary calls
   * are verified. The failure detection in these tests could be either through CCU or cache operation,
   * whichever occurs first
   */
  @Test
  public void testRegisterInterestAndMakePrimaryWithZeroRedundancy() {
    try {
      CacheServerTestUtil.disableShufflingOfEndpoints();
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 0);
      createEntriesK1andK2();
      registerK1AndK2();
      assertEquals(0, pool.getRedundantNames().size());
      server0.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server0.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      doPuts();
      server1.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server1.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      verifyLiveAndRedundantServers(3, 0);
      assertEquals(0, pool.getRedundantNames().size());
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      doPuts();
      server2.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      verifyLiveAndRedundantServers(2, 0);
      assertEquals(0, pool.getRedundantNames().size());
      server2.invoke(() -> RedundancyLevelTestBase.stopServer());
      doPuts();
      server3.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server3.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      verifyLiveAndRedundantServers(1, 0);
      assertEquals(0, pool.getRedundantNames().size());
      server3.invoke(() -> RedundancyLevelTestBase.stopServer());
      server0.invoke(() -> RedundancyLevelTestBase.startServer());
      doPuts();
      server0.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server0.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      verifyLiveAndRedundantServers(1, 0);
      assertEquals(0, pool.getRedundantNames().size());
    }
     catch (Exception ex) {
        ex.printStackTrace();
        Assert.fail(
            "test failed due to exception in test testRedundancySpecifiedMoreThanEPs ",
            ex);
     }
   }
  
  /**
   * This tests failing of a primary server in a situation where only one of the rest of teh servers is redundant.
   * After every failure, the order, the dispatcher, the interest registration and the makePrimary calls
   * are verified. The failure detection in these tests could be either through CCU or cache operation,
   * whichever occurs first
   */
  @Test
  public void testRegisterInterestAndMakePrimaryWithRedundancyOne() {
    try {
//      long maxWaitTime = 60000;
      CacheServerTestUtil.disableShufflingOfEndpoints();
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1);
      createEntriesK1andK2();
      registerK1AndK2();
      assertEquals(1, pool.getRedundantNames().size());
      server0.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server0.invoke(() -> RedundancyLevelTestBase.stopServer());
      doPuts();
      server1.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server2.invoke(() -> RedundancyLevelTestBase.verifyCCP());
      server2.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      verifyLiveAndRedundantServers(3, 1);
      assertEquals(1, pool.getRedundantNames().size());
      server1.invoke(() -> RedundancyLevelTestBase.stopServer());
      doPuts();
      server2.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server2.invoke(() -> RedundancyLevelTestBase.stopServer());
      doPuts();
      server3.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server3.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      verifyLiveAndRedundantServers(1, 0);
      assertEquals(0, pool.getRedundantNames().size());
      server3.invoke(() -> RedundancyLevelTestBase.stopServer());
      server0.invoke(() -> RedundancyLevelTestBase.startServer());
      doPuts();
      server0.invoke(() -> RedundancyLevelTestBase.verifyDispatcherIsAlive());
      server0.invoke(() -> RedundancyLevelTestBase.verifyInterestRegistration());
      verifyLiveAndRedundantServers(1, 0);
      assertEquals(0, pool.getRedundantNames().size());
     }
    catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail(
          "test failed due to exception in test testRedundancySpecifiedMoreThanEPs ",
          ex);
    }
  }

  
}
