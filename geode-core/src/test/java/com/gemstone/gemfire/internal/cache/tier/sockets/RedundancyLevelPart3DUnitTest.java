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

import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;

/**
 * Tests Redundancy Level Functionality
 * 
 * @author Suyog Bhokare
 * 
 */
public class RedundancyLevelPart3DUnitTest extends RedundancyLevelTestBase
{
    /** constructor */
  public RedundancyLevelPart3DUnitTest(String name) {
    super(name);
  }
  
  public static void caseSetUp() throws Exception {
    DistributedTestCase.disconnectAllFromDS();
  }
  
  /**
   * This tests failing of a primary server in a situation where teh rest of the server are all redundant.
   * After every failure, the order, the dispatcher, the interest registration and the makePrimary calls
   * are verified. The failure detection in these tests could be either through CCU or cache operation,
   * whichever occurs first
   *
   */
  public void testRegisterInterestAndMakePrimaryWithFullRedundancy()
  {
    try {
      CacheServerTestUtil.disableShufflingOfEndpoints();
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 3);
      createEntriesK1andK2();
      registerK1AndK2();
      assertEquals(3, pool.getRedundantNames().size());
      server0.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server1.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsNotAlive");
      server2.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsNotAlive");
      server3.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsNotAlive");
      server0.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
      server1.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
      server2.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
      server3.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
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
      server0.invoke(RedundancyLevelTestBase.class, "stopServer");
      doPuts();
      server1.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server2.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsNotAlive");
      server3.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsNotAlive");
      verifyLiveAndRedundantServers(3, 2);
      if(registerInterestCalled){
        fail("register interest should not have been called since we failed to a redundant server !");
      }
      if(!makePrimaryCalled){
        fail("make primary should have been called since primary did fail and a new primary was to be chosen ");
      }
      assertEquals(2, pool.getRedundantNames().size());
      makePrimaryCalled = false;
      server1.invoke(RedundancyLevelTestBase.class, "stopServer");
      doPuts();
      server2.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server3.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsNotAlive");
      verifyLiveAndRedundantServers(2, 1);
      if(registerInterestCalled){
        fail("register interest should not have been called since we failed to a redundant server !");
      }
      if(!makePrimaryCalled){
        fail("make primary should have been called since primary did fail and a new primary was to be chosen ");
      }
      assertEquals(1, pool.getRedundantNames().size());
      makePrimaryCalled = false;
      server2.invoke(RedundancyLevelTestBase.class, "stopServer");
      doPuts();
      server3.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      verifyLiveAndRedundantServers(1, 0);
      if(registerInterestCalled){
        fail("register interest should not have been called since we failed to a redundant server !");
      }
      if(!makePrimaryCalled){
        fail("make primary should have been called since primary did fail and a new primary was to be chosen ");
      }
      assertEquals(0, pool.getRedundantNames().size());
      server3.invoke(RedundancyLevelTestBase.class, "stopServer");
      server0.invoke(RedundancyLevelTestBase.class, "startServer");
      doPuts();
      server0.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server0.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
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
   *
   */
  
  public void testRegisterInterestAndMakePrimaryWithZeroRedundancy()
  {
    try {
      CacheServerTestUtil.disableShufflingOfEndpoints();
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 0);
      createEntriesK1andK2();
      registerK1AndK2();
      assertEquals(0, pool.getRedundantNames().size());
      server0.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server0.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
      server0.invoke(RedundancyLevelTestBase.class, "stopServer");
      doPuts();
      server1.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server1.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
      verifyLiveAndRedundantServers(3, 0);
      assertEquals(0, pool.getRedundantNames().size());
      server1.invoke(RedundancyLevelTestBase.class, "stopServer");
      doPuts();
      server2.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server2.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
      verifyLiveAndRedundantServers(2, 0);
      assertEquals(0, pool.getRedundantNames().size());
      server2.invoke(RedundancyLevelTestBase.class, "stopServer");
      doPuts();
      server3.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server3.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
      verifyLiveAndRedundantServers(1, 0);
      assertEquals(0, pool.getRedundantNames().size());
      server3.invoke(RedundancyLevelTestBase.class, "stopServer");
      server0.invoke(RedundancyLevelTestBase.class, "startServer");
      doPuts();
      server0.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server0.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
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
   *
   */
  public void testRegisterInterestAndMakePrimaryWithRedundancyOne()
  {
    try {
//      long maxWaitTime = 60000;
      CacheServerTestUtil.disableShufflingOfEndpoints();
      createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1, PORT2, PORT3, PORT4, 1);
      createEntriesK1andK2();
      registerK1AndK2();
      assertEquals(1, pool.getRedundantNames().size());
      server0.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server0.invoke(RedundancyLevelTestBase.class, "stopServer");
      doPuts();
      server1.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server2.invoke(RedundancyLevelTestBase.class, "verifyCCP");
      server2.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
      verifyLiveAndRedundantServers(3, 1);
      assertEquals(1, pool.getRedundantNames().size());
      server1.invoke(RedundancyLevelTestBase.class, "stopServer");
      doPuts();
      server2.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server2.invoke(RedundancyLevelTestBase.class, "stopServer");
      doPuts();
      server3.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server3.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
      verifyLiveAndRedundantServers(1, 0);
      assertEquals(0, pool.getRedundantNames().size());
      server3.invoke(RedundancyLevelTestBase.class, "stopServer");
      server0.invoke(RedundancyLevelTestBase.class, "startServer");
      doPuts();
      server0.invoke(RedundancyLevelTestBase.class, "verifyDispatcherIsAlive");
      server0.invoke(RedundancyLevelTestBase.class, "verifyInterestRegistration");
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
