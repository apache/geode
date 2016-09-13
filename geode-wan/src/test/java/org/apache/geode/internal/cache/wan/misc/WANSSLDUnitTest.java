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
package org.apache.geode.internal.cache.wan.misc;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class WANSSLDUnitTest extends WANTestBase{

  public WANSSLDUnitTest() {
    super();
  }

  @Test
  public void testSenderSSLReceiverSSL(){
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiverWithSSL( nyPort ));

    vm4.invoke(() -> WANTestBase.createCacheWithSSL( lnPort ));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
  }
  
  @Test
  public void testSenderNoSSLReceiverSSL() {
    IgnoredException.addIgnoredException("Unexpected IOException");
    IgnoredException.addIgnoredException("SSL Error");
    IgnoredException.addIgnoredException("Unrecognized SSL message");
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
      Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

      vm2.invoke(() -> WANTestBase.createReceiverWithSSL( nyPort ));

      vm4.invoke(() -> WANTestBase.createCache( lnPort ));

      vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
          false, 100, 10, false, false, null, true ));

      vm2.invoke(() -> WANTestBase.createReplicatedRegion(
          getTestMethodName() + "_RR", null, isOffHeap() ));

      vm4.invoke(() -> WANTestBase.startSender( "ln" ));

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(
          getTestMethodName() + "_RR", "ln", isOffHeap() ));

      vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
          1000 ));

      vm2.invoke(() -> WANTestBase.validateRegionSize(
          getTestMethodName() + "_RR", 1000 ));
      fail("Expected exception as only Receiver is SSL enabled. Not Sender");
    }
    catch (Exception e) {
      assertTrue(e.getCause().getMessage().contains("Server expecting SSL connection"));
    }
  }
  
  @Test
  public void testSenderSSLReceiverNoSSL(){
    IgnoredException.addIgnoredException("Acceptor received unknown");
    IgnoredException.addIgnoredException("failed accepting client");
    IgnoredException.addIgnoredException("Error in connecting to peer");
    IgnoredException.addIgnoredException("Remote host closed connection during handshake");
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
      Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(() -> WANTestBase.createReceiver());

      vm4.invoke(() -> WANTestBase.createCacheWithSSL( lnPort ));

      vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
          false, 100, 10, false, false, null, true ));

      vm2.invoke(() -> WANTestBase.createReplicatedRegion(
          getTestMethodName() + "_RR", null, isOffHeap() ));

      vm4.invoke(() -> WANTestBase.startSender( "ln" ));

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(
          getTestMethodName() + "_RR", "ln", isOffHeap() ));

      vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
          1 ));

      Boolean doesSizeMatch = (Boolean)vm2.invoke(() -> WANSSLDUnitTest.ValidateSSLRegionSize(
          getTestMethodName() + "_RR", 1 ));
      
      assertFalse(doesSizeMatch);
  }
  
  public static boolean ValidateSSLRegionSize (String regionName, final int regionSize) {
      final Region r = cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(r);
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          return false;
        }

        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(wc, 2000, 500, false);
      
      if(r.size() == regionSize){
        return true;
      }
      return false;
  }
}
