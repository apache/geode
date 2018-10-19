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
package org.apache.geode.internal.cache.wan.serial;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.MyGatewaySenderEventListener;
import org.apache.geode.internal.cache.wan.MyGatewaySenderEventListener2;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class SerialGatewaySenderEventListenerDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialGatewaySenderEventListenerDUnitTest() {
    super();
  }

  /**
   * Test validates whether the listener attached receives all the events. this test hangs after the
   * Darrel's checkin 36685. Need to work with Darrel.Commenting it out so that test suit will not
   * hang
   */
  @Ignore
  @Test
  public void testGatewaySenderEventListenerInvocationWithoutLocator() {
    int mPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    vm4.invoke(() -> WANTestBase.createCacheWithoutLocator(mPort));
    vm5.invoke(() -> WANTestBase.createCacheWithoutLocator(mPort));
    vm6.invoke(() -> WANTestBase.createCacheWithoutLocator(mPort));
    vm7.invoke(() -> WANTestBase.createCacheWithoutLocator(mPort));

    vm4.invoke(() -> WANTestBase.createSenderWithListener("ln", 2, false, 100, 10, false, false,
        null, false, true));
    vm5.invoke(() -> WANTestBase.createSenderWithListener("ln", 2, false, 100, 10, false, false,
        null, false, true));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    final Map keyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", keyValues.size()));

    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", keyValues.size()));

    vm4.invoke(() -> WANTestBase.printEventListenerMap());
    vm5.invoke(() -> WANTestBase.printEventListenerMap());

    fail("tried to invoke missing method");
    // vm4.invoke(() ->
    // SerialGatewaySenderEventListenerDUnitTest.validateReceivedEventsMapSizeListener1("ln",
    // keyValues ));
  }

  /**
   * Test validates whether the listener attached receives all the events.
   */
  @Test
  public void testGatewaySenderEventListenerInvocation() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));


    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSenderWithListener("ln", 2, false, 100, 10, false, false,
        null, false, true));
    vm5.invoke(() -> WANTestBase.createSenderWithListener("ln", 2, false, 100, 10, false, false,
        null, false, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    final Map keyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", keyValues.size()));

    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", keyValues.size()));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 0));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 0));

    vm4.invoke(() -> SerialGatewaySenderEventListenerDUnitTest
        .validateReceivedEventsMapSizeListener1("ln", keyValues));
  }

  /**
   * Test validates whether the listener attached receives all the events. When there are 2
   * listeners attached to the GatewaySender.
   */
  @Test
  public void testGatewaySender2EventListenerInvocation() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSenderWithListener("ln", 2, false, 100, 10, false, false,
        null, true, true));
    vm5.invoke(() -> WANTestBase.createSenderWithListener("ln", 2, false, 100, 10, false, false,
        null, true, true));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    final Map keyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }


    vm4.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 0));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 0));

    // TODO: move validateReceivedEventsMapSizeListener2 to a shared util class
    vm4.invoke(() -> SerialGatewaySenderEventListenerDUnitTest
        .validateReceivedEventsMapSizeListener2("ln", keyValues));
  }

  /**
   * Test validates whether the PoolImpl is created. Ideally when a listener is attached pool should
   * not be created.
   */
  @Test
  public void testGatewaySenderEventListenerPoolImpl() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4);

    vm4.invoke(() -> WANTestBase.createSenderWithListener("ln", 2, false, 100, 10, false, false,
        null, false, false));

    vm4.invoke(() -> SerialGatewaySenderEventListenerDUnitTest.validateNoPoolCreation("ln"));
  }

  // Test start/stop/resume on listener invocation
  // this test hangs after the Darrel's checkin 36685. Need to work with Darrel.Commenting it out so
  // that test suit will not hang
  @Ignore
  @Test
  public void testGatewaySenderEventListener_GatewayOperations() {

    int mPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    vm4.invoke(() -> WANTestBase.createCacheWithoutLocator(mPort));
    vm5.invoke(() -> WANTestBase.createCacheWithoutLocator(mPort));
    vm6.invoke(() -> WANTestBase.createCacheWithoutLocator(mPort));
    vm7.invoke(() -> WANTestBase.createCacheWithoutLocator(mPort));

    vm4.invoke(() -> WANTestBase.createSenderWithListener("ln", 2, false, 100, 10, false, false,
        null, false, true));

    vm4.invoke(() -> WANTestBase.startSender("ln"));

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    final Map initialKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      initialKeyValues.put(i, i);
    }

    vm4.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", initialKeyValues));

    fail("tried to invoke missing method");
    // vm4.invoke(() ->
    // SerialGatewaySenderEventListenerDUnitTest.validateReceivedEventsMapSizeListener1("ln",
    // initialKeyValues ));

    vm4.invoke(() -> WANTestBase.stopSender("ln"));

    final Map keyValues = new HashMap();
    for (int i = 1000; i < 2000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    fail("tried to invoke missing method");
    // vm4.invoke(() ->
    // SerialGatewaySenderEventListenerDUnitTest.validateReceivedEventsMapSizeListener1("ln",
    // initialKeyValues ));

    vm4.invoke(() -> WANTestBase.startSender("ln"));

    final Map finalKeyValues = new HashMap();
    for (int i = 2000; i < 3000; i++) {
      finalKeyValues.put(i, i);
    }

    vm4.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", finalKeyValues));

    finalKeyValues.putAll(initialKeyValues);
    fail("tried to invoke missing method");
    // vm4.invoke(() ->
    // SerialGatewaySenderEventListenerDUnitTest.validateReceivedEventsMapSizeListener1("ln",
    // finalKeyValues ));

  }

  public static void validateNoPoolCreation(final String siteId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    for (GatewaySender sender : senders) {
      if (sender.getId().equals(siteId)) {
        AbstractGatewaySender sImpl = (AbstractGatewaySender) sender;
        assertNull(sImpl.getProxy());
      }
    }
  }

  public static void validateReceivedEventsMapSizeListener1(final String senderId, final Map map) {

    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    final List<AsyncEventListener> listeners =
        ((AbstractGatewaySender) sender).getAsyncEventListeners();
    if (listeners.size() == 1) {
      final AsyncEventListener l = listeners.get(0);

      WaitCriterion wc = new WaitCriterion() {
        Map listenerMap;

        public boolean done() {
          listenerMap = ((MyGatewaySenderEventListener) l).getEventsMap();
          boolean sizeCorrect = map.size() == listenerMap.size();
          boolean keySetCorrect = listenerMap.keySet().containsAll(map.keySet());
          boolean valuesCorrect = listenerMap.values().containsAll(map.values());
          return sizeCorrect && keySetCorrect && valuesCorrect;
        }

        public String description() {
          return "Waiting for all sites to get updated, the sizes are " + listenerMap.size()
              + " and " + map.size();
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);
    }
  }

  public static void validateReceivedEventsMapSizeListener2(final String senderId, final Map map) {

    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    final List<AsyncEventListener> listeners =
        ((AbstractGatewaySender) sender).getAsyncEventListeners();
    if (listeners.size() == 2) {
      final AsyncEventListener l1 = listeners.get(0);
      final AsyncEventListener l2 = listeners.get(1);
      await().untilAsserted(() -> {
        Map listenerMap1 = ((MyGatewaySenderEventListener) l1).getEventsMap();

        Map listenerMap2 = ((MyGatewaySenderEventListener2) l2).getEventsMap();
        int listener1MapSize = listenerMap1.size();
        int listener2MapSize = listenerMap1.size();
        int expectedMapSize = map.size();
        boolean sizeCorrect = expectedMapSize == listener1MapSize;
        boolean keySetCorrect = listenerMap1.keySet().containsAll(map.keySet());
        boolean valuesCorrect = listenerMap1.values().containsAll(map.values());

        boolean sizeCorrect2 = expectedMapSize == listener2MapSize;
        boolean keySetCorrect2 = listenerMap2.keySet().containsAll(map.keySet());
        boolean valuesCorrect2 = listenerMap2.values().containsAll(map.values());

        assertEquals(
            "Failed while waiting for all sites to get updated with the correct events. \nThe "
                + "size of listener 1's map = " + listener1MapSize
                + "\n The size of listener 2's map = " + "" + listener2MapSize
                + "\n The expected map size =" + expectedMapSize,
            true, sizeCorrect && keySetCorrect && valuesCorrect && sizeCorrect2 && keySetCorrect2
                && valuesCorrect2);
      });
    }
  }
}
