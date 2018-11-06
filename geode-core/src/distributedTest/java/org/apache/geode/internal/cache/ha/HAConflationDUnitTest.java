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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.ha.HAConflationDUnitTest.LOCK;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.tier.sockets.ConflationDUnitTestHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This is Targetted conflation Dunit test. 1 Client & 1 Server. Slow down the dispatcher. From the
 * unit controller VM using separate invoke every time so that different thread context are created
 * on the server ,test the following 1) Do a create & then update on same key , the client should
 * receive 2 calabcks. 2) Do create , then update & update. The client should receive 2 callbacks ,
 * one for create & one for the last update. 3) Do create , then update, update, invalidate. The
 * client should receive 3 callbacks, one for create one for the last update and one for the
 * invalidate. 4) Do a create , update , update & destroy. The client should receive 3 callbacks (
 * craete , conflated update & destroy).
 */
@Category({ClientSubscriptionTest.class})
public class HAConflationDUnitTest extends JUnit4CacheTestCase {

  VM server1 = null;

  VM client1 = null;

  private static final String regionName = "HAConflationDUnitTest_region";

  static final String KEY1 = "KEY1";

  static final String KEY2 = "KEY2";

  static final String KEY3 = "KEY3";

  static final String VALUE1 = "VALUE1";

  static final String VALUE2 = "VALUE2";

  static final String VALUE3 = "VALUE3";

  static final String LAST_KEY = "lastkey";

  static final String LAST_VALUE = "lastvalue";

  static boolean lastKeyArrived = false;

  static Object LOCK = new Object();

  static int expectedNoEvents = 2;

  static int actualNoEvents = 0;

  public HAConflationDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);

    // Client 1 VM
    client1 = host.getVM(2);

    int PORT1 = ((Integer) server1
        .invoke(() -> HAConflationDUnitTest.createServerCache(new Boolean(false)))).intValue();
    server1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart());
    server1.invoke(() -> HAConflationDUnitTest.makeDispatcherSlow());
    client1
        .invoke(() -> HAConflationDUnitTest.createClientCache(NetworkUtils.getServerHostName(host),
            new Integer(PORT1), new Boolean(true)));
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    client1.invoke(() -> HAConflationDUnitTest.closeCacheAndDisconnect());
    // close server
    server1.invoke(() -> HAConflationDUnitTest.closeCacheAndDisconnect());
  }

  public static void closeCacheAndDisconnect() {
    if (basicGetCache() != null && !basicGetCache().isClosed()) {
      basicGetCache().close();
      basicGetCache().getDistributedSystem().disconnect();
    }
  }

  /**
   * In this test do a create & then update on same key , the client should receive 2 calabcks.
   *
   */

  @Test
  public void testConflationCreateUpdate() throws Exception {
    server1.invoke(putFromServer(KEY1, VALUE1));
    server1.invoke(putFromServer(KEY1, VALUE2));
    server1.invoke(putFromServer(LAST_KEY, LAST_VALUE));
    expectedNoEvents = 2;
    client1.invoke(checkNoEvents(expectedNoEvents));

  }

  /**
   * In this test do create , then update & update. The client should receive 2 callbacks , one for
   * create & one for the last update.
   *
   */
  @Test
  public void testConflationUpdate() throws Exception {

    server1.invoke(putFromServer(KEY1, VALUE1));
    server1.invoke(putFromServer(KEY1, VALUE2));
    server1.invoke(putFromServer(KEY1, VALUE3));
    server1.invoke(putFromServer(KEY1, VALUE1));
    server1.invoke(putFromServer(KEY1, VALUE2));
    server1.invoke(putFromServer(KEY1, VALUE3));
    server1.invoke(putFromServer(KEY1, VALUE1));
    server1.invoke(putFromServer(KEY1, VALUE2));
    server1.invoke(putFromServer(KEY1, VALUE3));
    server1.invoke(putFromServer(LAST_KEY, LAST_VALUE));
    expectedNoEvents = 2;
    client1.invoke(checkNoEvents(expectedNoEvents));

  }

  /**
   * In this test do create , then update, update, invalidate. The client should receive 3
   * callbacks, one for create one for the last update and one for the invalidate.
   *
   */
  @Test
  public void testConflationCreateUpdateInvalidate() throws Exception {

    server1.invoke(putFromServer(KEY1, VALUE1));
    server1.invoke(putFromServer(KEY1, VALUE2));
    server1.invoke(putFromServer(KEY1, VALUE3));
    server1.invoke(invalidateFromServer(KEY1));
    server1.invoke(putFromServer(LAST_KEY, LAST_VALUE));
    expectedNoEvents = 3;
    client1.invoke(checkNoEvents(expectedNoEvents));

  }

  /**
   * In this test do a create , update , update & destroy. The client should receive 3 callbacks (
   * craete , conflated update & destroy).
   *
   */
  @Test
  public void testConflationCreateUpdateDestroy() throws Exception {

    server1.invoke(putFromServer(KEY1, VALUE1));
    server1.invoke(putFromServer(KEY1, VALUE2));
    server1.invoke(putFromServer(KEY1, VALUE3));
    server1.invoke(destroyFromServer(KEY1));
    server1.invoke(putFromServer(LAST_KEY, LAST_VALUE));
    expectedNoEvents = 3;
    client1.invoke(checkNoEvents(expectedNoEvents));

  }

  private CacheSerializableRunnable putFromServer(final String key, final String value) {
    CacheSerializableRunnable performPut = new CacheSerializableRunnable("putFromServer") {
      public void run2() throws CacheException {
        Region region = basicGetCache().getRegion(Region.SEPARATOR + regionName);
        assertNotNull(region);
        basicGetCache().getLogger().info("starting put()");
        region.put(key, value);
        basicGetCache().getLogger().info("finished put()");
      }
    };

    return performPut;
  }

  private CacheSerializableRunnable invalidateFromServer(final String key) {
    CacheSerializableRunnable performInvalidate =
        new CacheSerializableRunnable("invalidateFromServer") {
          public void run2() throws CacheException {
            Region region = basicGetCache().getRegion(Region.SEPARATOR + regionName);
            assertNotNull(region);
            region.invalidate(key);
            basicGetCache().getLogger().info("done invalidate() successfully");

          }
        };

    return performInvalidate;
  }

  private CacheSerializableRunnable destroyFromServer(final String key) {
    CacheSerializableRunnable performDestroy = new CacheSerializableRunnable("performDestroy") {
      public void run2() throws CacheException {
        Region region = basicGetCache().getRegion(Region.SEPARATOR + regionName);
        assertNotNull(region);
        region.destroy(key);
        basicGetCache().getLogger().info("done destroy successfully");

      }
    };

    return performDestroy;
  }

  private CacheSerializableRunnable checkNoEvents(final int expectedEvents) {
    CacheSerializableRunnable checkEvents = new CacheSerializableRunnable("checkEvents") {
      final int interval = 200; // millis

      public void run2() throws CacheException {
        WaitCriterion w = new WaitCriterion() {
          public boolean done() {
            synchronized (LOCK) {

              if (!lastKeyArrived) {
                try {
                  LOCK.wait(interval);
                } catch (InterruptedException e) {
                  fail("interrupted");
                }
              }
            }
            return lastKeyArrived;
          }

          public String description() {
            return "expected " + expectedEvents + " events but received " + actualNoEvents;
          }
        };

        GeodeAwaitility.await().untilAsserted(w);
      }
    };
    return checkEvents;
  }

  public static void makeDispatcherSlow() {

    System.setProperty("slowStartTimeForTesting", "15000");

  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    getCache();
    assertNotNull(basicGetCache());
  }

  public static void createClientCache(String host, Integer port1, Boolean isListenerPresent)
      throws Exception {
    int PORT1 = port1.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAConflationDUnitTest().createCache(props);
    AttributesFactory factory = new AttributesFactory();
    ClientServerTestCase.configureConnectionPool(factory, host, new int[] {PORT1}, true, -1, -1,
        null);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableConflation(true);
    if (isListenerPresent.booleanValue() == true) {
      CacheListener clientListener = new HAClientCountEventListener();
      factory.setCacheListener(clientListener);
    }
    RegionAttributes attrs = factory.create();
    basicGetCache().createRegion(regionName, attrs);
    Region region = basicGetCache().getRegion(Region.SEPARATOR + regionName);
    assertNotNull(region);

    region.registerInterest(KEY1);
    region.registerInterest(KEY2);
    region.registerInterest(KEY3);
    region.registerInterest(LAST_KEY);
    lastKeyArrived = false;
    actualNoEvents = 0;

  }

  public static Integer createServerCache(Boolean isListenerPresent) throws Exception {
    new HAConflationDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    if (isListenerPresent.booleanValue() == true) {
      CacheListener serverListener = new HAClientCountEventListener();
      factory.setCacheListener(serverListener);
    }
    RegionAttributes attrs = factory.create();
    basicGetCache().createRegion(regionName, attrs);
    CacheServerImpl server = (CacheServerImpl) basicGetCache().addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

}


class HAClientCountEventListener implements CacheListener, Declarable {

  public void afterCreate(EntryEvent event) {
    String key = (String) event.getKey();
    if (key.equals(HAConflationDUnitTest.LAST_KEY)) {
      synchronized (HAConflationDUnitTest.LOCK) {
        HAConflationDUnitTest.lastKeyArrived = true;
        HAConflationDUnitTest.LOCK.notifyAll();
      }
    } else {
      HAConflationDUnitTest.actualNoEvents++;
    }

  }

  public void afterUpdate(EntryEvent event) {

    HAConflationDUnitTest.actualNoEvents++;

  }

  public void afterInvalidate(EntryEvent event) {

    HAConflationDUnitTest.actualNoEvents++;

  }

  public void afterDestroy(EntryEvent event) {
    HAConflationDUnitTest.actualNoEvents++;

  }

  public void afterRegionInvalidate(RegionEvent event) {
    // TODO Auto-generated method stub

  }

  public void afterRegionDestroy(RegionEvent event) {
    // TODO Auto-generated method stub

  }

  public void afterRegionClear(RegionEvent event) {
    // TODO Auto-generated method stub

  }

  public void afterRegionCreate(RegionEvent event) {
    // TODO Auto-generated method stub

  }

  public void afterRegionLive(RegionEvent event) {
    // TODO NOT Auto-generated method stub, added by vrao

  }



  public void close() {
    // TODO Auto-generated method stub

  }

  public void init(Properties props) {
    // TODO Auto-generated method stub

  }
}
