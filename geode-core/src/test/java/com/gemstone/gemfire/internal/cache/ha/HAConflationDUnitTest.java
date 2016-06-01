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
package com.gemstone.gemfire.internal.cache.ha;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.test.dunit.*;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;

/**
 * This is Targetted conflation Dunit test.
 *  1 Client & 1 Server.
 *  Slow down the dispatcher.
 *  From the unit controller VM using separate invoke every time so that different thread
 *  context are created on the server ,test the following
 *  1) Do a create & then update on same key , the client should receive 2 calabcks.
 *  2) Do create , then update & update. The client should receive 2 callbacks , one for create & one for the last update.
 *  3) Do create , then update, update, invalidate. The client should receive 3 callbacks, one for create one for the last update
 *     and one for the invalidate.
 *  4) Do a create , update , update & destroy. The client should receive 3 callbacks ( craete , conflated update & destroy).
 *
 *
 */

public class HAConflationDUnitTest extends CacheTestCase
{

  VM server1 = null;

  VM client1 = null;

  private static final String regionName = "HAConflationDUnitTest_region";

  final static String KEY1 = "KEY1";

  final static String KEY2 = "KEY2";

  final static String KEY3 = "KEY3";

  final static String VALUE1 = "VALUE1";

  final static String VALUE2 = "VALUE2";

  final static String VALUE3 = "VALUE3";

  final static String LAST_KEY = "lastkey";

  static final String LAST_VALUE = "lastvalue";

  static boolean lastKeyArrived = false;

  static Object LOCK = new Object();

  static int expectedNoEvents = 2;

  static int actualNoEvents = 0;

  public HAConflationDUnitTest(String name) {
    super(name);
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);

    // Client 1 VM
    client1 = host.getVM(2);

    int PORT1 = ((Integer)server1.invoke(() -> HAConflationDUnitTest.createServerCache( new Boolean(false) ))).intValue();
    server1.invoke(() -> ConflationDUnitTest.setIsSlowStart());
    server1.invoke(() -> HAConflationDUnitTest.makeDispatcherSlow());
    client1.invoke(() -> HAConflationDUnitTest.createClientCache( NetworkUtils.getServerHostName(host), new Integer(PORT1), new Boolean(true) ));
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    client1.invoke(() -> HAConflationDUnitTest.closeCacheAndDisconnect());
    // close server
    server1.invoke(() -> HAConflationDUnitTest.closeCacheAndDisconnect());
  }
  
  public static void closeCacheAndDisconnect()
  {
    if (basicGetCache() != null && !basicGetCache().isClosed()) {
      basicGetCache().close();
      basicGetCache().getDistributedSystem().disconnect();
    }
  }

  /**
   * In this test do a create & then update on same key ,
   * the client should receive 2 calabcks.
   * @throws Exception
   */

  public void testConflationCreateUpdate() throws Exception
  {
    server1.invoke(putFromServer(KEY1, VALUE1));
    server1.invoke(putFromServer(KEY1, VALUE2));
    server1.invoke(putFromServer(LAST_KEY, LAST_VALUE));
    expectedNoEvents = 2;
    client1.invoke(checkNoEvents(expectedNoEvents));

  }

  /**
   * In this test do create , then update & update.
   * The client should receive 2 callbacks , one for create & one for the last update.
   * @throws Exception
   */
  public void testConflationUpdate() throws Exception
  {

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
   * In this test do create , then update, update, invalidate.
   * The client should receive 3 callbacks, one for create one for the last update
   * and one for the invalidate.
   * @throws Exception
   */
  public void testConflationCreateUpdateInvalidate() throws Exception
  {

    server1.invoke(putFromServer(KEY1, VALUE1));
    server1.invoke(putFromServer(KEY1, VALUE2));
    server1.invoke(putFromServer(KEY1, VALUE3));
    server1.invoke(invalidateFromServer(KEY1));
    server1.invoke(putFromServer(LAST_KEY, LAST_VALUE));
    expectedNoEvents = 3;
    client1.invoke(checkNoEvents(expectedNoEvents));

  }

  /**
   * In this test do a create , update , update & destroy.
   * The client should receive 3 callbacks ( craete , conflated update & destroy).
   * @throws Exception
   */
  public void testConflationCreateUpdateDestroy() throws Exception
  {

    server1.invoke(putFromServer(KEY1, VALUE1));
    server1.invoke(putFromServer(KEY1, VALUE2));
    server1.invoke(putFromServer(KEY1, VALUE3));
    server1.invoke(destroyFromServer(KEY1));
    server1.invoke(putFromServer(LAST_KEY, LAST_VALUE));
    expectedNoEvents = 3;
    client1.invoke(checkNoEvents(expectedNoEvents));

  }

  private CacheSerializableRunnable putFromServer(final String key,
      final String value)
  {
    CacheSerializableRunnable performPut = new CacheSerializableRunnable(
        "putFromServer") {
      public void run2() throws CacheException
      {
        Region region = basicGetCache().getRegion(Region.SEPARATOR + regionName);
        assertNotNull(region);
        basicGetCache().getLogger().info("starting put()");
        region.put(key, value);
        basicGetCache().getLogger().info("finished put()");
      }
    };

    return performPut;
  }

  private CacheSerializableRunnable invalidateFromServer(final String key)
  {
    CacheSerializableRunnable performInvalidate = new CacheSerializableRunnable(
        "invalidateFromServer") {
      public void run2() throws CacheException
      {
        Region region = basicGetCache().getRegion(Region.SEPARATOR + regionName);
        assertNotNull(region);
        region.invalidate(key);
        basicGetCache().getLogger().info("done invalidate() successfully");

      }
    };

    return performInvalidate;
  }

  private CacheSerializableRunnable destroyFromServer(final String key)
  {
    CacheSerializableRunnable performDestroy = new CacheSerializableRunnable(
        "performDestroy") {
      public void run2() throws CacheException
      {
        Region region = basicGetCache().getRegion(Region.SEPARATOR + regionName);
        assertNotNull(region);
        region.destroy(key);
        basicGetCache().getLogger().info("done destroy successfully");

      }
    };

    return performDestroy;
  }

  private CacheSerializableRunnable checkNoEvents(final int expectedEvents)
  {
    CacheSerializableRunnable checkEvents = new CacheSerializableRunnable(
        "checkEvents") {
      final int interval = 200; // millis
      public void run2() throws CacheException {
          WaitCriterion w = new WaitCriterion() {
            public boolean done() {
              synchronized (HAConflationDUnitTest.LOCK) {

                if (!lastKeyArrived) {
                  try {
                    LOCK.wait(interval);
                  }
                  catch (InterruptedException e) {
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
          
          Wait.waitForCriterion(w, 3 * 60 * 1000, interval, true);
      }
    };
    return checkEvents;
  }

  public static void makeDispatcherSlow()
  {

    System.setProperty("slowStartTimeForTesting", "15000");

  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    getCache();
    assertNotNull(basicGetCache());
  }

  public static void createClientCache(String host, Integer port1, Boolean isListenerPresent)
      throws Exception
  {
    int PORT1 = port1.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAConflationDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    ClientServerTestCase.configureConnectionPool(factory, host, new int[] { PORT1 }, true, -1, -1, null);
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

  public static Integer createServerCache(Boolean isListenerPresent)
      throws Exception
  {
    new HAConflationDUnitTest("temp").createCache(new Properties());
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
    CacheServerImpl server = (CacheServerImpl)basicGetCache().addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

}

class HAClientCountEventListener implements
    CacheListener, Declarable
{

  public void afterCreate(EntryEvent event)
  {
    String key = (String)event.getKey();
    if (key.equals(HAConflationDUnitTest.LAST_KEY)) {
      synchronized (HAConflationDUnitTest.LOCK) {
        HAConflationDUnitTest.lastKeyArrived = true;
        HAConflationDUnitTest.LOCK.notifyAll();
      }
    }
    else {
      HAConflationDUnitTest.actualNoEvents++;
    }

  }

  public void afterUpdate(EntryEvent event)
  {

    HAConflationDUnitTest.actualNoEvents++;

  }

  public void afterInvalidate(EntryEvent event)
  {

    HAConflationDUnitTest.actualNoEvents++;

  }

  public void afterDestroy(EntryEvent event)
  {
    HAConflationDUnitTest.actualNoEvents++;

  }

  public void afterRegionInvalidate(RegionEvent event)
  {
    // TODO Auto-generated method stub

  }

  public void afterRegionDestroy(RegionEvent event)
  {
    // TODO Auto-generated method stub

  }
  public void afterRegionClear(RegionEvent event)
  {
    // TODO Auto-generated method stub

  }
  public void afterRegionCreate(RegionEvent event)
  {
    // TODO Auto-generated method stub

  }
  public void afterRegionLive(RegionEvent event)
  {
    // TODO NOT Auto-generated method stub, added by vrao

  }



  public void close()
  {
    // TODO Auto-generated method stub

  }

  public void init(Properties props)
  {
    // TODO Auto-generated method stub

  }
}
