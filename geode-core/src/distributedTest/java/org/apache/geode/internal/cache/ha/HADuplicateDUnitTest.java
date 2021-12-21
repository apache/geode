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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This is the Dunit test to verify the duplicates after the fail over The test perorms following
 * operations 1. Create 2 servers and 1 client 2. Perform put operations for knows set of keys
 * directy from the server1. 3. Stop the server1 so that fail over happens 4. Validate the
 * duplicates received by the client1
 */
@Category({ClientSubscriptionTest.class})
public class HADuplicateDUnitTest extends JUnit4DistributedTestCase {

  VM server1 = null;

  VM server2 = null;

  VM client1 = null;

  VM client2 = null;

  private static final String REGION_NAME = "HADuplicateDUnitTest_Region";

  protected static Cache cache = null;

  static boolean isEventDuplicate = true;

  static CacheServerImpl server = null;

  static final int NO_OF_PUTS = 100;

  public static final Object dummyObj = "dummyObject";

  static boolean waitFlag = true;

  static int put_counter = 0;

  static Map storeEvents = new HashMap();

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);

    server1 = host.getVM(0);

    server2 = host.getVM(1);

    client1 = host.getVM(2);

    client2 = host.getVM(3);
  }

  @Override
  public final void preTearDown() throws Exception {
    client1.invoke(() -> HADuplicateDUnitTest.closeCache());
    // close server
    server1.invoke(() -> HADuplicateDUnitTest.reSetQRMslow());
    server1.invoke(() -> HADuplicateDUnitTest.closeCache());
    server2.invoke(() -> HADuplicateDUnitTest.closeCache());
  }

  @Ignore("TODO")
  @Test
  public void testDuplicate() throws Exception {
    createClientServerConfiguration();
    server1.invoke(putForKnownKeys());
    server1.invoke(stopServer());

    // wait till all the duplicates are received by client
    client1.invoke(new CacheSerializableRunnable("waitForPutToComplete") {

      @Override
      public void run2() throws CacheException {
        synchronized (dummyObj) {
          while (waitFlag) {
            try {
              dummyObj.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }

        if (waitFlag) {
          fail("test failed");
        }
      }
    });

    // validate the duplicates received by client
    client1.invoke(new CacheSerializableRunnable("validateDuplicates") {
      @Override
      public void run2() throws CacheException {
        if (!isEventDuplicate) {
          fail(" Not all duplicates received");
        }

      }
    });

    server1.invoke(() -> HADuplicateDUnitTest.reSetQRMslow());
  }

  @Test
  public void testSample() throws Exception {
    IgnoredException.addIgnoredException("IOException");
    IgnoredException.addIgnoredException("Connection reset");
    createClientServerConfiguration();
    server1.invoke(new CacheSerializableRunnable("putKey") {

      @Override
      public void run2() throws CacheException {
        Region region = cache.getRegion(SEPARATOR + REGION_NAME);
        assertNotNull(region);
        region.put("key1", "value1");

      }
    });
  }

  // function to perform put operations for the known set of keys.
  private CacheSerializableRunnable putForKnownKeys() {

    CacheSerializableRunnable putforknownkeys = new CacheSerializableRunnable("putforknownkeys") {

      @Override
      public void run2() throws CacheException {
        Region region = cache.getRegion(SEPARATOR + REGION_NAME);
        assertNotNull(region);
        for (int i = 0; i < NO_OF_PUTS; i++) {
          region.put("key" + i, "value" + i);
        }
      }

    };

    return putforknownkeys;
  }

  // function to stop server so that the fail over happens
  private CacheSerializableRunnable stopServer() {

    CacheSerializableRunnable stopserver = new CacheSerializableRunnable("stopServer") {
      @Override
      public void run2() throws CacheException {
        server.stop();
      }

    };

    return stopserver;
  }

  // function to create 2servers and 1 clients
  private void createClientServerConfiguration() {
    int PORT1 =
        server1.invoke(() -> HADuplicateDUnitTest.createServerCache()).intValue();
    server1.invoke(() -> HADuplicateDUnitTest.setQRMslow());
    int PORT2 =
        server2.invoke(() -> HADuplicateDUnitTest.createServerCache()).intValue();
    String hostname = NetworkUtils.getServerHostName(Host.getHost(0));
    client1.invoke(() -> HADuplicateDUnitTest.createClientCache(hostname, new Integer(PORT1),
        new Integer(PORT2)));

  }

  // function to set QRM slow
  public static void setQRMslow() {
    System.setProperty("QueueRemovalThreadWaitTime", "100000");
  }

  public static void reSetQRMslow() {
    System.setProperty("QueueRemovalThreadWaitTime", "1000");
  }

  public static Integer createServerCache() throws Exception {
    new HADuplicateDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    server = (CacheServerImpl) cache.addCacheServer();
    assertNotNull(server);
    int port = getRandomAvailableTCPPort();
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String hostName, Integer port1, Integer port2)
      throws Exception {
    int PORT1 = port1.intValue();
    int PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HADuplicateDUnitTest().createCache(props);
    AttributesFactory factory = new AttributesFactory();
    ClientServerTestCase.configureConnectionPool(factory, hostName, new int[] {PORT1, PORT2}, true,
        -1, 2, null);

    factory.setScope(Scope.DISTRIBUTED_ACK);
    CacheListener clientListener = new HAValidateDuplicateListener();
    factory.setCacheListener(clientListener);

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);

  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  // Listener class for the validation purpose
  private static class HAValidateDuplicateListener extends CacheListenerAdapter {
    @Override
    public void afterCreate(EntryEvent event) {
      System.out.println("After Create");
      storeEvents.put(event.getKey(), event.getNewValue());
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      Object value = storeEvents.get(event.getKey());
      if (value == null) {
        isEventDuplicate = false;
      }
      synchronized (dummyObj) {
        try {
          put_counter++;
          if (put_counter == NO_OF_PUTS) {
            waitFlag = false;
            dummyObj.notifyAll();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

    }
  }
}
