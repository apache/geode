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

import static junit.framework.TestCase.assertNotNull;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.EndpointManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Start client 1 Start client 2 Start Server 1 Start Server 2 Register interest for client 1 on
 * Server 1/2 Kill Server 1 Verify that interest fails over to Server 2 Restart Server 1 Do a put
 * which goes against Server 1 Verify that Client 1 does not get the update Verify that Client 2
 * does get the update
 *
 * The key is to verify that the memberid being used by the client to register with the server is
 * the same across servers
 */
@Category({ClientSubscriptionTest.class})
public class UpdatePropagationDUnitTest extends JUnit4CacheTestCase {

  private static final String REGION_NAME = "UpdatePropagationDUnitTest_region";

  private VM server1 = null;
  private VM server2 = null;
  private VM client1 = null;
  private VM client2 = null;

  private int PORT1;
  private int PORT2;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);

    // Server2 VM
    server2 = host.getVM(1);

    // Client 1 VM
    client1 = host.getVM(2);

    // client 2 VM
    client2 = host.getVM(3);

    PORT1 = server1.invoke(() -> createServerCache());
    PORT2 = server2.invoke(() -> createServerCache());

    client1.invoke(
        () -> createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1, PORT2));
    client2.invoke(
        () -> createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1, PORT2));

    IgnoredException.addIgnoredException("java.net.SocketException");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }

  /**
   * This tests whether the updates are received by other clients or not , if there are situation of
   * Interest List fail over
   */
  @Test
  public void updatesAreProgegatedAfterFailover() {
    // First create entries on both servers via the two client
    client1.invoke(() -> createEntriesK1andK2());
    client2.invoke(() -> createEntriesK1andK2());
    client1.invoke(() -> registerKeysK1andK2());
    client2.invoke(() -> registerKeysK1andK2());
    // Induce fail over of InteretsList Endpoint to Server 2 by killing server1
    server1.invoke(() -> killServer(new Integer(PORT1)));
    // Wait for 10 seconds to allow fail over. This would mean that Interstist has failed
    // over to Server2.
    final CacheSerializableRunnable waitToDetectDeadServer =
        new CacheSerializableRunnable("Wait for server on port1 to be dead") {
          public void run2() throws CacheException {
            Region r = getCache().getRegion(REGION_NAME);

            String poolName = r.getAttributes().getPoolName();
            final PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
            await()
                .until(() -> !hasEndPointWithPort(pool, PORT1));
          }
        };
    client1.invoke(waitToDetectDeadServer);
    client2.invoke(waitToDetectDeadServer);

    // Start Server1 again so that both clients1 & Client 2 will establish connection to server1
    // too.
    server1.invoke(() -> startServer(new Integer(PORT1)));

    final CacheSerializableRunnable waitToDetectLiveServer =
        new CacheSerializableRunnable("Wait for servers to be alive") {
          public void run2() throws CacheException {
            Region r = getCache().getRegion(REGION_NAME);
            String poolName = r.getAttributes().getPoolName();
            final PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
            await()
                .until(() -> hasEndPointWithPort(pool, PORT1));
          }
        };
    client1.invoke(waitToDetectLiveServer);
    client2.invoke(waitToDetectLiveServer);

    // Do a put on Server1 via Connection object from client1.
    // Client1 should not receive updated value while client2 should receive
    client1.invoke(
        () -> acquireConnectionsAndPutonK1andK2(NetworkUtils.getServerHostName(client1.getHost())));
    // Check if both the puts ( on key1 & key2 ) have reached the servers
    server1.invoke(() -> verifyUpdates());
    server2.invoke(() -> verifyUpdates());
    // verify updates to other client
    client2.invoke(() -> verifyUpdates());

    // verify no updates for update originator
    client1.invoke(() -> verifySenderUpdateCount());
  }

  /**
   * Check to see if a client is connected to an endpoint with a specific port
   */
  private boolean hasEndPointWithPort(final PoolImpl pool, final int port) {
    EndpointManager endpointManager = pool.getEndpointManager();
    final Set<ServerLocation> servers = endpointManager.getEndpointMap().keySet();
    return servers.stream().anyMatch(location -> location.getPort() == port);
  }

  private void acquireConnectionsAndPutonK1andK2(String host) {
    Region r1 = getCache().getRegion(Region.SEPARATOR + REGION_NAME);
    r1.put("key1", "server-value1");
    r1.put("key2", "server-value2");
  }

  private void killServer(Integer port) {
    Iterator iter = getCache().getCacheServers().iterator();
    if (iter.hasNext()) {
      CacheServer server = (CacheServer) iter.next();
      if (server.getPort() == port.intValue()) {
        server.stop();
      }
    }
  }

  private void startServer(Integer port) throws IOException {
    CacheServer server1 = getCache().addCacheServer();
    server1.setPort(port.intValue());
    server1.setNotifyBySubscription(true);
    server1.start();
  }

  /**
   * Creates entries on the server
   */
  private void createEntriesK1andK2() {
    Region r1 = getCache().getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    if (!r1.containsKey("key1")) {
      r1.put("key1", "key-1");
    }
    if (!r1.containsKey("key2")) {
      r1.put("key2", "key-2");
    }
    assertEquals(r1.get("key1"), "key-1");
    if (r1.getAttributes().getPartitionAttributes() == null) {
      assertEquals(r1.getEntry("key1").getValue(), "key-1");
      assertEquals(r1.getEntry("key2").getValue(), "key-2");
    } else {
      assertEquals(r1.get("key1"), "key-1");
      assertEquals(r1.get("key2"), "key-2");
    }
  }

  private void createClientCache(String host, Integer port1, Integer port2) throws Exception {
    ClientCache cache;
    try {
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.DISABLE_RANDOM", "true");
      int PORT1 = port1.intValue();
      int PORT2 = port2.intValue();
      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");
      ClientCacheFactory cf = new ClientCacheFactory();
      cf.addPoolServer(host, PORT1).addPoolServer(host, PORT2).setPoolSubscriptionEnabled(true)
          .setPoolSubscriptionRedundancy(-1).setPoolMinConnections(4).setPoolSocketBufferSize(1000)
          .setPoolReadTimeout(2000).setPoolPingInterval(300);
      cache = getClientCache(cf);
    } finally {
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.DISABLE_RANDOM", "false");
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .addCacheListener(new EventTrackingCacheListener()).create(REGION_NAME);
  }

  private Integer createServerCache() throws Exception {
    Cache cache = getCache();
    RegionAttributes attrs = createCacheServerAttributes();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  protected RegionAttributes createCacheServerAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    return factory.create();
  }

  private void registerKeysK1andK2() {
    Region r = getCache().getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r);
    List list = new ArrayList();
    list.add("key1");
    list.add("key2");
    r.registerInterest(list);
  }

  private void verifySenderUpdateCount() {
    Region r = getCache().getRegion(Region.SEPARATOR + REGION_NAME);
    EventTrackingCacheListener listener =
        (EventTrackingCacheListener) r.getAttributes().getCacheListeners()[0];

    final List<EntryEvent> events = listener.receivedEvents;

    // We only expect to see 1 create and 1 update from the original put
    assertEquals("Expected only 2 events for key1", 2,
        events.stream().filter(event -> event.getKey().equals("key1")).count());
    assertEquals("Expected only 2 events for key2", 2,
        events.stream().filter(event -> event.getKey().equals("key2")).count());
  }

  private void verifyUpdates() {
    await().untilAsserted(() -> {
      Region r = getCache().getRegion(Region.SEPARATOR + REGION_NAME);
      // verify updates
      if (r.getAttributes().getPartitionAttributes() == null) {
        assertEquals("server-value2", r.getEntry("key2").getValue());
        assertEquals("server-value1", r.getEntry("key1").getValue());
      } else {
        assertEquals("server-value2", r.get("key2"));
        assertEquals("server-value1", r.get("key1"));
      }
    });
  }

  private static class EventTrackingCacheListener extends CacheListenerAdapter {

    List<EntryEvent> receivedEvents = new ArrayList<>();

    @Override
    public void afterCreate(final EntryEvent event) {
      receivedEvents.add(event);
    }

    @Override
    public void afterUpdate(final EntryEvent event) {
      receivedEvents.add(event);
    }

    @Override
    public void afterDestroy(final EntryEvent event) {
      receivedEvents.add(event);
    }
  }
}
