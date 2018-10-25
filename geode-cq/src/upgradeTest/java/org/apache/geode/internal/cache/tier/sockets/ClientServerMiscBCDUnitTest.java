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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({ClientServerTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClientServerMiscBCDUnitTest extends ClientServerMiscDUnitTestBase {
  @Parameterized.Parameters
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  public ClientServerMiscBCDUnitTest(String version) {
    super();
    testVersion = version;
  }

  @Override
  void createClientCacheAndVerifyPingIntervalIsSet(String host, int port) throws Exception {
    // this functionality was introduced in 1.5. If we let the test run in older
    // clients it will throw a NoSuchMethodError
    if (Version.CURRENT_ORDINAL >= 80 /* GEODE_150 */) {
      super.createClientCacheAndVerifyPingIntervalIsSet(host, port);
    }
  }

  @Test
  public void testSubscriptionWithCurrentServerAndOldClients() throws Exception {
    // start server first
    int serverPort = initServerCache(true);
    VM client1 = Host.getHost(0).getVM(testVersion, 1);
    VM client2 = Host.getHost(0).getVM(testVersion, 3);
    String hostname = NetworkUtils.getServerHostName(Host.getHost(0));
    client1.invoke("create client1 cache", () -> {
      createClientCache(hostname, serverPort);
      populateCache();
      registerInterest();
    });
    client2.invoke("create client2 cache", () -> {
      Pool ignore = createClientCache(hostname, serverPort);
    });

    client2.invoke("putting data in client2", () -> putForClient());

    // client1 will receive client2's updates asynchronously
    client1.invoke(() -> {
      Region r2 = getCache().getRegion(REGION_NAME2);
      MemberIDVerifier verifier = (MemberIDVerifier) ((LocalRegion) r2).getCacheListener();
      await().until(() -> verifier.eventReceived);
    });

    // client2's update should have included a memberID - GEODE-2954
    client1.invoke(() -> {
      Region r2 = getCache().getRegion(REGION_NAME2);
      MemberIDVerifier verifier = (MemberIDVerifier) ((LocalRegion) r2).getCacheListener();
      assertFalse(verifier.memberIDNotReceived);
    });
  }

  @Test
  public void testSubscriptionWithMixedServersAndNewPeerFeed() throws Exception {
    doTestSubscriptionWithMixedServersAndPeerFeed(VersionManager.CURRENT_VERSION, true);
  }

  @Test
  public void testSubscriptionWithMixedServersAndOldPeerFeed() throws Exception {
    doTestSubscriptionWithMixedServersAndPeerFeed(testVersion, true);
  }

  @Test
  public void testSubscriptionWithMixedServersAndOldClientFeed() throws Exception {
    doTestSubscriptionWithMixedServersAndPeerFeed(testVersion, false);
  }

  private void doTestSubscriptionWithMixedServersAndPeerFeed(String version,
      boolean usePeerForFeed) {
    server1 = Host.getHost(0).getVM(testVersion, 2);
    server2 = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 3);
    VM server3 = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 4);
    VM interestClient = Host.getHost(0).getVM(testVersion, 0);
    VM feeder = Host.getHost(0).getVM(version, 1);

    // start servers first
    int server1Port = initServerCache(true);

    int server2Port = initServerCache2();

    int server3Port = server3.invoke(() -> createServerCache(true, getMaxThreads(), false));

    System.out.println("old server is vm 2 and new server is vm 3");
    System.out
        .println("old server port is " + server1Port + " and new server port is " + server2Port);

    String hostname = NetworkUtils.getServerHostName(Host.getHost(0));
    interestClient.invoke("create interestClient cache", () -> {
      createClientCache(hostname, 300000, false, server1Port, server2Port, server3Port);
      populateCache();
      registerInterest();
    });

    if (!usePeerForFeed) {
      feeder.invoke("create client cache for feed", () -> {
        Pool ignore = createClientCache(hostname, server1Port);
      });
    }
    feeder.invoke("putting data in feeder", () -> putForClient());

    // interestClient will receive feeder's updates asynchronously
    interestClient.invoke("verification 1", () -> {
      Region r2 = getCache().getRegion(REGION_NAME2);
      MemberIDVerifier verifier = (MemberIDVerifier) ((LocalRegion) r2).getCacheListener();
      await().until(() -> verifier.eventReceived);
      verifier.reset();
    });

    server1.invoke("shutdown old server", () -> {
      getCache().getDistributedSystem().disconnect();
    });

    server2.invoke("wait for failover queue to drain", () -> {
      CacheClientProxy proxy =
          CacheClientNotifier.getInstance().getClientProxies().iterator().next();
      await()
          .until(() -> proxy.getHARegionQueue().isEmpty());
    });

    // the client should now get duplicate events from the current-version server
    interestClient.invoke("verification 2", () -> {
      Cache cache = getCache();
      Region r2 = cache.getRegion(REGION_NAME2);
      MemberIDVerifier verifier = (MemberIDVerifier) ((LocalRegion) r2).getCacheListener();
      assertFalse(verifier.eventReceived); // no duplicate events should have arrived
      PoolImpl pool = (PoolImpl) PoolManager.find("ClientServerMiscDUnitTestPool");

      Map seqMap = pool.getThreadIdToSequenceIdMap();
      assertEquals(3, seqMap.size()); // one for each server and one for the feed
      verifier.reset();
    });

    server2.invoke("shutdown new server", () -> {
      getCache().getDistributedSystem().disconnect();
    });

    server3.invoke("wait for failover queue to drain", () -> {
      CacheClientProxy proxy =
          CacheClientNotifier.getInstance().getClientProxies().iterator().next();
      await()
          .until(() -> proxy.getHARegionQueue().isEmpty());
    });

    // the client should now get duplicate events from the current-version server
    interestClient.invoke("verification 3", () -> {
      Cache cache = getCache();
      Region r2 = cache.getRegion(REGION_NAME2);
      MemberIDVerifier verifier = (MemberIDVerifier) ((LocalRegion) r2).getCacheListener();
      assertFalse(verifier.eventReceived); // no duplicate events should have arrived
      PoolImpl pool = (PoolImpl) PoolManager.find("ClientServerMiscDUnitTestPool");

      Map seqMap = pool.getThreadIdToSequenceIdMap();
      assertEquals(4, seqMap.size()); // one for each server and one for the feed
    });
  }

  @Test
  public void giiEventQueueFromOldToCurrentMemberShouldSucceed() {
    giiEventQueueShouldSucceedWithMixedVersions(testVersion, VersionManager.CURRENT_VERSION);
  }

  @Test
  public void giiEventQueueFromCurrentToOldMemberShouldSucceed() {
    giiEventQueueShouldSucceedWithMixedVersions(VersionManager.CURRENT_VERSION, testVersion);

  }

  public void giiEventQueueShouldSucceedWithMixedVersions(String server1Version,
      String server2Version) {
    VM interestClient = Host.getHost(0).getVM(testVersion, 0);
    VM feeder = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 1);
    server1 = Host.getHost(0).getVM(server1Version, 2);
    server2 = Host.getHost(0).getVM(server2Version, 3);

    // start servers first
    int server1Port = initServerCache(true, server1, true);
    int server2Port = initServerCache(true, server2, true);
    server2.invoke(() -> {
      getCache().getCacheServers().stream().forEach(CacheServer::stop);
    });


    String hostname = NetworkUtils.getServerHostName(Host.getHost(0));
    interestClient.invoke("create interestClient cache", () -> {
      createClientCache(hostname, 300000, false, server1Port, server2Port);
      registerInterest();
      registerCQ();
    });

    feeder.invoke("putting data in feeder", () -> putForClient());

    // Start server 2
    server2.invoke(() -> {
      for (CacheServer server : getCache().getCacheServers()) {
        server.start();
      }
    });

    // Make sure server 2 copies the queue
    server2.invoke(() -> {
      await().untilAsserted(() -> {
        final Collection<CacheClientProxy> clientProxies =
            CacheClientNotifier.getInstance().getClientProxies();
        assertFalse(clientProxies.isEmpty());
        CacheClientProxy proxy = clientProxies.iterator().next();
        assertFalse(proxy.getHARegionQueue().isEmpty());
      });
    });

    // interestClient will receive feeder's updates asynchronously
    interestClient.invoke("verification 1", () -> {
      Region r2 = getCache().getRegion(REGION_NAME2);
      MemberIDVerifier verifier = (MemberIDVerifier) ((LocalRegion) r2).getCacheListener();
      await().until(() -> verifier.eventReceived);
      verifier.reset();
    });

    server1.invoke("shutdown old server", () -> {
      getCache().getDistributedSystem().disconnect();
    });

    server2.invoke("wait for failover queue to drain", () -> {
      CacheClientProxy proxy =
          CacheClientNotifier.getInstance().getClientProxies().iterator().next();
      await()
          .until(() -> proxy.getHARegionQueue().isEmpty());
    });
  }

  public static void registerCQ() throws Exception {
    Cache cache = new ClientServerMiscDUnitTestBase().getCache();
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
    assertNotNull(r);
    CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
    cqAttributesFactory.addCqListener(Mockito.mock(CqListener.class));
    final CqQuery cq = cache.getQueryService().newCq("testCQ", "select * from " + r.getFullPath(),
        cqAttributesFactory.create());
    cq.execute();
  }

  @Test
  public void testDistributedMemberBytesWithCurrentServerAndOldClient() throws Exception {
    // Start current version server
    int serverPort = initServerCache(true);

    // Start old version client and do puts
    VM client = Host.getHost(0).getVM(testVersion, 1);
    String hostname = NetworkUtils.getServerHostName(Host.getHost(0));
    client.invoke("create client cache", () -> {
      createClientCache(hostname, serverPort);
      populateCache();
    });

    // Get client member id byte array on client
    byte[] clientMembershipIdBytesOnClient =
        client.invoke(() -> getClientMembershipIdBytesOnClient());

    // Get client member id byte array on server
    byte[] clientMembershipIdBytesOnServer =
        server1.invoke(() -> getClientMembershipIdBytesOnServer());

    // Verify member id bytes on client and server are equal
    String complaint = "size on client=" + clientMembershipIdBytesOnClient.length
        + "; size on server=" + clientMembershipIdBytesOnServer.length + "\nclient bytes="
        + Arrays.toString(clientMembershipIdBytesOnClient) + "\nserver bytes="
        + Arrays.toString(clientMembershipIdBytesOnServer);
    assertTrue(complaint,
        Arrays.equals(clientMembershipIdBytesOnClient, clientMembershipIdBytesOnServer));
  }

  private byte[] getClientMembershipIdBytesOnClient() {
    DistributedSystem system = getCache().getDistributedSystem();
    byte[] result =
        EventID.getMembershipId(new ClientProxyMembershipID(system.getDistributedMember()));
    System.out.println("client ID bytes are " + Arrays.toString(result));
    return result;
  }

  private byte[] getClientMembershipIdBytesOnServer() {
    Set cpmIds = ClientHealthMonitor.getInstance().getClientHeartbeats().keySet();
    assertEquals(1, cpmIds.size());
    ClientProxyMembershipID cpmId = (ClientProxyMembershipID) cpmIds.iterator().next();
    System.out.println("client ID on server is " + cpmId.getDistributedMember());
    byte[] result = EventID.getMembershipId(cpmId);
    System.out.println("client ID bytes are " + Arrays.toString(result));
    return result;
  }
}
