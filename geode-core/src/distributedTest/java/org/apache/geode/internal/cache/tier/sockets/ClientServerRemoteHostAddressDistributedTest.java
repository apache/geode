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

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class ClientServerRemoteHostAddressDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Test
  public void testRemoteHostAddress() throws Exception {
    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start server
    int locatorPort = locator.getPort();
    String regionName = testName.getMethodName() + "_region";
    MemberVM server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withRegion(RegionShortcut.PARTITION, regionName));

    // Connect client 1
    ClientVM client1 =
        cluster.startClientVM(2, getDurableClientProperties(testName.getMethodName() + "_1"),
            (ccf) -> {
              ccf.setPoolSubscriptionEnabled(true);
              ccf.addPoolLocator("localhost", locatorPort);
            });

    // Connect client 2
    ClientVM client2 =
        cluster.startClientVM(3, getDurableClientProperties(testName.getMethodName() + "_2"),
            (ccf) -> {
              ccf.setPoolSubscriptionEnabled(true);
              ccf.addPoolLocator("localhost", locatorPort);
            });

    // Invoke readyForEvents in both clients
    client1.invoke(this::readyForEvents);
    client2.invoke(this::readyForEvents);

    // Verify CacheClientProxies have different remoteHostAddresses
    server.invoke(() -> verifyRemoteHostAddresses());
  }

  protected Properties getDurableClientProperties(String durableClientId) {
    Properties properties = new Properties();
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    return properties;
  }

  private void readyForEvents() {
    ClusterStartupRule.clientCacheRule.getCache().readyForEvents();
  }

  private void verifyRemoteHostAddresses() {
    verifyRemoteHostAddresses(getAcceptor(), 2);
  }

  private AcceptorImpl getAcceptor() {
    Cache cache = ClusterStartupRule.getCache();
    List<CacheServer> cacheServers = cache.getCacheServers();
    CacheServer cacheServer = cacheServers.get(0);
    return (AcceptorImpl) ((InternalCacheServer) cacheServer).getAcceptor();
  }

  private void verifyRemoteHostAddresses(AcceptorImpl acceptor, int expectedNumProxies) {
    // Wait for the expected number of CacheClientProxies to be created which happens asynchronously
    await().until(
        () -> acceptor.getCacheClientNotifier().getClientProxies().size() == expectedNumProxies);

    // Get their remoteHostAddresses
    Collection<CacheClientProxy> proxies = acceptor.getCacheClientNotifier().getClientProxies();
    Set<String> remoteHostAddresses =
        proxies.stream().map(CacheClientProxy::getRemoteHostAddress).collect(Collectors.toSet());

    // Verify the expected number of remoteHostAddresses
    assertThat(remoteHostAddresses.size()).isEqualTo(expectedNumProxies);
  }
}
