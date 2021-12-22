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
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class DurableClientConnectDisconnectSocketDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Test
  public void testSocketClosedOnClientDisconnect() throws Exception {
    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start server
    int locatorPort = locator.getPort();
    MemberVM server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));

    // Connect client
    ClientVM client =
        cluster.startClientVM(2, getDurableClientProperties(testName.getMethodName()),
            (ccf) -> {
              ccf.setPoolSubscriptionEnabled(true);
              ccf.addPoolLocator("localhost", locatorPort);
            });

    // Invoke readyForEvents in client
    client.invoke(this::readyForEvents);

    // Verify client socket is connected on the server
    server.invoke(this::verifyCacheClientProxySocketIsOpen);

    // Close client
    client.invoke(this::closeClient);

    // Wait for the client socket to be closed on the server
    server.invoke(this::waitForCacheClientProxySocketToBeClosed);

    // Reconnect the client
    client =
        cluster.startClientVM(2, getDurableClientProperties(testName.getMethodName()),
            (ccf) -> {
              ccf.setPoolSubscriptionEnabled(true);
              ccf.addPoolLocator("localhost", locatorPort);
            });

    // Invoke readyForEvents in client
    client.invoke(this::readyForEvents);

    // Verify client socket is connected on the server
    server.invoke(this::verifyCacheClientProxySocketIsOpen);

    // Close client
    client.invoke(this::closeClient);

    // Wait for the client socket to be closed on the server
    server.invoke(this::waitForCacheClientProxySocketToBeClosed);
  }

  protected Properties getDurableClientProperties(String durableClientId) {
    Properties properties = new Properties();
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    return properties;
  }

  private void closeClient() {
    ClusterStartupRule.getClientCache().close(true);
  }

  private void readyForEvents() {
    ClusterStartupRule.getClientCache().readyForEvents();
  }

  private AcceptorImpl getAcceptor() {
    CacheServer cacheServer = ClusterStartupRule.getCache().getCacheServers().get(0);
    return (AcceptorImpl) ((InternalCacheServer) cacheServer).getAcceptor();
  }

  private void verifyCacheClientProxySocketIsOpen() {
    // Get the acceptor
    AcceptorImpl acceptor = getAcceptor();

    // Wait for the CacheClientProxy to be created since its asynchronous
    await().until(() -> acceptor.getCacheClientNotifier().getClientProxies().size() == 1);

    CacheClientProxy proxy = acceptor.getCacheClientNotifier().getClientProxies().iterator().next();
    assertThat(proxy.getSocket().isClosed()).isFalse();
  }

  private void waitForCacheClientProxySocketToBeClosed() {
    // Get the acceptor
    AcceptorImpl acceptor = getAcceptor();

    // Get the CacheClientProxy
    CacheClientProxy proxy = acceptor.getCacheClientNotifier().getClientProxies().iterator().next();

    // Wait for the CacheClientProxy's socket to be closed
    await().until(() -> proxy.getSocket().isClosed());
  }
}
