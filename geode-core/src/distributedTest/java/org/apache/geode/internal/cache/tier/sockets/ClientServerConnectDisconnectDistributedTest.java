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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.shiro.subject.Subject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({SecurityTest.class})
public class ClientServerConnectDisconnectDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private static List<Subject> serverConnectionSubjects;

  private static Subject proxySubject;

  private static List<ClientUserAuths> authorizations;

  @Test
  public void testSubjectsLoggedOutOnClientConnectDisconnect() throws Exception {
    // Start Locator
    MemberVM locator =
        cluster.startLocatorVM(0, l -> l.withSecurityManager(SimpleSecurityManager.class));

    // Start server
    int locatorPort = locator.getPort();
    String regionName = testName.getMethodName() + "_region";
    MemberVM server = cluster.startServerVM(1, s -> s.withCredential("cluster", "cluster")
        .withConnectionToLocator(locatorPort).withRegion(RegionShortcut.PARTITION, regionName));

    // Connect client
    ClientVM client = cluster.startClientVM(3, c -> c.withCredential("data", "data")
        .withPoolSubscription(true)
        .withLocatorConnection(locatorPort));

    // Do some puts
    client.invoke(() -> {
      Region region = ClusterStartupRule.clientCacheRule.createProxyRegion(regionName);
      for (int i = 0; i < 10; i++) {
        Object key = String.valueOf(i);
        region.put(key, key);
      }
    });

    // Verify client sessions are logged in on the server
    server.invoke(() -> verifySubjectsAreLoggedIn());

    // Close client
    client.invoke(() -> {
      ClusterStartupRule.getClientCache().close();
    });

    // Verify client sessions are logged out on the server
    server.invoke(() -> verifySubjectsAreLoggedOut());
  }

  @Test
  public void testFilterProfileCleanupOnClientConnectDisconnect() throws Exception {
    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start server
    int locatorPort = locator.getPort();
    String regionName = testName.getMethodName() + "_region";
    MemberVM server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withRegion(RegionShortcut.PARTITION, regionName));

    // Connect client
    ClientVM client = cluster.startClientVM(3,
        c -> c.withPoolSubscription(true).withLocatorConnection(locatorPort));

    // Create client region and register interest
    client.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion(regionName).registerInterestForAllKeys();
    });

    // Verify proxy id is registered in filter profile
    server.invoke(() -> verifyRealAndWireProxyIdsInFilterProfile(regionName, 1));

    // Close client
    client.invoke(() -> {
      ClusterStartupRule.getClientCache().close();
    });

    // Wait for CacheClientProxy to be closed
    server.invoke(() -> waitForCacheClientProxyToBeClosed());

    // Verify proxy id is unregistered from filter profile
    server.invoke(() -> verifyRealAndWireProxyIdsInFilterProfile(regionName, 0));
  }

  private void verifySubjectsAreLoggedIn() {
    AcceptorImpl acceptor = getAcceptor();

    // Verify ServerConnection subjects are logged in
    verifyServerConnectionSubjectsAreLoggedIn(acceptor);

    // Verify CacheClientProxy subject is logged out
    verifyCacheClientProxySubjectIsLoggedIn(acceptor);
  }

  private AcceptorImpl getAcceptor() {
    Cache cache = ClusterStartupRule.getCache();
    List<CacheServer> cacheServers = cache.getCacheServers();
    CacheServer cacheServer = cacheServers.get(0);
    return (AcceptorImpl) ((InternalCacheServer) cacheServer).getAcceptor();
  }

  private void verifyServerConnectionSubjectsAreLoggedIn(AcceptorImpl acceptor) {
    serverConnectionSubjects = new ArrayList<>();
    authorizations = new ArrayList<>();
    for (ServerConnection sc : acceptor.getAllServerConnections()) {
      ClientUserAuths auth = sc.getClientUserAuths();
      assertThat(auth.getAllSubjects().size()).isNotEqualTo(0);
      authorizations.add(auth);
      for (Subject subject : auth.getAllSubjects()) {
        assertThat(subject.getPrincipal()).isNotNull();
        assertThat(subject.getPrincipals()).isNotNull();
        assertThat(subject.isAuthenticated()).isTrue();
        serverConnectionSubjects.add(subject);
      }
    }
  }

  private void verifyCacheClientProxySubjectIsLoggedIn(AcceptorImpl acceptor) {
    // Wait for the CacheClientProxy to be created since its asynchronous
    await().until(() -> acceptor.getCacheClientNotifier().getClientProxies().size() == 1);
    CacheClientProxy proxy = acceptor.getCacheClientNotifier().getClientProxies().iterator().next();

    // Check CacheClientProxy subject
    proxySubject = proxy.getSubject();
    assertThat(proxySubject).isNotNull();
    assertThat(proxySubject.getPrincipal()).isNotNull();
    assertThat(proxySubject.getPrincipals()).isNotNull();
    assertThat(proxySubject.isAuthenticated()).isTrue();
  }

  private void verifySubjectsAreLoggedOut() {
    AcceptorImpl acceptor = getAcceptor();

    // Wait for ServerConnections to be closed
    waitForServerConnectionsToBeClosed(acceptor);

    // Verify ServerConnection subjects are logged out
    verifyServerConnectionSubjectsAreLoggedOut();

    // Wait for CacheClientProxy to be closed
    waitForCacheClientProxyToBeClosed(acceptor);

    // Verify the CacheClientProxy subject is logged out
    verifyCacheClientProxyIsLoggedOut();
  }

  private void waitForServerConnectionsToBeClosed(AcceptorImpl acceptor) {
    // Wait for all ServerConnections to be closed since handleTermination is in the finally block
    await().until(() -> acceptor.getAllServerConnections().isEmpty());
  }

  private void verifyServerConnectionSubjectsAreLoggedOut() {
    for (Subject subject : serverConnectionSubjects) {
      assertThat(subject.getPrincipal()).isNull();
      assertThat(subject.getPrincipals()).isNull();
      assertThat(subject.isAuthenticated()).isFalse();
    }

    for (ClientUserAuths auth : authorizations) {
      assertThat(auth.getAllSubjects().size()).isEqualTo(0);
    }
  }

  private void waitForCacheClientProxyToBeClosed(AcceptorImpl acceptor) {
    // Wait for the CacheClientProxy to be closed since handleTermination is in the finally block
    await().until(() -> acceptor.getCacheClientNotifier().getClientProxies().isEmpty());
  }

  private void verifyCacheClientProxyIsLoggedOut() {
    assertThat(proxySubject.getPrincipal()).isNull();
    assertThat(proxySubject.getPrincipals()).isNull();
    assertThat(proxySubject.isAuthenticated()).isFalse();
  }

  private void waitForCacheClientProxyToBeClosed() {
    waitForCacheClientProxyToBeClosed(getAcceptor());
  }

  private void verifyRealAndWireProxyIdsInFilterProfile(String regionName, int expectedNumIds) {
    // Get filter profile
    Cache cache = ClusterStartupRule.getCache();
    LocalRegion region = (LocalRegion) cache.getRegion(regionName);
    FilterProfile fp = region.getFilterProfile();

    // Assert expectedNumIds real proxy id
    Set realProxyIds = fp.getRealClientIds();
    assertThat(realProxyIds.size()).isEqualTo(expectedNumIds);

    // Assert expectedNumIds wire proxy id
    Set wireProxyIds = fp.getWireClientIds();
    assertThat(wireProxyIds.size()).isEqualTo(expectedNumIds);
  }
}
