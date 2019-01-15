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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.shiro.subject.Subject;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class ClientServerConnectDisconnectDistributedTest implements Serializable {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator;
  private static MemberVM server;
  private static ClientVM client;

  private static List<Subject> serverConnectionSubjects;

  private static Subject proxySubject;

  private static List<ClientUserAuths> authorizations;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties locatorProps = new Properties();
    locatorProps.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getCanonicalName());
    locator = cluster.startLocatorVM(0, locatorProps);

    Properties serverProps = new Properties();
    serverProps.setProperty("security-username", "cluster");
    serverProps.setProperty("security-password", "cluster");
    server = cluster.startServerVM(1, serverProps, locator.getPort());

    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
    });
  }

  @Test
  public void testClientConnectDisconnect() throws Exception {
    // Connect client
    int locatorPort = locator.getPort();
    client = cluster.startClientVM(3, c -> c.withCredential("data", "data")
        .withPoolSubscription(true)
        .withLocatorConnection(locatorPort));

    // Do some puts
    client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
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

  private void verifySubjectsAreLoggedIn() {
    Cache cache = ClusterStartupRule.getCache();
    List<CacheServer> cacheServers = cache.getCacheServers();
    CacheServer cacheServer = cacheServers.get(0);
    AcceptorImpl acceptor = ((CacheServerImpl) cacheServer).getAcceptor();

    // Verify ServerConnection subjects are logged in
    verifyServerConnectionSubjectsAreLoggedIn(acceptor);

    // Verify CacheClientProxy subject is logged out
    verifyCacheClientProxySubjectIsLoggedIn(acceptor);
  }

  private void verifyServerConnectionSubjectsAreLoggedIn(AcceptorImpl acceptor) {
    serverConnectionSubjects = new ArrayList<>();
    authorizations = new ArrayList<>();
    for (ServerConnection sc : acceptor.getAllServerConnections()) {
      ClientUserAuths auth = sc.getClientUserAuths();
      assertThat(auth.getSubjects().size()).isNotEqualTo(0);
      authorizations.add(auth);
      for (Subject subject : auth.getSubjects()) {
        assertThat(subject.getPrincipal()).isNotNull();
        assertThat(subject.getPrincipals()).isNotNull();
        assertThat(subject.isAuthenticated()).isTrue();
        serverConnectionSubjects.add(subject);
      }
    }
  }

  private void verifyCacheClientProxySubjectIsLoggedIn(AcceptorImpl acceptor) {
    // Ensure the CacheClientProxy is created since its asynchronous
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
    // Verify ServerConnection subjects are logged out
    verifyServerConnectionSubjectsAreLoggedOut();

    // Verify the CacheClientProxy subject is logged out
    verifyCacheClientProxyIsLoggedOut();
  }

  private void verifyServerConnectionSubjectsAreLoggedOut() {
    for (Subject subject : serverConnectionSubjects) {
      assertThat(subject.getPrincipal()).isNull();
      assertThat(subject.getPrincipals()).isNull();
      assertThat(subject.isAuthenticated()).isFalse();
    }

    for (ClientUserAuths auth : authorizations) {
      assertThat(auth.getSubjects().size()).isEqualTo(0);
    }
  }

  private void verifyCacheClientProxyIsLoggedOut() {
    assertThat(proxySubject.getPrincipal()).isNull();
    assertThat(proxySubject.getPrincipals()).isNull();
    assertThat(proxySubject.isAuthenticated()).isFalse();
  }
}
