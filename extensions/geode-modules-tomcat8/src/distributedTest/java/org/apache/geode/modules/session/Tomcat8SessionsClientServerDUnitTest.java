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
package org.apache.geode.modules.session;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import javax.security.auth.message.config.AuthConfigFactory;

import org.apache.catalina.LifecycleState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.modules.session.catalina.ClientServerCacheLifecycleListener;
import org.apache.geode.modules.session.catalina.DeltaSessionManager;
import org.apache.geode.modules.session.catalina.Tomcat8DeltaSessionManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SessionTest;



@Category(SessionTest.class)
public class Tomcat8SessionsClientServerDUnitTest extends TestSessionsTomcat8Base {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(2);

  private ClientCache clientCache;

  @Before
  public void setUp() throws Exception {
    int locatorPortSuggestion = AvailablePortHelper.getRandomAvailableTCPPort();
    MemberVM locatorVM = clusterStartupRule.startLocatorVM(0, locatorPortSuggestion);
    assertThat(locatorVM).isNotNull();

    Integer locatorPort = locatorVM.getPort();
    assertThat(locatorPort).isGreaterThan(0);

    MemberVM serverVM = clusterStartupRule.startServerVM(1, locatorPort);
    assertThat(serverVM).isNotNull();

    port = AvailablePortHelper.getRandomAvailableTCPPort();
    assertThat(port).isGreaterThan(0);

    server = new EmbeddedTomcat8(port, "JVM-1");
    assertThat(server).isNotNull();

    ClientCacheFactory cacheFactory = new ClientCacheFactory();
    assertThat(cacheFactory).isNotNull();

    cacheFactory.addPoolServer("localhost", serverVM.getPort()).setPoolSubscriptionEnabled(true);
    clientCache = cacheFactory.create();
    assertThat(clientCache).isNotNull();

    DeltaSessionManager manager = new Tomcat8DeltaSessionManager();
    assertThat(manager).isNotNull();

    ClientServerCacheLifecycleListener listener = new ClientServerCacheLifecycleListener();
    assertThat(listener).isNotNull();

    listener.setProperty(MCAST_PORT, "0");
    listener.setProperty(LOG_LEVEL, "config");
    server.addLifecycleListener(listener);

    sessionManager = manager;
    sessionManager.setEnableCommitValve(true);
    server.getRootContext().setManager(sessionManager);

    AuthConfigFactory.setFactory(null);

    servlet = server.addServlet("/test/*", "default", CommandServlet.class.getName());
    assertThat(servlet).isNotNull();

    server.startContainer();
    // Can only retrieve the region once the container has started up (& the cache has started too).
    region = sessionManager.getSessionCache().getSessionRegion();
    assertThat(region).isNotNull();

    sessionManager.getTheContext().setSessionTimeout(30);
    await().until(() -> sessionManager.getState() == LifecycleState.STARTED);

    basicConnectivityCheck();
  }

  @After
  public void tearDown() {
    clientCache.close();
    clientCache = null;
    port = -1;

    server.stopContainer();
    server = null;
    servlet = null;

    sessionManager = null;
    region = null;

  }
}
