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

import javax.security.auth.message.config.AuthConfigFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.springframework.util.SocketUtils;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
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
  private MemberVM locatorVM;
  private MemberVM serverVM;

  @Before
  public void setUp() throws Exception {
    locatorVM = clusterStartupRule.startLocatorVM(0, 0);
    Integer locatorPort = locatorVM.getPort();
    serverVM = clusterStartupRule.startServerVM(1, locatorPort);

    port = SocketUtils.findAvailableTcpPort();
    server = new EmbeddedTomcat8(port, "JVM-1");

    ClientCacheFactory cacheFactory = new ClientCacheFactory();
    cacheFactory.addPoolServer("localhost", serverVM.getPort()).setPoolSubscriptionEnabled(true);
    clientCache = cacheFactory.create();
    DeltaSessionManager manager = new Tomcat8DeltaSessionManager();

    ClientServerCacheLifecycleListener listener = new ClientServerCacheLifecycleListener();
    listener.setProperty(MCAST_PORT, "0");
    listener.setProperty(LOG_LEVEL, "config");
    server.addLifecycleListener(listener);
    sessionManager = manager;
    sessionManager.setEnableCommitValve(true);
    server.getRootContext().setManager(sessionManager);
    AuthConfigFactory.setFactory(null);

    servlet = server.addServlet("/test/*", "default", CommandServlet.class.getName());
    server.startContainer();


    // Can only retrieve the region once the container has started up (& the cache has started too).
    region = sessionManager.getSessionCache().getSessionRegion();
    sessionManager.getTheContext().setSessionTimeout(30);
  }

  @After
  public void tearDown() {
    clientCache.close();
    server.stopContainer();
  }
}
