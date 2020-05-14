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
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.modules.session.catalina.PeerToPeerCacheLifecycleListener;
import org.apache.geode.modules.session.catalina.Tomcat8DeltaSessionManager;
import org.apache.geode.test.junit.categories.SessionTest;

@Category(SessionTest.class)
public class Tomcat8SessionsDUnitTest extends TestSessionsTomcat8Base {

  @Before
  public void setUp() throws Exception {
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new EmbeddedTomcat8(port, "JVM-1");

    PeerToPeerCacheLifecycleListener p2pListener = new PeerToPeerCacheLifecycleListener();
    p2pListener.setProperty(MCAST_PORT, "0");
    p2pListener.setProperty(LOG_LEVEL, "config");
    server.addLifecycleListener(p2pListener);
    sessionManager = new Tomcat8DeltaSessionManager();
    sessionManager.setEnableCommitValve(true);
    server.getRootContext().setManager(sessionManager);
    AuthConfigFactory.setFactory(null);

    servlet = server.addServlet("/test/*", "default", CommandServlet.class.getName());
    server.startContainer();

    // Can only retrieve the region once the container has started up (& the cache has started too).
    region = sessionManager.getSessionCache().getSessionRegion();

    sessionManager.getTheContext().setSessionTimeout(30);
    region.clear();
    basicConnectivityCheck();
  }

  @After
  public void tearDown() {
    server.stopContainer();
  }
}
