/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.gemstone.gemfire.modules.session;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.modules.session.catalina.DeltaSessionManager;
import com.gemstone.gemfire.modules.session.catalina.PeerToPeerCacheLifecycleListener;
import com.gemstone.gemfire.modules.session.catalina.Tomcat8DeltaSessionManager;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class Tomcat8SessionsDUnitTest extends TestSessionsTomcat8Base {

  // Set up the session manager we need
  @Override
  public void postSetUp() throws Exception {
    setupServer(new Tomcat8DeltaSessionManager());
  }

  @Override
  public void preTearDown() throws Exception {
    server.stopContainer();
  }

  public void setupServer(DeltaSessionManager manager) throws Exception {
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new EmbeddedTomcat8("/test", port, "JVM-1");

    PeerToPeerCacheLifecycleListener p2pListener = new PeerToPeerCacheLifecycleListener();
    p2pListener.setProperty(MCAST_PORT, "0");
    p2pListener.setProperty(LOG_LEVEL, "config");
    server.addLifecycleListener(p2pListener);
    sessionManager = manager;
    sessionManager.setEnableCommitValve(true);
    server.getRootContext().setManager(sessionManager);

    servlet = server.addServlet("/test/*", "default", CommandServlet.class.getName());
    server.startContainer();

    /*
     * Can only retrieve the region once the container has started up
     * (and the cache has started too).
     */
    region = sessionManager.getSessionCache().getSessionRegion();
  }

  /**
   * Reset some data
   */
  @Before
  public void setup() throws Exception {
    sessionManager.getTheContext().setSessionTimeout(30);
    region.clear();
  }
}
