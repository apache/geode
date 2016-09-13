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
package org.apache.geode.modules.session;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.internal.cache.CacheServerLauncher.serverPort;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.modules.session.catalina.ClientServerCacheLifecycleListener;
import org.apache.geode.modules.session.catalina.DeltaSessionManager;
import org.apache.geode.modules.session.catalina.Tomcat8DeltaSessionManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(DistributedTest.class)
public class Tomcat8SessionsClientServerDUnitTest extends TestSessionsTomcat8Base {

  // Set up the session manager we need
  @Override
  public void postSetUp() throws Exception {
    setupServer(new Tomcat8DeltaSessionManager());
  }

  @Override
  public void preTearDown() {
    vm0.invoke(() -> {
      GemFireCacheImpl.getInstance().getCacheServers().forEach(e -> ((CacheServer)e).stop());
    });
    server.stopContainer();
  }

  // Set up the servers we need
  public void setupServer(DeltaSessionManager manager) throws Exception {
    Host host = Host.getHost(0);
    vm0 = host.getVM(1);
    String hostName = vm0.getHost().getHostName();
    int cacheServerPort = vm0.invoke(() -> {
      Properties props = new Properties();
      CacheFactory cf = new CacheFactory(props);
      Cache cache = cf.create();
      CacheServer server = cache.addCacheServer();
      server.start();
      return server.getPort();
    });

    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new EmbeddedTomcat8("/test", port, "JVM-1");

    ClientServerCacheLifecycleListener listener = new ClientServerCacheLifecycleListener();
    listener.setProperty(MCAST_PORT, "0");
    listener.setProperty(LOG_LEVEL, "config");
    server.addLifecycleListener(listener);
    sessionManager = manager;
    sessionManager.setEnableCommitValve(true);
    server.getRootContext().setManager(sessionManager);

    servlet = server.addServlet("/test/*", "default", CommandServlet.class.getName());
    server.startContainer();

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(hostName, cacheServerPort);
    pf.create("Pool Connecting to Cache Server");

    /*
     * Can only retrieve the region once the container has started up
     * (and the cache has started too).
     */
    region = sessionManager.getSessionCache().getSessionRegion();
    sessionManager.getTheContext().setSessionTimeout(30);
  }
}
