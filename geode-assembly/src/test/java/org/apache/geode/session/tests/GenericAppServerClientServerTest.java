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
package org.apache.geode.session.tests;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.junit.Before;

/**
 * Extends the {@link CargoTestBase} class to support client server tests of generic app servers
 *
 * Currently being used to test Jetty 9 containers in client server mode.
 */
public abstract class GenericAppServerClientServerTest extends CargoTestBase {
  /**
   * Starts the server for the client containers to connect to while testing.
   */
  @Before
  public void startServers() throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> {
      Cache cache = getCache();
      CacheServer server = cache.addCacheServer();
      server.setPort(0);
      server.start();
    });
    Thread.sleep(5000);
  }
}
