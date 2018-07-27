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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;

import javax.servlet.http.HttpSession;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.DUnitEnv;

/**
 * Jetty 9 Client Server tests
 *
 * Runs all the tests in {@link CargoTestBase} on the Jetty 9 install, setup in the
 * {@link #setupJettyInstall()} method before tests are run.
 */
public class Jetty9CachingClientServerTest extends GenericAppServerClientServerTest {
  private static ContainerInstall install;

  @BeforeClass
  public static void setupJettyInstall() throws Exception {
    install = new GenericAppServerInstall(GenericAppServerInstall.GenericAppServerVersion.JETTY9,
        ContainerInstall.ConnectionType.CACHING_CLIENT_SERVER,
        ContainerInstall.DEFAULT_INSTALL_DIR + "Jetty9CachingClientServerTest");
    install.setDefaultLocator(DUnitEnv.get().getLocatorAddress(), DUnitEnv.get().getLocatorPort());
  }

  @Override
  public ContainerInstall getInstall() {
    return install;
  }

  /**
   * Test that we cache the user's session on the client, rather than going to the server for each
   * request
   */
  @Test
  public void shouldCacheSessionOnClient()
      throws IOException, URISyntaxException, InterruptedException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionExpiration";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    // Modify the values on the server
    serverVM.invoke(() -> {
      Cache cache = getCache();
      Region<String, HttpSession> region = cache.getRegion("gemfire_modules_sessions");
      region.values().forEach(session -> session.setAttribute(key, "bogus"));
    });

    // Make sure the client still sees it's original cached value
    resp = client.get(key);
    assertEquals(value, resp.getResponse());
  }
}
