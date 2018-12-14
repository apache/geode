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

import static org.apache.geode.session.tests.ContainerInstall.ConnectionType.CACHING_CLIENT_SERVER;
import static org.apache.geode.session.tests.GenericAppServerInstall.GenericAppServerVersion.JETTY9;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.function.IntSupplier;

import javax.servlet.http.HttpSession;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

public class Jetty9CachingClientServerTest extends GenericAppServerClientServerTest {

  @Override
  public ContainerInstall getInstall(IntSupplier portSupplier)
      throws IOException, InterruptedException {
    return new GenericAppServerInstall(getClass().getSimpleName(), JETTY9, CACHING_CLIENT_SERVER,
        portSupplier);
  }

  /**
   * Test that we cache the user's session on the client, rather than going to the server for each
   * request
   */
  @Test
  public void shouldCacheSessionOnClient()
      throws Exception {
    manager.startAllInactiveContainers();

    String key = "value_testSessionExpiration";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    serverVM.invoke("set bogus session key", () -> {
      final InternalCache cache = ClusterStartupRule.memberStarter.getCache();
      Region<String, HttpSession> region = cache.getRegion("gemfire_modules_sessions");
      region.values().forEach(session -> session.setAttribute(key, "bogus"));
    });

    // Make sure the client still sees its original cached value
    resp = client.get(key);
    assertEquals(value, resp.getResponse());
  }
}
