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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;

import javax.servlet.http.HttpSession;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.modules.session.functions.GetSessionCount;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

/**
 * Extends the {@link CargoTestBase} class to support client server tests of generic app servers
 *
 * Currently being used to test Jetty 9 containers in client server mode.
 */
public abstract class GenericAppServerClientServerTest extends CargoTestBase {
  protected MemberVM serverVM;

  @Before
  public void startServer() {
    serverVM = clusterStartupRule.startServerVM(1, locatorVM.getPort());
  }

  /**
   * Test that we don't leave native sessions in the container, wasting memory
   */
  @Test
  public void shouldNotLeaveNativeSessionInContainer()
      throws Exception {
    manager.startAllInactiveContainers();

    String key = "value_testSessionExpiration";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertThat(resp.getSessionCookie()).as("Sessions are not replicating properly")
          .isEqualTo(cookie);
      assertThat(resp.getResponse()).isEqualTo(value);
    }

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.executionFunction(GetSessionCount.class);
      assertThat(resp.getResponse()).as("Should have 0 native sessions").isEqualTo("0");
    }
  }

  @Override
  protected void verifySessionIsRemoved(String key) throws IOException, URISyntaxException {
    serverVM.invoke("verify session is removed", () -> {
      final InternalCache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("gemfire_modules_sessions");
      await("for region to be empty").untilAsserted(() -> assertThat(region.size()).isEqualTo(0));
    });
    super.verifySessionIsRemoved(key);
  }

  @Override
  protected void verifyMaxInactiveInterval(int expected) throws IOException, URISyntaxException {
    super.verifyMaxInactiveInterval(expected);
    serverVM.invoke("verify max inactive interval", () -> {
      final InternalCache cache = ClusterStartupRule.getCache();
      Region<Object, HttpSession> region = cache.getRegion("gemfire_modules_sessions");
      region.values().forEach(session -> assertThat(session.getMaxInactiveInterval())
          .isEqualTo(expected));
    });
  }
}
