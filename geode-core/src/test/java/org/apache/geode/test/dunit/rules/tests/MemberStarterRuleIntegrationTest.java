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

package org.apache.geode.test.dunit.rules.tests;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(IntegrationTest.class)
public class MemberStarterRuleIntegrationTest {


  private LocatorStarterRule locator;
  private ServerStarterRule server;

  @After
  public void cleanupAnyStartedMembers() {
    if (locator != null) {
      locator.after();
    }
    if (server != null) {
      server.after();
    }
  }

  @Test
  public void testWithPortOnLocator() {
    int targetPort = AvailablePort.getRandomAvailablePort(1);
    locator = new LocatorStarterRule().withPort(targetPort).withAutoStart();
    locator.before();

    InternalLocator internalMember = locator.getLocator();

    // This is the rule framework's port
    assertThat(locator.getPort()).isEqualTo(targetPort);
    // This is the actual, live member's port.
    assertThat(internalMember.getPort()).isEqualTo(targetPort);

  }

  @Test
  public void testWithPortOnServer() {
    int targetPort = AvailablePort.getRandomAvailablePort(1);
    server = new ServerStarterRule().withPort(targetPort).withAutoStart();
    server.before();

    CacheServer internalMember = server.getServer();

    // This is the rule framework's port
    assertThat(server.getPort()).isEqualTo(targetPort);
    // This is the actual, live member's port.
    assertThat(internalMember.getPort()).isEqualTo(targetPort);

  }
}
