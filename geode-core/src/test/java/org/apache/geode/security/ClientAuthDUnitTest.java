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
package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({DistributedTest.class, SecurityTest.class})
public class ClientAuthDUnitTest extends JUnit4DistributedTestCase {

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  int serverPort;

  @Before
  public void setup() {
    serverPort = server.getServerPort();
  }

  @Rule
  public transient LocalServerStarterRule server = new ServerStarterBuilder()
      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName()).buildInThisVM();

  @Test
  public void authWithCorrectPasswordShouldPass() {
    client1.invoke("logging in super-user with correct password", () -> {
      SecurityTestUtil.createClientCache("test", "test", serverPort);
    });
  }

  @Test
  public void authWithIncorrectPasswordShouldFail() {
    IgnoredException.addIgnoredException(AuthenticationFailedException.class.getName());
    client2.invoke("logging in super-user with wrong password", () -> {
      assertThatThrownBy(() -> SecurityTestUtil.createClientCache("test", "wrong", serverPort))
          .isInstanceOf(AuthenticationFailedException.class);
    });
  }
}


