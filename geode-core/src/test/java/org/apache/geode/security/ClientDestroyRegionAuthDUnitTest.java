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
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.Host;
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
public class ClientDestroyRegionAuthDUnitTest extends JUnit4DistributedTestCase {
  private static String REGION_NAME = "testRegion";
  private int serverPort;

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);
  final VM client3 = host.getVM(3);

  @Rule
  public LocalServerStarterRule server =
      new ServerStarterBuilder().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withRegion(RegionShortcut.REPLICATE, REGION_NAME).buildInThisVM();

  @Before
  public void setup() {
    serverPort = server.getServerPort();
  }

  @Test
  public void testDestroyRegion() throws InterruptedException {
    client1.invoke(() -> {
      ClientCache cache =
          SecurityTestUtil.createClientCache("dataWriter", "1234567", server.getServerPort());

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      SecurityTestUtil.assertNotAuthorized(() -> region.destroyRegion(), "DATA:MANAGE");
    });

    client2.invoke(() -> {
      ClientCache cache = SecurityTestUtil.createClientCache("authRegionManager", "1234567",
          server.getServerPort());

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      SecurityTestUtil.assertNotAuthorized(() -> region.destroyRegion(), "DATA:MANAGE");
    });

    client3.invoke(() -> {
      ClientCache cache =
          SecurityTestUtil.createClientCache("super-user", "1234567", server.getServerPort());

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      region.destroyRegion();
      assertThat(region.isDestroyed()).isTrue();
    });
  }

}
