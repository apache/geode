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
import static org.apache.geode.security.SecurityTestUtil.assertNotAuthorized;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class ClientGetEntryAuthDUnitTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME = "AuthRegion";

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withAutoStart();

  @Before
  public void before() throws Exception {
    Region region =
        server.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
    for (int i = 0; i < 5; i++) {
      region.put("key" + i, "value" + i);
    }
  }

  @Test
  public void testGetEntry() throws Exception {
    // client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache = createClientCache("stranger", "1234567", server.getPort());

      CacheTransactionManager transactionManager = cache.getCacheTransactionManager();
      transactionManager.begin();
      try {
        Region region = createProxyRegion(cache, REGION_NAME);
        assertNotAuthorized(() -> region.getEntry("key3"), "DATA:READ:AuthRegion:key3");
      } finally {
        transactionManager.commit();
      }

    });

    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache = createClientCache("authRegionReader", "1234567", server.getPort());

      CacheTransactionManager transactionManager = cache.getCacheTransactionManager();
      transactionManager.begin();
      try {
        Region region = createProxyRegion(cache, REGION_NAME);
        region.getEntry("key3");
      } finally {
        transactionManager.commit();
      }

    });

    ai1.await();
    ai2.await();

  }
}
