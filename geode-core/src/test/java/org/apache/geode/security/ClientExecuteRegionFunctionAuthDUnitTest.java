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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({DistributedTest.class, SecurityTest.class})
public class ClientExecuteRegionFunctionAuthDUnitTest extends JUnit4DistributedTestCase {

  private static String REGION_NAME = "AuthRegion";

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  private final static Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withRegion(RegionShortcut.REPLICATE, REGION_NAME);

  @Test
  public void testExecuteRegionFunction() {

    FunctionService.registerFunction(function);

    client1.invoke("logging in with dataReader", () -> {
      ClientCache cache = createClientCache("dataReader", "1234567", server.getPort());

      Region region = createProxyRegion(cache, REGION_NAME);
      FunctionService.registerFunction(function);
      assertNotAuthorized(
          () -> FunctionService.onRegion(region).withArgs(Boolean.TRUE).execute(function.getId()),
          "DATA:WRITE");
    });

    client2.invoke("logging in with super-user", () -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());

      Region region = createProxyRegion(cache, REGION_NAME);
      FunctionService.registerFunction(function);
      ResultCollector rc =
          FunctionService.onRegion(region).withArgs(Boolean.TRUE).execute(function.getId());
      rc.getResult();
    });
  }
}


