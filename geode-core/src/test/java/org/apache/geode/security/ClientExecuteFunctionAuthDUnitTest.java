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

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class ClientExecuteFunctionAuthDUnitTest extends JUnit4DistributedTestCase {

  private static String REGION_NAME = "testRegion";

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  private final static Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);

  @Rule
  public LocalServerStarterRule server =
      new ServerStarterBuilder().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withRegion(RegionShortcut.REPLICATE, REGION_NAME).buildInThisVM();

  @Before
  public void setup() {

  }

  @Test
  public void testExecuteRegionFunctionWithClientRegistration() {

    FunctionService.registerFunction(function);
    client1.invoke("logging in with dataReader", () -> {
      ClientCache cache = createClientCache("dataReader", "1234567", server.getServerPort());

      FunctionService.registerFunction(function);

      assertNotAuthorized(() -> FunctionService.onServer(cache.getDefaultPool())
          .withArgs(Boolean.TRUE).execute(function.getId()), "DATA:WRITE");
    });

    client2.invoke("logging in with super-user", () -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getServerPort());

      FunctionService.registerFunction(function);
      ResultCollector rc = FunctionService.onServer(cache.getDefaultPool()).withArgs(Boolean.TRUE)
          .execute(function.getId());
      rc.getResult();
    });
  }

  @Test
  // this would trigger the client to send a GetFunctionAttribute command before executing it
  public void testExecuteRegionFunctionWithOutClientRegistration() {
    FunctionService.registerFunction(function);
    client1.invoke("logging in with dataReader", () -> {
      ClientCache cache = createClientCache("dataReader", "1234567", server.getServerPort());
      assertNotAuthorized(() -> FunctionService.onServer(cache.getDefaultPool())
          .withArgs(Boolean.TRUE).execute(function.getId()), "DATA:WRITE");
    });
  }
}


