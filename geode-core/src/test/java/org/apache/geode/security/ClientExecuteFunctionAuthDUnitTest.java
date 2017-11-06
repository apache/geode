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

import static org.apache.geode.cache.execute.FunctionService.onServer;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.security.SecurityTestUtil.assertNotAuthorized;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.management.internal.security.TestFunctions.ReadFunction;
import org.apache.geode.management.internal.security.TestFunctions.WriteFunction;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({DistributedTest.class, SecurityTest.class})
public class ClientExecuteFunctionAuthDUnitTest extends JUnit4DistributedTestCase {
  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  private Function writeFunction;
  private Function readFunction;

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName()).withAutoStart();

  @Before
  public void before() {
    writeFunction = new WriteFunction();
    readFunction = new ReadFunction();
    FunctionService.registerFunction(writeFunction);
    FunctionService.registerFunction(readFunction);
  }

  @Test
  public void testExecuteFunctionWithClientRegistration() {
    client1.invoke("logging in with dataReader", () -> {
      ClientCache cache = createClientCache("dataRead", "dataRead", server.getPort());

      FunctionService.registerFunction(writeFunction);
      FunctionService.registerFunction(readFunction);

      // can not write
      assertNotAuthorized(() -> onServer(cache.getDefaultPool()).execute(writeFunction.getId()),
          "DATA:WRITE");

      // can read
      ResultCollector rc = onServer(cache.getDefaultPool()).execute(readFunction.getId());
      assertThat(((ArrayList) rc.getResult()).get(0)).isEqualTo(ReadFunction.SUCCESS_OUTPUT);
    });

    client2.invoke("logging in with dataWriter", () -> {
      ClientCache cache = createClientCache("dataWrite", "dataWrite", server.getPort());

      FunctionService.registerFunction(writeFunction);
      FunctionService.registerFunction(readFunction);
      // can write
      ResultCollector rc = onServer(cache.getDefaultPool()).execute(writeFunction.getId());
      assertThat(((ArrayList) rc.getResult()).get(0)).isEqualTo(WriteFunction.SUCCESS_OUTPUT);

      // can not read
      assertNotAuthorized(() -> onServer(cache.getDefaultPool()).execute(readFunction.getId()),
          "DATA:READ");
    });
  }

  @Test
  // this would trigger the client to send a GetFunctionAttribute command before executing it
  public void testExecuteFunctionWithOutClientRegistration() {
    client1.invoke("logging in with dataReader", () -> {
      ClientCache cache = createClientCache("dataRead", "dataRead", server.getPort());
      assertNotAuthorized(() -> onServer(cache.getDefaultPool()).execute(writeFunction.getId()),
          "DATA:WRITE");
    });
  }


}
