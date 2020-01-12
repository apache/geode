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

import static org.apache.geode.cache.execute.FunctionService.onRegion;
import static org.apache.geode.cache.execute.FunctionService.registerFunction;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.management.internal.security.TestFunctions.ReadFunction;
import static org.apache.geode.security.SecurityTestUtil.assertNotAuthorized;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class ClientExecuteRegionFunctionAuthDUnitTest extends JUnit4DistributedTestCase {

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  private Function readFunction;

  @Before
  public void before() {
    readFunction = new ReadFunction();
    registerFunction(readFunction);
  }

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName())
      .withRegion(RegionShortcut.REPLICATE, "RegionA");

  @Test
  public void testExecuteRegionFunction() {

    client1.invoke("logging in with dataReader", () -> {
      ClientCache cache = createClientCache("dataRead", "dataRead", server.getPort());

      Region region = createProxyRegion(cache, "RegionA");
      registerFunction(readFunction);
      ResultCollector rc = onRegion(region).execute(readFunction.getId());
      assertThat(((ArrayList) rc.getResult()).get(0)).isEqualTo(ReadFunction.SUCCESS_OUTPUT);
    });

    client2.invoke("logging in with another region's reader", () -> {
      ClientCache cache = createClientCache("dataReadRegionB", "dataReadRegionB", server.getPort());

      Region region = createProxyRegion(cache, "RegionA");
      registerFunction(readFunction);
      assertNotAuthorized(() -> onRegion(region).execute(readFunction.getId()),
          "DATA:READ:RegionA");
    });
  }
}
