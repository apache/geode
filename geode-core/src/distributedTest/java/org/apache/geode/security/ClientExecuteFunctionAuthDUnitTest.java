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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.security.TestFunctions.ReadFunction;
import org.apache.geode.management.internal.security.TestFunctions.WriteFunction;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({SecurityTest.class})
public class ClientExecuteFunctionAuthDUnitTest {
  private static Function writeFunction;
  private static Function readFunction;

  private static MemberVM server;
  private static ClientVM client1, client2;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    properties.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.management.internal.security.TestFunctions*");
    server = cluster.startServerVM(0, properties);

    server.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region");
    });
    int serverPort = server.getPort();
    client1 = cluster.startClientVM(1, c1 -> c1.withCredential("dataRead", "dataRead")
        .withPoolSubscription(true)
        .withServerConnection(serverPort));
    client2 = cluster.startClientVM(2, c -> c.withCredential("dataWrite", "dataWrite")
        .withPoolSubscription(true)
        .withServerConnection(serverPort));

    VMProvider.invokeInEveryMember(() -> {
      writeFunction = new WriteFunction();
      readFunction = new ReadFunction();
    }, server, client1, client2);
  }

  @Test
  public void testExecuteFunctionWithFunctionObject() throws Exception {
    client1.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();

      // can not write
      assertThatThrownBy(() -> onServer(cache.getDefaultPool()).execute(writeFunction))
          .hasMessageContaining("DATA:WRITE");

      // can read
      ResultCollector rc = onServer(cache.getDefaultPool()).execute(readFunction);
      assertThat(((ArrayList) rc.getResult()).get(0)).isEqualTo(ReadFunction.SUCCESS_OUTPUT);
    });

    client2.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      // can write
      ResultCollector rc = onServer(cache.getDefaultPool()).execute(writeFunction);
      assertThat(((ArrayList) rc.getResult()).get(0)).isEqualTo(WriteFunction.SUCCESS_OUTPUT);

      // can not read
      assertThatThrownBy(() -> onServer(cache.getDefaultPool()).execute(readFunction))
          .hasMessageContaining("DATA:READ");
    });
  }

}
