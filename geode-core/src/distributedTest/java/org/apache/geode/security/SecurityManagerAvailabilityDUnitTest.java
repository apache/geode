/*
 *
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
 *
 */

package org.apache.geode.security;


import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.internal.security.SecurityServiceFactory.SECURITY_SERVICE_SYSTEM_PROPERTY;
import static org.apache.geode.security.TestIntegratedSecurityService.FAIL_TIMES;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.SerializableFunction;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({SecurityTest.class})
public class SecurityManagerAvailabilityDUnitTest {
  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  private MemberVM server1;
  private MemberVM server2;
  private ClientVM clientVM;

  @Before
  public void setup() throws Exception {
    MemberVM locatorVM =
        clusterStartupRule.startLocatorVM(0,
            l -> l.withSecurityManager(ExpirableSecurityManager.class)
                .withProperty(SECURITY_LOG_LEVEL, "debug"));
    int locatorPort = locatorVM.getPort();

    SerializableFunction<ServerStarterRule> serverStarterRuleSerializableFunction = s -> s
        .withSecurityManager(ExpirableSecurityManager.class)
        .withProperty(SECURITY_LOG_LEVEL, "debug")
        .withCredential("test", "test")
        .withConnectionToLocator(locatorPort)
        .withSystemProperty(SECURITY_SERVICE_SYSTEM_PROPERTY,
            TestIntegratedSecurityService.class.getName());

    server1 = clusterStartupRule.startServerVM(1, serverStarterRuleSerializableFunction);
    server2 = clusterStartupRule.startServerVM(2, serverStarterRuleSerializableFunction);

    VMProvider.invokeInEveryMember(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      TestIntegratedSecurityService securityService =
          (TestIntegratedSecurityService) cache.getSecurityService();
      securityService.setFailInterval(50);
      cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
    }, server1, server2);

    int serverVM0Port = server1.getPort();
    int serverVM1Port = server2.getPort();
    clientVM = clusterStartupRule.startClientVM(3,
        c -> c.withServerConnection(serverVM0Port, serverVM1Port)
            .withPoolSubscription(true)
            .withProperty(SECURITY_LOG_LEVEL, "debug")
            .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName()));
  }

  @Test
  public void confirmSecurityManagerAvailabilityExceptionDoesNotStopClientPutOperations() {
    int tries = 1000;
    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      UpdatableUserAuthInitialize.setUser("data1");
      Region<Object, Object> region =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");

      List<Object> returns = new ArrayList<>(2);
      returns.add(new Exception("no exception"));
      int count = 0;
      // even though we throw out exceptions in the security service, the client will retry another
      // server, so client wouldn't see any exceptions
      for (; count < tries; count++) {
        region.put(count, "value" + count);
      }
    });

    Map<String, Object> resultMap0 = server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      TestIntegratedSecurityService securityService =
          (TestIntegratedSecurityService) cache.getSecurityService();
      return securityService.getGetSubjectFailInformation();
    });

    Map<String, Object> resultMap1 = server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      TestIntegratedSecurityService securityService =
          (TestIntegratedSecurityService) cache.getSecurityService();
      return securityService.getGetSubjectFailInformation();
    });

    assertThat((Integer) resultMap0.get(FAIL_TIMES)).isGreaterThan(0);
    assertThat((Integer) resultMap1.get(FAIL_TIMES)).isGreaterThan(0);
  }
}
