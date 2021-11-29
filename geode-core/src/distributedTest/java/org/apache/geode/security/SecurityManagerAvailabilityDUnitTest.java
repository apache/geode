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
import static org.apache.geode.internal.security.SecurityServiceFactory.TEST_SECURITY_SERVICE_SYSTEM_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;

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

    SerializableFunction<ServerStarterRule> severStarterFunction = s -> s
        .withSecurityManager(ExpirableSecurityManager.class)
        .withProperty(SECURITY_LOG_LEVEL, "debug")
        .withCredential("test", "test")
        .withConnectionToLocator(locatorPort)
        .withRegion(RegionShortcut.REPLICATE, "region")
        .withSystemProperty(TEST_SECURITY_SERVICE_SYSTEM_PROPERTY,
            TestIntegratedSecurityService.class.getName());

    server1 = clusterStartupRule.startServerVM(1, severStarterFunction);
    server2 = clusterStartupRule.startServerVM(2, severStarterFunction);

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
    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      UpdatableUserAuthInitialize.setUser("data1");
      Region<Object, Object> region =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");

      // even though we throw exceptions in the security service, the client will retry another
      // server, so client wouldn't see any exceptions
      IntStream.range(0, 1000).forEach(i -> region.put(i, "value" + i));
    });

    Integer timeFailedOnServer1 = server1.invoke(
        SecurityManagerAvailabilityDUnitTest::getTimeFailed);
    Integer timeFailedOnServer2 = server2.invoke(
        SecurityManagerAvailabilityDUnitTest::getTimeFailed);

    assertThat(timeFailedOnServer1).isGreaterThan(0);
    assertThat(timeFailedOnServer2).isGreaterThan(0);

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region<Object, Object> region = cache.getRegion("/region");
      assertThat(region.size()).isEqualTo(1000);
    });
  }

  private static Integer getTimeFailed() {
    InternalCache cache = ClusterStartupRule.getCache();
    TestIntegratedSecurityService securityService =
        (TestIntegratedSecurityService) cache.getSecurityService();
    return securityService.getTimesFailed();
  }
}
