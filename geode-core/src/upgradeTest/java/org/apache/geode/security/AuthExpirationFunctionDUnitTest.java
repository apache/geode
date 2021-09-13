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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.test.version.VersionManager.CURRENT_VERSION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.management.internal.security.TestFunctions;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class AuthExpirationFunctionDUnitTest {
  private static final String RELEASE_VERSION = "1.13.3";

  private static Function<Object> writeFunction;

  @Parameterized.Parameter
  public String clientVersion;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    // only test the current version and the latest released version
    return Arrays.asList(CURRENT_VERSION, RELEASE_VERSION);
  }

  private MemberVM serverVM;
  private ClientVM clientVM;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SECURITY_MANAGER, ExpirableSecurityManager.class.getName());
    properties.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.management.internal.security.TestFunctions*");
    serverVM = lsRule.startServerVM(0, properties);

    serverVM.invoke(() -> {
      Objects.requireNonNull(ClusterStartupRule.getCache())
          .createRegionFactory(RegionShortcut.REPLICATE).create("region");
    });
    int serverPort = serverVM.getPort();
    clientVM = lsRule.startClientVM(1, clientVersion, c1 -> c1
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withServerConnection(serverPort));

    VMProvider.invokeInEveryMember(() -> writeFunction = new TestFunctions.WriteFunction(),
        serverVM, clientVM);
  }

  @Test
  public void clientShouldReAuthenticateWhenCredentialExpiredAndFunctionExecutionSucceed() {
    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      UpdatableUserAuthInitialize.setUser("data1");
      ResultCollector rc = onServer(clientCache.getDefaultPool()).execute(writeFunction);
      assertThat(((ArrayList) rc.getResult()).get(0))
          .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);
    });

    // expire the current user
    serverVM.invoke(() -> getSecurityManager().addExpiredUser("data1"));

    // do a second function execution, if this is successful, it means new credentials are provided
    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      UpdatableUserAuthInitialize.setUser("data2");
      ResultCollector rc = onServer(clientCache.getDefaultPool()).execute(writeFunction);
      assertThat(((ArrayList) rc.getResult()).get(0))
          .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);
    });

    // all put operation succeeded
    serverVM.invoke(() -> {
      ExpirableSecurityManager securityManager = getSecurityManager();
      assertThat(securityManager.getExpiredUsers().size()).isEqualTo(1);
      assertThat(securityManager.getExpiredUsers().contains("data1")).isTrue();
      Map<String, List<String>> authorizedOps = securityManager.getAuthorizedOps();
      assertThat(authorizedOps.get("data1")).asList().hasSize(1);
      assertThat(authorizedOps.get("data1")).asList().containsExactly("DATA:WRITE");
      assertThat(authorizedOps.get("data2")).asList().hasSize(1);
      assertThat(authorizedOps.get("data2")).asList().containsExactly("DATA:WRITE");
      Map<String, List<String>> unauthorizedOps = securityManager.getUnAuthorizedOps();
      assertThat(unauthorizedOps.get("data1")).asList().hasSize(1);
      assertThat(unauthorizedOps.get("data1")).asList().containsExactly("DATA:WRITE");
      return authorizedOps.get("data1");
    });
  }

  private static ExpirableSecurityManager getSecurityManager() {
    return (ExpirableSecurityManager) ClusterStartupRule.getCache().getSecurityService()
        .getSecurityManager();
  }
}
