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

import static org.apache.geode.cache.execute.FunctionService.onRegion;
import static org.apache.geode.cache.execute.FunctionService.onServer;
import static org.apache.geode.cache.execute.FunctionService.onServers;
import static org.apache.geode.cache.query.dunit.SecurityTestUtils.collectSecurityManagers;
import static org.apache.geode.cache.query.dunit.SecurityTestUtils.getSecurityManager;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.security.SecurityManager.PASSWORD;
import static org.apache.geode.security.SecurityManager.USER_NAME;
import static org.apache.geode.test.version.VersionManager.CURRENT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.management.internal.security.TestFunctions;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ClientCacheRule;
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

  private MemberVM serverVM0;
  private MemberVM serverVM1;
  private MemberVM serverVM2;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Before
  public void setup() {
    MemberVM locatorVM =
        clusterStartupRule.startLocatorVM(0,
            l -> l.withSecurityManager(ExpirableSecurityManager.class));
    int locatorPort = locatorVM.getPort();

    Properties serverProperties = new Properties();
    serverProperties.setProperty(SECURITY_MANAGER, ExpirableSecurityManager.class.getName());
    serverProperties.setProperty(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.management.internal.security.TestFunctions*");
    serverProperties.setProperty(USER_NAME, "test");
    serverProperties.setProperty(PASSWORD, "test");

    serverVM0 = clusterStartupRule.startServerVM(1, serverProperties, locatorPort);
    serverVM1 = clusterStartupRule.startServerVM(2, serverProperties, locatorPort);
    serverVM2 = clusterStartupRule.startServerVM(3, serverProperties, locatorPort);

    VMProvider.invokeInEveryMember(() -> {
      Objects.requireNonNull(ClusterStartupRule.getCache())
          .createRegionFactory(RegionShortcut.REPLICATE).create("region");
    }, serverVM0, serverVM1, serverVM2);

    VMProvider.invokeInEveryMember(() -> writeFunction = new TestFunctions.WriteFunction(),
        serverVM0, serverVM1, serverVM2);

    clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withLocatorConnection(locatorPort);
  }

  @Test
  public void clientShouldReAuthenticateWhenCredentialExpiredAndFunctionExecutionOnServerSucceed()
      throws Exception {
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("data1");
    writeFunction = new TestFunctions.WriteFunction();

    ResultCollector rc = onServer(clientCache.getDefaultPool()).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);

    // expire the current user
    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("data1"),
        serverVM0, serverVM1, serverVM2);

    // do a second function execution, if this is successful, it means new credentials are provided
    UpdatableUserAuthInitialize.setUser("data2");
    rc = onServer(clientCache.getDefaultPool()).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);


    ExpirableSecurityManager consolidated =
        collectSecurityManagers(serverVM0, serverVM1, serverVM2);
    Set<String> combinedExpiredUsers = consolidated.getExpiredUsers();
    assertThat(combinedExpiredUsers).containsExactly("data1");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("data1")).containsExactly("DATA:WRITE");
    assertThat(authorizedOps.get("data2")).containsExactly("DATA:WRITE");

    Map<String, List<String>> unauthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unauthorizedOps.get("data1")).containsExactly("DATA:WRITE");
  }

  @Test
  public void clientShouldReAuthenticateWhenCredentialExpiredAndFunctionExecutionOnServersSucceed()
      throws Exception {
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("data1");
    writeFunction = new TestFunctions.WriteFunction();

    ResultCollector rc = onServers(clientCache.getDefaultPool()).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);

    // expire the current user
    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("data1"),
        serverVM0, serverVM1, serverVM2);

    // do a second function execution, if this is successful, it means new credentials are provided
    UpdatableUserAuthInitialize.setUser("data2");
    rc = onServers(clientCache.getDefaultPool()).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);

    ExpirableSecurityManager consolidated =
        collectSecurityManagers(serverVM0, serverVM1, serverVM2);
    Set<String> combinedExpiredUsers = consolidated.getExpiredUsers();
    assertThat(combinedExpiredUsers).containsExactly("data1");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("data1"))
        .containsExactly("DATA:WRITE", "DATA:WRITE", "DATA:WRITE");
    assertThat(authorizedOps.get("data2"))
        .containsExactly("DATA:WRITE", "DATA:WRITE", "DATA:WRITE");

    Map<String, List<String>> unauthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unauthorizedOps.get("data1"))
        .containsExactly("DATA:WRITE", "DATA:WRITE", "DATA:WRITE");
  }

  @Test
  public void clientShouldReAuthenticateWhenCredentialExpiredAndFunctionExecutionOnRegionSucceed()
      throws Exception {
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("data1");
    Region<Object, Object> region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
    writeFunction = new TestFunctions.WriteFunction();

    ResultCollector rc = onRegion(region).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);

    // expire the current user
    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("data1"),
        serverVM0, serverVM1, serverVM2);

    // do a second function execution, if this is successful, it means new credentials are provided
    UpdatableUserAuthInitialize.setUser("data2");
    rc = onRegion(region).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);

    ExpirableSecurityManager consolidated =
        collectSecurityManagers(serverVM0, serverVM1, serverVM2);
    Set<String> combinedExpiredUsers = consolidated.getExpiredUsers();
    assertThat(combinedExpiredUsers).containsExactly("data1");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("data1")).containsExactly("DATA:WRITE");
    assertThat(authorizedOps.get("data2")).containsExactly("DATA:WRITE");

    Map<String, List<String>> unauthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unauthorizedOps.get("data1")).containsExactly("DATA:WRITE");
  }

  @Test
  public void clientShouldReAuthenticateWhenCredentialExpiredAndFunctionExecutionOnServerWithRegionServiceSucceed()
      throws Exception {
    clientCacheRule.withMultiUser(true);
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("data1");
    writeFunction = new TestFunctions.WriteFunction();

    Properties userSecurityProperties = new Properties();
    userSecurityProperties.put(SECURITY_CLIENT_AUTH_INIT,
        UpdatableUserAuthInitialize.class.getName());
    RegionService regionService = clientCache.createAuthenticatedView(userSecurityProperties);

    ResultCollector rc = onServer(regionService).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);

    // expire the current user
    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("data1"),
        serverVM0, serverVM1, serverVM2);

    // do a second function execution, if this is successful, it means new credentials are provided
    UpdatableUserAuthInitialize.setUser("data2");
    rc = onServer(regionService).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);

    ExpirableSecurityManager consolidated =
        collectSecurityManagers(serverVM0, serverVM1, serverVM2);
    Set<String> combinedExpiredUsers = consolidated.getExpiredUsers();
    assertThat(combinedExpiredUsers).containsExactly("data1");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("data1")).containsExactly("DATA:WRITE");
    assertThat(authorizedOps.get("data2")).containsExactly("DATA:WRITE");

    Map<String, List<String>> unauthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unauthorizedOps.get("data1")).containsExactly("DATA:WRITE");
  }

  @Test
  public void clientShouldReAuthenticateWhenCredentialExpiredAndFunctionExecutionOnServersWithRegionServiceSucceed()
      throws Exception {
    clientCacheRule.withMultiUser(true);
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("data1");
    writeFunction = new TestFunctions.WriteFunction();

    Properties userSecurityProperties = new Properties();
    userSecurityProperties.put(SECURITY_CLIENT_AUTH_INIT,
        UpdatableUserAuthInitialize.class.getName());
    RegionService regionService = clientCache.createAuthenticatedView(userSecurityProperties);

    ResultCollector rc = onServers(regionService).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);

    // expire the current user
    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("data1"),
        serverVM0, serverVM1, serverVM2);

    // do a second function execution, if this is successful, it means new credentials are provided
    UpdatableUserAuthInitialize.setUser("data2");
    rc = onServers(regionService).execute(writeFunction);
    assertThat(((List) rc.getResult()).get(0))
        .isEqualTo(TestFunctions.WriteFunction.SUCCESS_OUTPUT);


    ExpirableSecurityManager consolidated =
        collectSecurityManagers(serverVM0, serverVM1, serverVM2);

    Set<String> combinedExpiredUsers = consolidated.getExpiredUsers();
    assertThat(combinedExpiredUsers).containsExactly("data1");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("data1"))
        .containsExactly("DATA:WRITE", "DATA:WRITE", "DATA:WRITE");
    assertThat(authorizedOps.get("data2"))
        .containsExactly("DATA:WRITE", "DATA:WRITE", "DATA:WRITE");

    Map<String, List<String>> unauthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unauthorizedOps.get("data1"))
        .containsExactly("DATA:WRITE", "DATA:WRITE", "DATA:WRITE");
  }
}
