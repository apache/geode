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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({SecurityTest.class, FunctionServiceTest.class})
public class MultiUserAuthenticationFunctionServiceDUnitTest {
  private static MemberVM locator, server;
  private static Properties clientProperties;

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    IgnoredException.addIgnoredException("org.apache.geode.security.AuthenticationFailedException");

    // Start Cluster
    Properties locatorProps = new Properties();
    locatorProps.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getCanonicalName());
    locator = lsRule.startLocatorVM(0, locatorProps);
    Properties serverProps = new Properties();
    serverProps.setProperty("security-username", "cluster");
    serverProps.setProperty("security-password", "cluster");
    server = lsRule.startServerVM(1, serverProps, locator.getPort());

    // Create region and deploy function
    gfsh.connectAndVerify(locator);
    server.invoke(() -> FunctionService.registerFunction(new TestFunction()));
    locator.invoke(() -> FunctionService.registerFunction(new TestFunction()));
    gfsh.executeAndAssertThat("create region --name=testRegion --type=REPLICATE").statusIsSuccess();
    gfsh.executeAndAssertThat("execute function --id=TestFunction").statusIsSuccess()
        .containsOutput("TestFunction Executed!");

    // Default Client Properties
    clientProperties = new Properties();
    clientProperties.setProperty(UserPasswordAuthInit.USER_NAME, "stranger");
    clientProperties.setProperty(UserPasswordAuthInit.PASSWORD, "stranger");
    clientProperties.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());
    clientProperties.setProperty(LOCATORS, "");
    clientProperties.setProperty(MCAST_PORT, "0");
  }

  static Properties getClientSecurityProperties(String username, String password) {
    Properties properties = new Properties(clientProperties);
    properties.setProperty(UserPasswordAuthInit.USER_NAME, username);
    properties.setProperty(UserPasswordAuthInit.PASSWORD, password);

    return properties;
  }

  @Test
  public void multiUserAuthenticatedFunctionExecutionsOnServerShouldBeAuthorizedByServers() {
    int locatorPort = locator.getPort();
    ClientCache clientCache = new ClientCacheFactory(clientProperties)
        .setPoolMultiuserAuthentication(true).addPoolLocator("localhost", locatorPort).create();
    RegionService authorizedRegionService =
        clientCache.createAuthenticatedView(getClientSecurityProperties("data", "data"));
    RegionService unauthorizedRegionService =
        clientCache.createAuthenticatedView(getClientSecurityProperties("cluster", "cluster"));

    assertThat(FunctionService.onServer(authorizedRegionService).execute(new TestFunction())
        .getResult().toString()).isEqualTo("[TestFunction Executed!]");
    assertThat(FunctionService.onServer(authorizedRegionService).execute("TestFunction").getResult()
        .toString()).isEqualTo("[TestFunction Executed!]");

    assertThatThrownBy(
        () -> FunctionService.onServer(unauthorizedRegionService).execute(new TestFunction()))
            .isInstanceOf(ServerOperationException.class)
            .hasCauseExactlyInstanceOf(NotAuthorizedException.class);
    assertThatThrownBy(
        () -> FunctionService.onServer(unauthorizedRegionService).execute("TestFunction"))
            .isInstanceOf(ServerOperationException.class)
            .hasCauseExactlyInstanceOf(NotAuthorizedException.class);

    authorizedRegionService.close();
    unauthorizedRegionService.close();
    clientCache.close();
  }

  @Test
  public void multiUserAuthenticatedFunctionExecutionsOnRegionShouldBeAuthorizedByServers() {
    int locatorPort = locator.getPort();
    ClientCache clientCache = new ClientCacheFactory(clientProperties)
        .setPoolMultiuserAuthentication(true).addPoolLocator("localhost", locatorPort).create();
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("testRegion");
    RegionService authorizedRegionService =
        clientCache.createAuthenticatedView(getClientSecurityProperties("data", "data"));
    RegionService unauthorizedRegionService =
        clientCache.createAuthenticatedView(getClientSecurityProperties("cluster", "cluster"));
    Region authorizedRegion = authorizedRegionService.getRegion("testRegion");
    Region unauthorizedRegion = unauthorizedRegionService.getRegion("testRegion");

    assertThat(FunctionService.onRegion(authorizedRegion).execute(new TestFunction()).getResult()
        .toString()).isEqualTo("[TestFunction Executed!]");
    assertThat(
        FunctionService.onRegion(authorizedRegion).execute("TestFunction").getResult().toString())
            .isEqualTo("[TestFunction Executed!]");
    assertThatThrownBy(
        () -> FunctionService.onRegion(unauthorizedRegion).execute(new TestFunction()))
            .isInstanceOf(FunctionException.class)
            .hasCauseExactlyInstanceOf(ServerOperationException.class)
            .hasMessageContaining("NotAuthorizedException");
    assertThatThrownBy(() -> FunctionService.onRegion(unauthorizedRegion).execute("TestFunction"))
        .isInstanceOf(FunctionException.class)
        .hasCauseExactlyInstanceOf(ServerOperationException.class)
        .hasMessageContaining("NotAuthorizedException");

    authorizedRegionService.close();
    unauthorizedRegionService.close();
    clientCache.close();
  }

  public static class TestFunction implements Function, DataSerializable {
    @SuppressWarnings("unchecked")
    public void execute(FunctionContext context) {
      context.getResultSender().lastResult("TestFunction Executed!");
    }

    @Override
    public String getId() {
      return getClass().getSimpleName();
    }

    @Override
    public void toData(DataOutput out) throws IOException {}

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {}
  }
}
