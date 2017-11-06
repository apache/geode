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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.management.internal.security.TestFunctions.ReadFunction;
import static org.apache.geode.test.junit.rules.GfshShellConnectionRule.PortType.http;
import static org.apache.geode.test.junit.rules.GfshShellConnectionRule.PortType.jmxManager;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.management.internal.security.TestFunctions.WriteFunction;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;

@Category(DistributedTest.class)
public class ExecuteFunctionCommandWithSecurityDUnitTest {
  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties locatorProps = new Properties();
    locatorProps.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    locator = lsRule.startLocatorVM(0, locatorProps);

    Properties serverProps = new Properties();
    serverProps.setProperty(ResourceConstants.USER_NAME, "clusterManage");
    serverProps.setProperty(ResourceConstants.PASSWORD, "clusterManage");
    MemberVM server = lsRule.startServerVM(1, serverProps, locator.getPort());

    server.invoke(() -> {
      FunctionService.registerFunction(new ReadFunction());
      FunctionService.registerFunction(new WriteFunction());
    });
  }

  @Test
  public void executeOverHttp() throws Exception {
    givenReadOnlyConnectionOverHttp();

    executeReadFunctionIsSuccessful();
  }

  @Test
  public void executeOverJmx() throws Exception {
    givenReadOnlyConnectionOverJmx();

    executeReadFunctionIsSuccessful();
  }

  @Test
  public void failOverHttpWithInvalidPermissions() throws Exception {
    givenReadOnlyConnectionOverHttp();

    executeWriteFunctionThrowsError();
  }

  @Test
  public void failOverJmxWithInvalidPermissions() throws Exception {
    givenReadOnlyConnectionOverJmx();

    executeWriteFunctionThrowsError();
  }

  private void executeReadFunctionIsSuccessful() {
    gfsh.executeAndAssertThat("execute function --id=" + new ReadFunction().getId())
        .statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains(ReadFunction.SUCCESS_OUTPUT);
  }

  private void executeWriteFunctionThrowsError() {
    gfsh.executeAndAssertThat("execute function --id=" + new WriteFunction().getId())
        .statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("dataRead not authorized for DATA:WRITE");
  }

  private void givenReadOnlyConnectionOverHttp() throws Exception {
    gfsh.secureConnectAndVerify(locator.getHttpPort(), http, "dataRead", "dataRead");
  }

  private void givenReadOnlyConnectionOverJmx() throws Exception {
    gfsh.secureConnectAndVerify(locator.getJmxPort(), jmxManager, "dataRead", "dataRead");
  }
}
