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

import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.AUTHORIZER_PARAMETERS;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.METHOD_AUTHORIZER_NAME;
import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.ALL_METHODS_ALLOWED;
import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.AUTHORIZER_CLASS_NAME;
import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.QUERY_SERVICE_DATA_SECTION;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.query.security.JavaBeanAccessorMethodAuthorizer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class DescribeQueryServiceCommandDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void commandReturnsCorrectInformationAboutMethodAuthorizerWhenSecurityIsNotEnabled()
      throws Exception {
    MemberVM locator = cluster.startLocatorVM(0);
    cluster.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat(COMMAND_NAME).statusIsSuccess()
        .hasDataSection(QUERY_SERVICE_DATA_SECTION).hasContent()
        .containsEntry(AUTHORIZER_CLASS_NAME, ALL_METHODS_ALLOWED);
  }

  @Test
  public void commandReturnsCorrectInformationAboutMethodAuthorizerWhenSecurityIsEnabledAndNoServersAreAvailable()
      throws Exception {
    MemberVM locator =
        cluster.startLocatorVM(0, l -> l.withSecurityManager(SimpleSecurityManager.class));
    gfsh.connectAndVerify(locator);

    changeMethodAuthorizerAndValidateDescribe();
  }

  @Test
  public void commandReturnsCorrectInformationAboutMethodAuthorizerWhenSecurityIsEnabled()
      throws Exception {
    MemberVM locator =
        cluster.startLocatorVM(0, l -> l.withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();
    cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withCredential("clusterManage", "clusterManage"));
    gfsh.connectAndVerify(locator);

    changeMethodAuthorizerAndValidateDescribe();
  }

  @Test
  public void commandReturnsCorrectInformationAboutMethodAuthorizerWhenSecurityIsEnabledAndClusterConfigIsDisabled()
      throws Exception {
    MemberVM locator =
        cluster.startLocatorVM(0, l -> l.withoutClusterConfigurationService()
            .withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();
    cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withSecurityManager(SimpleSecurityManager.class)
        .withCredential("clusterManage", "clusterManage"));
    gfsh.connectAndVerify(locator);

    changeMethodAuthorizerAndValidateDescribe();
  }

  private void changeMethodAuthorizerAndValidateDescribe() {
    String authorizerName = JavaBeanAccessorMethodAuthorizer.class.getName();
    String parameters = "param1;param2;param3";
    String alterQueryService = AlterQueryServiceCommand.COMMAND_NAME + " --"
        + METHOD_AUTHORIZER_NAME + "=" + authorizerName
        + " --" + AUTHORIZER_PARAMETERS + "=" + parameters;

    gfsh.executeAndAssertThat(alterQueryService).statusIsSuccess();

    gfsh.executeAndAssertThat(COMMAND_NAME).statusIsSuccess()
        .hasDataSection(QUERY_SERVICE_DATA_SECTION).hasContent()
        .containsEntry(AUTHORIZER_CLASS_NAME, authorizerName);
  }
}
