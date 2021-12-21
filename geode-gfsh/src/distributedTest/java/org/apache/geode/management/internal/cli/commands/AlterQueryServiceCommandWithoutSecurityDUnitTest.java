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

import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.AUTHORIZER_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction.SECURITY_NOT_ENABLED_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.query.internal.QueryConfigurationService;
import org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class AlterQueryServiceCommandWithoutSecurityDUnitTest {
  private static MemberVM locator;
  private static final List<MemberVM> servers = new ArrayList<>();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void setUp() throws Exception {
    locator = cluster.startLocatorVM(0);
    IntStream.range(0, 2)
        .forEach(i -> servers.add(i, cluster.startServerVM(i + 1, locator.getPort())));
    gfsh.connectAndVerify(locator);
  }

  private void verifyNoOpAuthorizer() {
    servers.forEach(s -> s.invoke(() -> {
      MethodInvocationAuthorizer methodAuthorizer = Objects
          .requireNonNull(ClusterStartupRule.getCache()).getService(QueryConfigurationService.class)
          .getMethodAuthorizer();
      assertThat(methodAuthorizer).isEqualTo(QueryConfigurationServiceImpl.getNoOpAuthorizer());
    }));
  }

  @Test
  public void alterQueryServiceCommandDoesNotUpdateMethodAuthorizerWhenSecurityIsNotEnabled() {
    verifyNoOpAuthorizer();
    String authorizerName = UnrestrictedMethodAuthorizer.class.getName();

    gfsh.executeAndAssertThat(COMMAND_NAME + " --" + AUTHORIZER_NAME + "=" + authorizerName)
        .statusIsError().containsOutput(SECURITY_NOT_ENABLED_MESSAGE);
    verifyNoOpAuthorizer();
  }
}
