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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.CONTINUOUS_QUERIES_RUNNING_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.AUTHORIZER_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.FORCE_UPDATE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Objects;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.QueryConfigurationService;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.query.TestCqListener;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class AlterQueryServiceCommandDistributedTest {
  private static final String REPLICATE_REGION_NAME = "ReplicateRegion";
  private static final String PARTITION_REGION_NAME = "PartitionRegion";
  private static final Class DEFAULT_AUTHORIZER_CLASS = RestrictedMethodAuthorizer.class;
  private MemberVM server;
  private ClientVM client;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void setUp() throws Exception {
    MemberVM locator =
        cluster.startLocatorVM(0, l -> l.withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();
    server = cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort).withCredential("cluster", "cluster"));

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + REPLICATE_REGION_NAME + " --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=" + PARTITION_REGION_NAME + " --type=PARTITION")
        .statusIsSuccess();
    locator.invoke(() -> {
      ClusterStartupRule.memberStarter
          .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + REPLICATE_REGION_NAME, 1);
      ClusterStartupRule.memberStarter
          .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + PARTITION_REGION_NAME, 1);
    });

    client = cluster.startClientVM(2, ccf -> ccf.withCredential("data", "data")
        .withPoolSubscription(true).withLocatorConnection(locatorPort));
  }

  @SuppressWarnings("unused")
  private Object[] getRegionNameAndCqExecutionType() {
    return new Object[] {
        new Object[] {REPLICATE_REGION_NAME, true},
        new Object[] {REPLICATE_REGION_NAME, false},
        new Object[] {PARTITION_REGION_NAME, true},
        new Object[] {PARTITION_REGION_NAME, false},
    };
  }

  private String buildCommand(String authorizerName, Boolean forceUpdate) {
    StringBuilder commandBuilder = new StringBuilder(COMMAND_NAME);
    commandBuilder.append(" --").append(AUTHORIZER_NAME).append("=").append(authorizerName);

    if (forceUpdate != null) {
      commandBuilder.append(" --").append(FORCE_UPDATE).append("=").append(forceUpdate);
    }

    return commandBuilder.toString();
  }

  private void verifyCurrentAuthorizerClass(Class authorizerClass) {
    server.invoke(() -> {
      MethodInvocationAuthorizer methodAuthorizer =
          Objects.requireNonNull(ClusterStartupRule.getCache())
              .getService(QueryConfigurationService.class).getMethodAuthorizer();
      assertThat(methodAuthorizer.getClass()).isEqualTo(authorizerClass);
    });
  }

  private void createClientCq(String queryString, boolean executeWithInitialResults) {
    client.invoke(() -> {
      TestCqListener cqListener = new TestCqListener();
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
      cqAttributesFactory.addCqListener(cqListener);

      CqQuery cq = queryService.newCq(queryString, cqAttributesFactory.create());
      if (!executeWithInitialResults) {
        cq.execute();
      } else {
        assertThat(cq.executeWithInitialResults().size()).isEqualTo(0);
      }
    });
  }

  @Test
  @Parameters(method = "getRegionNameAndCqExecutionType")
  @TestCaseName("[{index}] {method}(RegionName:{0};ExecuteWithInitialResults:{1})")
  public void commandShouldFailWhenThereAreCqsRunningAndForceUpdateFlagIsFalse(String regionName,
      boolean initialResults) {
    verifyCurrentAuthorizerClass(DEFAULT_AUTHORIZER_CLASS);
    createClientCq("SELECT * FROM " + SEPARATOR + regionName, initialResults);
    String command = buildCommand(DummyMethodAuthorizer.class.getName(), false);

    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(CONTINUOUS_QUERIES_RUNNING_MESSAGE);
    verifyCurrentAuthorizerClass(DEFAULT_AUTHORIZER_CLASS);
  }

  @Test
  @Parameters(method = "getRegionNameAndCqExecutionType")
  @TestCaseName("[{index}] {method}(RegionName:{0};ExecuteWithInitialResults:{1})")
  public void commandShouldSucceedWhenThereAreCqsRunningAndForceUpdateFlagIsTrue(String regionName,
      boolean initialResults) {
    verifyCurrentAuthorizerClass(DEFAULT_AUTHORIZER_CLASS);
    createClientCq("SELECT * FROM " + SEPARATOR + regionName, initialResults);
    String command = buildCommand(DummyMethodAuthorizer.class.getName(), true);

    gfsh.executeAndAssertThat(command).statusIsSuccess();
    verifyCurrentAuthorizerClass(DummyMethodAuthorizer.class);
  }

  public static class DummyMethodAuthorizer implements MethodInvocationAuthorizer {
    @Override
    public boolean authorize(Method method, Object target) {
      return true;
    }
  }
}
