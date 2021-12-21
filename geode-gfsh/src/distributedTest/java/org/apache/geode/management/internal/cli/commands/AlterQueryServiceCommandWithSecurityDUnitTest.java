/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.AUTHORIZER_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.AUTHORIZER_PARAMETERS;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.NO_MEMBERS_FOUND_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.SPLITTING_REGEX;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.cache.query.internal.QueryConfigurationService;
import org.apache.geode.cache.query.security.JavaBeanAccessorMethodAuthorizer;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RegExMethodAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.cli.util.TestMethodAuthorizer;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class AlterQueryServiceCommandWithSecurityDUnitTest {
  private static final Class<RestrictedMethodAuthorizer> DEFAULT_AUTHORIZER_CLASS =
      RestrictedMethodAuthorizer.class;
  private static final String TEST_AUTHORIZER_TXT = "TestMethodAuthorizer.txt";
  private static final String USER_AUTHORIZER_PARAMETERS = "param1;param2;param3";
  private static final String JAVA_BEAN_AUTHORIZER_PARAMETERS = "java.lang;java.util";
  // A regex containing a comma to confirm that '--authorizer-parameter' is correctly parsed
  private static final String REGEX_AUTHORIZER_PARAMETERS =
      "^java.util.List..{4,8}$;^java.util.Set..{4,8}$";
  private MemberVM locator;
  private final List<MemberVM> servers = new ArrayList<>();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void setUp() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();
    IntStream.range(0, 2).forEach(i -> servers.add(i, cluster.startServerVM(i + 1, s -> s
        .withConnectionToLocator(locatorPort).withCredential("clusterManage", "clusterManage"))));
    gfsh.connectAndVerify(locator);
  }

  private String buildCommand(String authorizerName, String authorizerParameters) {
    StringBuilder commandBuilder = new StringBuilder(COMMAND_NAME);
    commandBuilder.append(" --").append(AUTHORIZER_NAME).append("=").append(authorizerName);

    if (authorizerParameters != null) {
      commandBuilder.append(" --").append(AUTHORIZER_PARAMETERS).append("=")
          .append(authorizerParameters);
    }

    return commandBuilder.toString();
  }

  private void executeAndAssertGfshCommandIsSuccessful(
      Class<? extends MethodInvocationAuthorizer> authorizerClass,
      String authorizerParameters) {
    gfsh.executeAndAssertThat(buildCommand(authorizerClass.getName(), authorizerParameters))
        .statusIsSuccess().hasTableSection().hasRowSize(servers.size());

    verifyCurrentAuthorizerClass(authorizerClass);
  }

  private void startServerAndVerifyJavaBeanAccessorMethodAuthorizer() {
    int locatorPort = locator.getPort();
    MemberVM newServer = cluster.startServerVM(servers.size() + 1, s -> s
        .withConnectionToLocator(locatorPort).withCredential("clusterManage", "clusterManage"));
    newServer.invoke(() -> {
      JavaBeanAccessorMethodAuthorizer methodAuthorizer =
          (JavaBeanAccessorMethodAuthorizer) Objects.requireNonNull(ClusterStartupRule.getCache())
              .getService(QueryConfigurationService.class).getMethodAuthorizer();
      assertThat(methodAuthorizer.getClass()).isEqualTo(JavaBeanAccessorMethodAuthorizer.class);
      Set<String> expectedParameters =
          new HashSet<>(Arrays.asList(JAVA_BEAN_AUTHORIZER_PARAMETERS.split(SPLITTING_REGEX)));
      assertThat(methodAuthorizer.getAllowedPackages()).isEqualTo(expectedParameters);
    });
  }

  private void verifyCurrentAuthorizerClass(
      Class<? extends MethodInvocationAuthorizer> authorizerClass) {
    servers.forEach(server -> server.invoke(() -> {
      MethodInvocationAuthorizer methodAuthorizer =
          Objects.requireNonNull(ClusterStartupRule.getCache())
              .getService(QueryConfigurationService.class).getMethodAuthorizer();
      assertThat(methodAuthorizer.getClass()).isEqualTo(authorizerClass);
    }));
  }

  private String getTestMethodAuthorizerFilePath() throws IOException {
    URL url = getClass().getResource(TEST_AUTHORIZER_TXT);
    File textFile = tempFolder.newFile(TEST_AUTHORIZER_TXT);
    FileUtils.copyURLToFile(url, textFile);

    return textFile.getAbsolutePath();
  }

  // This test verifies that the cluster starts with the default method authorizer configured, then
  // changes the currently configured authorizer to each of the out-of-the-box authorizers.
  @Test
  public void alterQueryServiceCommandUpdatesMethodAuthorizerClass() {
    verifyCurrentAuthorizerClass(DEFAULT_AUTHORIZER_CLASS);
    executeAndAssertGfshCommandIsSuccessful(UnrestrictedMethodAuthorizer.class, "");
    executeAndAssertGfshCommandIsSuccessful(JavaBeanAccessorMethodAuthorizer.class,
        JAVA_BEAN_AUTHORIZER_PARAMETERS);
    executeAndAssertGfshCommandIsSuccessful(RegExMethodAuthorizer.class,
        REGEX_AUTHORIZER_PARAMETERS);
    executeAndAssertGfshCommandIsSuccessful(RestrictedMethodAuthorizer.class, "");
  }

  @Test
  public void alterQueryServiceCommandUpdatesMethodAuthorizerParameters() {
    gfsh.executeAndAssertThat(buildCommand(JavaBeanAccessorMethodAuthorizer.class.getName(),
        JAVA_BEAN_AUTHORIZER_PARAMETERS)).statusIsSuccess().hasTableSection()
        .hasRowSize(servers.size());
    servers.forEach(server -> server.invoke(() -> {
      JavaBeanAccessorMethodAuthorizer methodAuthorizer =
          (JavaBeanAccessorMethodAuthorizer) Objects.requireNonNull(ClusterStartupRule.getCache())
              .getService(QueryConfigurationService.class).getMethodAuthorizer();
      Set<String> expectedParameters =
          new HashSet<>(Arrays.asList(JAVA_BEAN_AUTHORIZER_PARAMETERS.split(SPLITTING_REGEX)));
      assertThat(methodAuthorizer.getAllowedPackages()).isEqualTo(expectedParameters);
    }));

    gfsh.executeAndAssertThat(
        buildCommand(RegExMethodAuthorizer.class.getName(), REGEX_AUTHORIZER_PARAMETERS))
        .statusIsSuccess().hasTableSection().hasRowSize(servers.size());
    servers.forEach(server -> server.invoke(() -> {
      RegExMethodAuthorizer methodAuthorizer =
          (RegExMethodAuthorizer) Objects.requireNonNull(ClusterStartupRule.getCache())
              .getService(QueryConfigurationService.class).getMethodAuthorizer();
      Set<String> expectedParameters =
          new HashSet<>(Arrays.asList(REGEX_AUTHORIZER_PARAMETERS.split(SPLITTING_REGEX)));
      assertThat(methodAuthorizer.getAllowedPatterns()).isEqualTo(expectedParameters);
    }));
  }

  @Test
  public void alterQueryServiceCommandWithUserSpecifiedMethodAuthorizerUpdatesMethodAuthorizer()
      throws IOException {
    verifyCurrentAuthorizerClass(DEFAULT_AUTHORIZER_CLASS);
    Class<TestMethodAuthorizer> authorizerClass = TestMethodAuthorizer.class;
    String authorizerName = authorizerClass.getName();
    String classContent =
        new String(Files.readAllBytes(Paths.get(getTestMethodAuthorizerFilePath())));
    String jarFileName = "testJar.jar";
    File jarFile = tempFolder.newFile(jarFileName);
    new ClassBuilder().writeJarFromContent(authorizerName, classContent, jarFile);

    gfsh.executeAndAssertThat("deploy --jars=" + jarFile.getAbsolutePath()).statusIsSuccess();
    executeAndAssertGfshCommandIsSuccessful(TestMethodAuthorizer.class, USER_AUTHORIZER_PARAMETERS);
    servers.forEach(server -> server.invoke(() -> {
      TestMethodAuthorizer methodAuthorizer =
          (TestMethodAuthorizer) Objects.requireNonNull(ClusterStartupRule.getCache())
              .getService(QueryConfigurationService.class).getMethodAuthorizer();
      Set<String> expectedParameters =
          new HashSet<>(Arrays.asList(USER_AUTHORIZER_PARAMETERS.split(SPLITTING_REGEX)));
      Assertions.assertThat(methodAuthorizer.getParameters()).isEqualTo(expectedParameters);
    }));
  }

  @Test
  public void alterQueryServiceCommandUpdatesClusterConfigWhenNoMembersAreFound() {
    servers.forEach(VMProvider::stop);
    Class<JavaBeanAccessorMethodAuthorizer> authorizerClass =
        JavaBeanAccessorMethodAuthorizer.class;
    String authorizerName = authorizerClass.getName();

    gfsh.executeAndAssertThat(buildCommand(authorizerName, JAVA_BEAN_AUTHORIZER_PARAMETERS))
        .statusIsSuccess().hasInfoSection().hasLines().contains(NO_MEMBERS_FOUND_MESSAGE);
    startServerAndVerifyJavaBeanAccessorMethodAuthorizer();
  }

  @Test
  public void alterQueryServiceCommandDoesNotUpdateClusterConfigWhenFunctionExecutionFailsOnAllMembers() {
    String authorizerName = "badAuthorizerName";

    gfsh.executeAndAssertThat(buildCommand(authorizerName, JAVA_BEAN_AUTHORIZER_PARAMETERS))
        .statusIsError();
    locator.invoke(() -> assertThat(Objects.requireNonNull(ClusterStartupRule.getLocator())
        .getConfigurationPersistenceService().getCacheConfig(null)).isNull());
  }
}
