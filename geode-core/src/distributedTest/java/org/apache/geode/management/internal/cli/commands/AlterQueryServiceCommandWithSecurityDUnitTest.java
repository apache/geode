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
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.METHOD_AUTHORIZER_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.SINGLE_AUTHORIZER_PARAMETER;
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

import junitparams.JUnitParamsRunner;
import org.apache.commons.io.FileUtils;
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

@RunWith(JUnitParamsRunner.class)
public class AlterQueryServiceCommandWithSecurityDUnitTest {
  private static final String DEFAULT_AUTHORIZER_NAME = RestrictedMethodAuthorizer.class.getName();
  private static final String JAVA_BEAN_AUTHORIZER_PARAMS = "java.lang,java.util";
  // A regex containing a comma to ensure that the '--authorizer-parameter' option is parsed
  // correctly
  private static final String REGEX_AUTHORIZER_PARAMETER = "^java.util.List..{4,8}$";
  private static final String USER_AUTHORIZER_PARAMETERS = "param1,param2,param3";
  private static final String TEST_AUTHORIZER_TXT = "TestMethodAuthorizer.txt";
  private final int serversToStart = 2;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator;
  private static List<MemberVM> servers = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();
    IntStream.range(0, serversToStart).forEach(
        i -> servers.add(i, cluster.startServerVM(i + 1, s -> s.withConnectionToLocator(locatorPort)
            .withCredential("clusterManage", "clusterManage"))));
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void alterQueryServiceCommandWithRestrictedMethodAuthorizerSpecifiedUpdatesMethodAuthorizer() {
    verifyCurrentAuthorizer(DEFAULT_AUTHORIZER_NAME);

    String authorizerName = RestrictedMethodAuthorizer.class.getName();
    gfsh.executeAndAssertThat(COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "=" + authorizerName)
        .statusIsSuccess().hasTableSection().hasRowSize(serversToStart);

    verifyCurrentAuthorizer(authorizerName);
  }

  @Test
  public void alterQueryServiceCommandWithUnrestrictedMethodAuthorizerSpecifiedUpdatesMethodAuthorizer() {
    verifyCurrentAuthorizer(DEFAULT_AUTHORIZER_NAME);

    String authorizerName = UnrestrictedMethodAuthorizer.class.getName();
    gfsh.executeAndAssertThat(COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "=" + authorizerName)
        .statusIsSuccess().hasTableSection().hasRowSize(serversToStart);

    verifyCurrentAuthorizer(authorizerName);
  }

  @Test
  public void alterQueryServiceCommandWithJavaBeanAccessorMethodAuthorizerSpecifiedUpdatesMethodAuthorizer() {
    verifyCurrentAuthorizer(DEFAULT_AUTHORIZER_NAME);

    String authorizerName = JavaBeanAccessorMethodAuthorizer.class.getName();
    gfsh.executeAndAssertThat(COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "=" + authorizerName
        + " --" + AUTHORIZER_PARAMETERS + "=" + JAVA_BEAN_AUTHORIZER_PARAMS).statusIsSuccess()
        .hasTableSection().hasRowSize(serversToStart);

    verifyCurrentAuthorizer(authorizerName);
    servers.forEach(server -> server.invoke(() -> {
      JavaBeanAccessorMethodAuthorizer methodAuthorizer = (JavaBeanAccessorMethodAuthorizer) Objects
          .requireNonNull(ClusterStartupRule.getCache()).getService(QueryConfigurationService.class)
          .getMethodAuthorizer();

      Set<String> expectedParameters =
          new HashSet<>(Arrays.asList(JAVA_BEAN_AUTHORIZER_PARAMS.split(",")));
      assertThat(methodAuthorizer.getAllowedPackages()).isEqualTo(expectedParameters);
    }));
  }

  @Test
  public void alterQueryServiceCommandWithRegExMethodAuthorizerSpecifiedUpdatesMethodAuthorizer() {
    verifyCurrentAuthorizer(DEFAULT_AUTHORIZER_NAME);

    String authorizerName = RegExMethodAuthorizer.class.getName();
    gfsh.executeAndAssertThat(COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "=" + authorizerName
        + " --" + SINGLE_AUTHORIZER_PARAMETER + "=" + REGEX_AUTHORIZER_PARAMETER).statusIsSuccess()
        .hasTableSection().hasRowSize(serversToStart);

    verifyCurrentAuthorizer(authorizerName);
    servers.forEach(server -> server.invoke(() -> {
      RegExMethodAuthorizer methodAuthorizer = (RegExMethodAuthorizer) Objects
          .requireNonNull(ClusterStartupRule.getCache()).getService(QueryConfigurationService.class)
          .getMethodAuthorizer();

      assertThat(methodAuthorizer.getAllowedPatterns().size()).isEqualTo(1);
      assertThat(methodAuthorizer.getAllowedPatterns()).containsOnly(REGEX_AUTHORIZER_PARAMETER);
    }));
  }

  @Test
  public void alterQueryServiceCommandWithUserSpecifiedMethodAuthorizerUpdatesMethodAuthorizer()
      throws IOException {
    verifyCurrentAuthorizer(DEFAULT_AUTHORIZER_NAME);

    String className = TestMethodAuthorizer.class.getName();
    String classContent =
        new String(Files.readAllBytes(Paths.get(getFilePath(TEST_AUTHORIZER_TXT))));

    String jarFileName = "testJar.jar";

    File jarFile = tempFolder.newFile(jarFileName);

    new ClassBuilder().writeJarFromContent(className, classContent, jarFile);

    gfsh.executeAndAssertThat("deploy --jars=" + jarFile.getAbsolutePath()).statusIsSuccess();

    String authorizerName = TestMethodAuthorizer.class.getName();
    gfsh.executeAndAssertThat(COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "=" + authorizerName
        + " --" + AUTHORIZER_PARAMETERS + "=" + USER_AUTHORIZER_PARAMETERS).statusIsSuccess()
        .hasTableSection().hasRowSize(serversToStart);

    verifyCurrentAuthorizer(authorizerName);
    servers.forEach(server -> server.invoke(() -> {
      TestMethodAuthorizer methodAuthorizer = (TestMethodAuthorizer) Objects
          .requireNonNull(ClusterStartupRule.getCache()).getService(QueryConfigurationService.class)
          .getMethodAuthorizer();

      Set<String> expectedParameters =
          new HashSet<>(Arrays.asList(USER_AUTHORIZER_PARAMETERS.split(",")));
      assertThat(methodAuthorizer.getParameters()).isEqualTo(expectedParameters);
    }));
  }

  @Test
  public void alterQueryServiceCommandUpdatesClusterConfig() {
    String authorizerName = JavaBeanAccessorMethodAuthorizer.class.getName();

    gfsh.executeAndAssertThat(COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "=" + authorizerName
        + " --" + AUTHORIZER_PARAMETERS + "=" + JAVA_BEAN_AUTHORIZER_PARAMS)
        .statusIsSuccess().hasTableSection().hasRowSize(serversToStart);

    int locatorPort = locator.getPort();
    MemberVM newServer = cluster.startServerVM(serversToStart + 1, s -> s
        .withConnectionToLocator(locatorPort).withCredential("clusterManage", "clusterManage"));
    newServer.invoke(() -> {
      JavaBeanAccessorMethodAuthorizer methodAuthorizer =
          (JavaBeanAccessorMethodAuthorizer) Objects.requireNonNull(ClusterStartupRule.getCache())
              .getService(QueryConfigurationService.class).getMethodAuthorizer();
      assertThat(methodAuthorizer.getClass().getName()).isEqualTo(authorizerName);

      Set<String> expectedParameters =
          new HashSet<>(Arrays.asList(JAVA_BEAN_AUTHORIZER_PARAMS.split(",")));
      assertThat(methodAuthorizer.getAllowedPackages()).isEqualTo(expectedParameters);
    });
  }

  private void verifyCurrentAuthorizer(String authorizerName) {
    servers.forEach(server -> server.invoke(() -> {
      MethodInvocationAuthorizer methodAuthorizer = Objects
          .requireNonNull(ClusterStartupRule.getCache()).getService(QueryConfigurationService.class)
          .getMethodAuthorizer();
      assertThat(methodAuthorizer.getClass().getName()).isEqualTo(authorizerName);
    }));
  }

  private String getFilePath(String fileName) throws IOException {
    URL url = getClass().getResource(fileName);
    File textFile = this.tempFolder.newFile(fileName);
    FileUtils.copyURLToFile(url, textFile);

    return textFile.getAbsolutePath();
  }
}
