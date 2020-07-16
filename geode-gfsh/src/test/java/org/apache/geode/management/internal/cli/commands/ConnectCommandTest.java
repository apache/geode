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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.rules.GfshParserRule;

@SuppressWarnings("deprecation")
public class ConnectCommandTest {

  @ClassRule
  public static GfshParserRule gfshParserRule = new GfshParserRule();

  private ConnectCommand connectCommand;
  private Gfsh gfsh;
  private CommandResult result;
  private ResultModel resultModel;
  private OperationInvoker operationInvoker;
  private Properties properties;
  private ArgumentCaptor<File> fileCaptor;

  @Before
  public void before() {
    properties = new Properties();
    gfsh = mock(Gfsh.class);
    operationInvoker = mock(OperationInvoker.class);
    when(gfsh.getOperationInvoker()).thenReturn(operationInvoker);
    // using spy instead of mock because we want to call the real method when we do connect
    connectCommand = spy(ConnectCommand.class);
    when(connectCommand.getGfsh()).thenReturn(gfsh);
    doReturn(properties).when(connectCommand).loadProperties(any());
    result = mock(CommandResult.class);
    resultModel = mock(ResultModel.class);
    when(connectCommand.httpConnect(any(), any(), anyBoolean())).thenReturn(resultModel);
    when(connectCommand.jmxConnect(any(), anyBoolean(), any(), any(), anyBoolean()))
        .thenReturn(resultModel);
    fileCaptor = ArgumentCaptor.forClass(File.class);
  }

  @Test
  public void whenGfshIsAlreadyConnected() {
    when(gfsh.isConnectedAndReady()).thenReturn(true);
    gfshParserRule.executeAndAssertThat(connectCommand, "connect")
        .containsOutput("Already connected to");
  }

  @Test
  public void whenGfshIsNotConnected() {
    when(gfsh.isConnectedAndReady()).thenReturn(false);
    when(resultModel.getStatus()).thenReturn(Result.Status.OK);
    gfshParserRule.executeAndAssertThat(connectCommand, "connect")
        .statusIsSuccess();
  }

  @Test
  public void tokenIsGiven() {
    when(resultModel.getStatus()).thenReturn(Result.Status.OK);
    gfshParserRule.executeAndAssertThat(connectCommand, "connect --token=FOO_BAR")
        .statusIsSuccess();
  }

  @Test
  public void tokenIsGivenWithUserName() {
    gfshParserRule.executeAndAssertThat(connectCommand, "connect --token=FOO_BAR --user")
        .containsOutput("--token cannot be combined with --user or --password")
        .statusIsError();
  }

  @Test
  public void tokenIsGivenWithPassword() {
    gfshParserRule.executeAndAssertThat(connectCommand, "connect --token=FOO_BAR --password")
        .containsOutput("--token cannot be combined with --user or --password")
        .statusIsError();
  }

  @Test
  public void tokenIsGivenWithUserNameAndPassword() {
    gfshParserRule.executeAndAssertThat(connectCommand, "connect --token=FOO_BAR --user --password")
        .containsOutput("--token cannot be combined with --user or --password")
        .statusIsError();
  }

  @Test
  public void tokenIsGivenWithNoValue() {
    gfshParserRule.executeAndAssertThat(connectCommand, "connect --token")
        .containsOutput("--token requires a value, for example --token=foo")
        .statusIsError();
  }

  @Test
  public void givenTokenIsSetInProperties() {
    doReturn(properties).when(connectCommand).resolveSslProperties(any(), anyBoolean(), any(),
        any());
    gfshParserRule.executeAndAssertThat(connectCommand, "connect --token=FOO_BAR");

    assertThat(properties.getProperty("security-token")).isEqualTo("FOO_BAR");
  }

  @Test
  public void promptForPasswordIfUsernameIsGiven() {
    doReturn(properties).when(connectCommand).resolveSslProperties(any(), anyBoolean(), any(),
        any());
    result = gfshParserRule.executeCommandWithInstance(connectCommand, "connect --user=user");
    verify(gfsh).readPassword(CliStrings.CONNECT__PASSWORD + ": ");

    assertThat(properties.getProperty("security-username")).isEqualTo("user");
    assertThat(properties.getProperty("security-password")).isEqualTo("");
  }

  @Test
  public void notPromptForPasswordIfUsernameIsGiven() {
    doReturn(properties).when(connectCommand).resolveSslProperties(any(), anyBoolean(), any(),
        any());
    result = gfshParserRule.executeCommandWithInstance(connectCommand,
        "connect --user=user --password=pass");
    verify(gfsh, times(0)).readPassword(CliStrings.CONNECT__PASSWORD + ": ");

    assertThat(properties.getProperty("security-username")).isEqualTo("user");
    assertThat(properties.getProperty("security-password")).isEqualTo("pass");
  }

  @Test
  public void notPromptForPasswordIfuserNameIsGivenInFile() {
    // username specified in property file won't prompt for password
    properties.setProperty("security-username", "user");
    doReturn(properties).when(connectCommand).loadProperties(any(File.class));

    result = gfshParserRule.executeCommandWithInstance(connectCommand, "connect");
    verify(gfsh, times(0)).readPassword(CliStrings.CONNECT__PASSWORD + ": ");

    assertThat(properties).doesNotContainKey("security-password");
  }

  @Test
  public void plainConnectNotLoadFileNotPrompt() {
    result = gfshParserRule.executeCommandWithInstance(connectCommand, "connect");
    // will not try to load from any file
    verify(connectCommand).loadProperties(null, null);

    // will not try to prompt
    verify(gfsh, times(0)).readText(any());
    verify(gfsh, times(0)).readPassword(any());
  }

  @Test
  public void connectUseSsl() {
    result = gfshParserRule.executeCommandWithInstance(connectCommand, "connect --use-ssl");

    // will not try to load from any file
    verify(connectCommand).loadProperties(null, null);

    // gfsh will prompt for the all the ssl properties
    verify(gfsh).readText("key-store: ");
    verify(gfsh).readPassword("key-store-password: ");
    verify(gfsh).readText("key-store-type(default: JKS): ");
    verify(gfsh).readText("trust-store: ");
    verify(gfsh).readPassword("trust-store-password: ");
    verify(gfsh).readText("trust-store-type(default: JKS): ");
    verify(gfsh).readText("ssl-ciphers(default: any): ");
    verify(gfsh).readText("ssl-protocols(default: any): ");

    // verify the resulting properties has correct values
    assertThat(properties).hasSize(9);
    assertThat(properties.getProperty("ssl-keystore")).isEqualTo("");
    assertThat(properties.getProperty("ssl-keystore-password")).isEqualTo("");
    assertThat(properties.getProperty("ssl-keystore-type")).isEqualTo("JKS");
    assertThat(properties.getProperty("ssl-truststore")).isEqualTo("");
    assertThat(properties.getProperty("ssl-truststore-password")).isEqualTo("");
    assertThat(properties.getProperty("ssl-truststore-type")).isEqualTo("JKS");
    assertThat(properties.getProperty("ssl-ciphers")).isEqualTo("any");
    assertThat(properties.getProperty("ssl-protocols")).isEqualTo("any");
    assertThat(properties.getProperty("ssl-enabled-components")).isEqualTo("all");
  }

  @Test
  public void securityFileContainsSSLPropsAndNoUseSSL() {
    properties.setProperty(SSL_KEYSTORE, "keystore");
    result = gfshParserRule.executeCommandWithInstance(connectCommand,
        "connect --security-properties-file=test");

    // will try to load from this file
    verify(connectCommand).loadProperties(any(), fileCaptor.capture());
    assertThat(fileCaptor.getValue()).hasName("test");

    // it will prompt for missing properties
    verify(gfsh, times(6)).readText(any());
    verify(gfsh, times(2)).readPassword(any());
  }

  @Test
  public void securityFileContainsNoSSLPropsAndNoUseSSL() {
    result = gfshParserRule.executeCommandWithInstance(connectCommand,
        "connect --security-properties-file=test");

    // will try to load from this file
    verify(connectCommand).loadProperties(any(), fileCaptor.capture());
    assertThat(fileCaptor.getValue()).hasName("test");

    // it will prompt for missing properties
    verify(gfsh, times(0)).readText(any());
    verify(gfsh, times(0)).readPassword(any());
  }

  @Test
  public void connectUseLegacySecurityPropertiesFile() {
    properties.setProperty(
        org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE,
        "jmx-keystore");
    result = gfshParserRule.executeCommandWithInstance(connectCommand,
        "connect --security-properties-file=test --key-store=keystore --key-store-password=password");

    // wil try to load from this file
    verify(connectCommand).loadProperties(fileCaptor.capture());
    assertThat(fileCaptor.getValue()).hasName("test");

    // it will not prompt for missing properties
    verify(gfsh, times(0)).readText(any());
    verify(gfsh, times(0)).readPassword(any());

    // the command option will be ignored
    assertThat(properties).hasSize(1);
    assertThat(properties
        .get(org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE))
            .isEqualTo("jmx-keystore");
  }

  @Test
  public void connectUseSecurityPropertiesFile_promptForMissing() {
    properties.setProperty(SSL_KEYSTORE, "keystore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    result = gfshParserRule.executeCommandWithInstance(connectCommand,
        "connect --security-properties-file=test");

    // since nothing is loaded, will prompt for all missing values
    verify(gfsh, times(6)).readText(any());
    verify(gfsh, times(1)).readPassword(any());
  }

  @Test
  public void connectUseSecurityPropertiesFileAndOption_promptForMissing() {
    properties.setProperty(SSL_KEYSTORE, "keystore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    result = gfshParserRule.executeCommandWithInstance(connectCommand,
        "connect --security-properties-file=test --key-store=keystore2 --trust-store=truststore2");

    // since nothing is loaded, will prompt for all missing values
    verify(gfsh, times(5)).readText(any());
    verify(gfsh, times(1)).readPassword(any());

    assertThat(properties).hasSize(9);
    assertThat(properties.getProperty("ssl-keystore")).isEqualTo("keystore2");
    assertThat(properties.getProperty("ssl-keystore-password")).isEqualTo("password");
    assertThat(properties.getProperty("ssl-keystore-type")).isEqualTo("JKS");
    assertThat(properties.getProperty("ssl-truststore")).isEqualTo("truststore2");
    assertThat(properties.getProperty("ssl-truststore-password")).isEqualTo("");
    assertThat(properties.getProperty("ssl-truststore-type")).isEqualTo("JKS");
    assertThat(properties.getProperty("ssl-ciphers")).isEqualTo("any");
    assertThat(properties.getProperty("ssl-protocols")).isEqualTo("any");
    assertThat(properties.getProperty("ssl-enabled-components")).isEqualTo("all");
  }

  @Test
  public void containsLegacySSLConfigTest_ssl() {
    properties.setProperty(SSL_KEYSTORE, "keystore");
    assertThat(ConnectCommand.containsLegacySSLConfig(properties)).isFalse();
  }

  @Test
  public void containsLegacySSLConfigTest_cluster() {
    properties.setProperty(
        org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE,
        "cluster-keystore");
    assertThat(ConnectCommand.containsLegacySSLConfig(properties)).isTrue();
  }

  @Test
  public void containsLegacySSLConfigTest_jmx() {
    properties.setProperty(
        org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE,
        "jmx-keystore");
    assertThat(ConnectCommand.containsLegacySSLConfig(properties)).isTrue();
  }

  @Test
  public void containsLegacySSLConfigTest_http() {
    properties.setProperty(
        org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE,
        "http-keystore");
    assertThat(ConnectCommand.containsLegacySSLConfig(properties)).isTrue();
  }

  @Test
  public void loadPropertiesWithNull() {
    doCallRealMethod().when(connectCommand).loadProperties(any());
    assertThat(connectCommand.loadProperties(null, null)).isEmpty();
  }

  @Test
  public void isSslImpliedByOptions() {
    assertThat(connectCommand.isSslImpliedBySslOptions((String) null)).isFalse();
    assertThat(connectCommand.isSslImpliedBySslOptions((String[]) null)).isFalse();

    assertThat(connectCommand.isSslImpliedBySslOptions(null, null, null)).isFalse();

    assertThat(connectCommand.isSslImpliedBySslOptions(null, "test")).isTrue();
  }

  @Test
  public void resolveSslProperties() {
    // assume properties loaded from either file has an ssl property
    properties.setProperty(SSL_KEYSTORE, "keystore");
    properties = connectCommand.resolveSslProperties(gfsh, false, null, null);
    assertThat(properties).hasSize(9);

    properties.clear();

    properties.setProperty(SSL_KEYSTORE, "keystore");
    properties =
        connectCommand.resolveSslProperties(gfsh, false, null, null, "keystore2", "password");
    assertThat(properties).hasSize(9);
    assertThat(properties.getProperty(SSL_KEYSTORE)).isEqualTo("keystore2");
    assertThat(properties.getProperty(SSL_KEYSTORE_PASSWORD)).isEqualTo("password");
  }

  @Test
  public void connectToManagerWithDifferentMajorVersion() {
    when(gfsh.getVersion()).thenReturn("2.2");
    when(operationInvoker.getRemoteVersion()).thenReturn("1.2");
    when(operationInvoker.isConnected()).thenReturn(true);
    gfshParserRule.executeAndAssertThat(connectCommand, "connect --locator=localhost:4040")
        .statusIsError()
        .containsOutput("Cannot use a 2.2 gfsh client to connect to a 1.2 cluster.");
  }

  @Test
  public void connectToManagerWithDifferentMinorVersion() {
    when(gfsh.getVersion()).thenReturn("1.2");
    when(operationInvoker.getRemoteVersion()).thenReturn("1.3");
    when(operationInvoker.isConnected()).thenReturn(true);
    gfshParserRule.executeAndAssertThat(connectCommand, "connect --locator=localhost:4040")
        .statusIsError()
        .containsOutput("Cannot use a 1.2 gfsh client to connect to a 1.3 cluster.");
  }

  @Test
  public void connectToManagerOlderThan1_10() {
    when(operationInvoker.getRemoteVersion()).thenReturn("1.10");
    when(operationInvoker.isConnected()).thenReturn(true);

    ResultModel resultModel = new ResultModel();
    when(connectCommand.jmxConnect(any(), anyBoolean(), any(), any(), anyBoolean()))
        .thenReturn(resultModel);

    gfshParserRule.executeAndAssertThat(connectCommand, "connect --locator=localhost:4040")
        .statusIsSuccess()
        .containsOutput("You are connected to a cluster of version: 1.10");
  }

  @Test
  public void connectToOlderManagerWithNoRemoteVersion() {
    when(gfsh.getVersion()).thenReturn("1.14");
    when(operationInvoker.getRemoteVersion())
        .thenThrow(new RuntimeException("release version not available"));
    when(operationInvoker.isConnected()).thenReturn(true);

    gfshParserRule.executeAndAssertThat(connectCommand, "connect --locator=localhost:4040")
        .statusIsError()
        .containsOutput("Cannot use a 1.14 gfsh client to connect to this cluster.");
  }

  @Test
  public void connectToManagerBefore1_10() {
    when(gfsh.getVersion()).thenReturn("1.14");
    when(operationInvoker.getRemoteVersion()).thenReturn("1.9");
    when(operationInvoker.isConnected()).thenReturn(true);

    gfshParserRule.executeAndAssertThat(connectCommand, "connect --locator=localhost:4040")
        .statusIsError()
        .containsOutput("Cannot use a 1.14 gfsh client to connect to a 1.9 cluster");
  }
}
