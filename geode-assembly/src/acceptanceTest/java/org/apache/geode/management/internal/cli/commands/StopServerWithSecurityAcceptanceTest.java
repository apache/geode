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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class StopServerWithSecurityAcceptanceTest {

  private final Properties securityProps = newSecurityProperties();

  private File securityPropertiesFile;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  @Before
  public void setUp() throws IOException {
    Path rootFolder = folderRule.getFolder().toPath();
    securityPropertiesFile = rootFolder.resolve("security.properties").toFile();
    try (FileOutputStream out = new FileOutputStream(securityPropertiesFile)) {
      securityProps.store(out, null);
    }
  }

  @Test
  public void cannotStopServerAsDataReaderOverHttp() {
    startCluster();

    GfshExecution stopServer = dataReaderCannotStopServer(true);
    assertThat(stopServer.getOutputText())
        .contains("dataReader not authorized for CLUSTER:READ");
  }

  @Test
  public void canStopServerAsClusterAdminOverHttp() {
    startCluster();

    clusterAdminCanStopServer(true);
  }

  @Test
  public void cannotStopServerAsDataReaderOverJmx() {
    startCluster();

    GfshExecution stopServer = dataReaderCannotStopServer(false);
    assertThat(stopServer.getOutputText()).contains("dataReader not authorized for CLUSTER:READ");
  }

  @Test
  public void canStopServerAsClusterAdminOverJmx() {
    startCluster();

    clusterAdminCanStopServer(false);
  }

  @Test
  public void cannotStopServerAsClusterReaderOverJmx() {
    startCluster();

    GfshExecution stopServer = clusterReaderCannotStopServer(false);
    assertThat(stopServer.getOutputText())
        .contains("clusterRead not authorized for CLUSTER:MANAGE");
  }

  @Test
  public void cannotStopServerAsClusterReaderOverHttp() {
    startCluster();

    GfshExecution stopServer = clusterReaderCannotStopServer(true);
    assertThat(stopServer.getOutputText())
        .contains("clusterRead not authorized for CLUSTER:MANAGE");
  }

  private Properties newSecurityProperties() {
    Properties properties = new Properties();
    properties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    properties.setProperty("security-username", "cluster");
    properties.setProperty("security-password", "cluster");
    return properties;
  }

  private GfshExecution startCluster() {
    String startLocator = new CommandStringBuilder("start locator")
        .addOption("name", "locator")
        .addOption("security-properties-file", securityPropertiesFile.getAbsolutePath())
        .toString();

    String startServer = new CommandStringBuilder("start server")
        .addOption("name", "server")
        .addOption("disable-default-server", "true")
        .addOption("security-properties-file", securityPropertiesFile.getAbsolutePath()).toString();

    return GfshScript
        .of(startLocator,
            startServer)
        .withName("cluster-setup")
        .execute(gfshRule);
  }

  private GfshExecution dataReaderCannotStopServer(boolean useHttp) {
    return GfshScript
        .of(connectCommand("dataReader", useHttp),
            "stop server --name=server")
        .expectFailure().execute(gfshRule);
  }

  private GfshExecution clusterAdminCanStopServer(boolean useHttp) {
    return GfshScript
        .of(connectCommand("cluster", useHttp),
            "stop server --name=server")
        .execute(gfshRule);
  }

  private GfshExecution clusterReaderCannotStopServer(boolean useHttp) {
    return GfshScript
        .of(connectCommand("clusterRead", useHttp),
            "stop server --name=server")
        .expectFailure()
        .execute(gfshRule);
  }

  private String connectCommand(String permission, boolean useHttp) {
    CommandStringBuilder cmd = new CommandStringBuilder("connect")
        .addOption("user", permission)
        .addOption("password", permission);
    if (useHttp) {
      cmd.addOption("use-http");
    }
    return cmd.getCommandString();
  }
}
