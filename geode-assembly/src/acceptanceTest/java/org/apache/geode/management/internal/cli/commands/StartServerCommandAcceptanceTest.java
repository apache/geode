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

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

/**
 * Start server parameters should take precedence over cache.xml file and cluster configuration
 * service. See GEODE-5256.
 */
public class StartServerCommandAcceptanceTest {

  private Path rootFolder;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    rootFolder = folderRule.getFolder().toPath();
  }

  @Test
  public void startStandaloneServerWithParametersShouldOverrideCacheXmlConfiguration()
      throws IOException {
    Path logFile = rootFolder.resolve(testName.getMethodName() + ".log");
    Path cacheXmlFile = rootFolder.resolve(testName.getMethodName() + "Cache.xml");

    int[] ports = getRandomAvailableTCPPorts(2);
    int serverPortInXml = ports[0];
    int serverPortInGfsh = ports[1];

    CacheCreation creation = new CacheCreation();
    CacheServer server = creation.addCacheServer();
    server.setPort(serverPortInXml);
    server.setBindAddress(null);
    server.setHostnameForClients(null);
    server.setMaxConnections(800);
    server.setMaxThreads(0);
    server.setMaximumMessageCount(230000);
    server.setMessageTimeToLive(180);
    server.setSocketBufferSize(32768);

    PrintWriter pw = new PrintWriter(new FileWriter(cacheXmlFile.toFile()), true);
    CacheXmlGenerator.generate(creation, pw);
    pw.close();

    String startServerCommand = String.join(" ",
        "start server",
        "--max-threads=100",
        "--max-connections=1200",
        "--max-message-count=5000",
        "--message-time-to-live=360",
        "--socket-buffer-size=16384",
        "--server-port=" + serverPortInGfsh,
        "--name=" + testName.getMethodName(),
        "--cache-xml-file=" + cacheXmlFile,
        "--J=-Dgemfire.log-file=" + logFile);

    GfshExecution execution = GfshScript
        .of(startServerCommand)
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("Server .* " + testName.getMethodName() + " is currently online.");

    // Assert Server Properties.
    Boolean configurationLineFound = Boolean.FALSE;
    LineIterator lineIterator = FileUtils.lineIterator(logFile.toFile());
    while (lineIterator.hasNext()) {
      String line = lineIterator.nextLine();
      if (line.contains("CacheServer Configuration:")) {
        configurationLineFound = Boolean.TRUE;
        assertThat(line).contains("max-threads=100");
        assertThat(line).contains("port=" + serverPortInGfsh);
        assertThat(line).contains("max-connections=1200");
        assertThat(line).contains("message-time-to-live=360");
        assertThat(line).contains("socket-buffer-size=16384");
        assertThat(line).contains("maximum-message-count=5000");
      }
    }

    assertThat(configurationLineFound).isTrue();
  }

  @Test
  public void startServerWithParametersWhenClusterConfigurationServiceIsEnabledShouldOverrideDefaults()
      throws IOException {
    int serverPort = getRandomAvailableTCPPort();
    Path logFile = rootFolder.resolve(testName.getMethodName() + ".log");

    String startServerCommand = String.join(" ",
        "start server",
        "--max-threads=50",
        "--max-connections=200",
        "--max-message-count=500",
        "--message-time-to-live=120",
        "--socket-buffer-size=8192",
        "--server-port=" + serverPort,
        "--use-cluster-configuration=true",
        "--name=" + testName.getMethodName(),
        "--J=-Dgemfire.log-file=" + logFile);

    // Start Locator, configure PDX (just to have a non-empty cluster-configuration) and start
    // server.
    GfshExecution startClusterExecution = GfshScript
        .of("start locator --name=locator1 --connect=true --enable-cluster-configuration=true",
            "configure pdx --read-serialized=true",
            startServerCommand)
        .execute(gfshRule);

    assertThat(startClusterExecution.getOutputText())
        .contains("Successfully connected to: JMX Manager")
        .contains("Cluster configuration for group 'cluster' is updated")
        .containsPattern("Server .* " + testName.getMethodName() + " is currently online.");

    // Assert Server Properties.
    boolean configurationLineFound = false;
    LineIterator lineIterator = FileUtils.lineIterator(logFile.toFile());
    while (lineIterator.hasNext()) {
      String line = lineIterator.nextLine();
      if (line.contains("CacheServer Configuration:")) {
        configurationLineFound = true;
        assertThat(line).contains("max-threads=50");
        assertThat(line).contains("port=" + serverPort);
        assertThat(line).contains("max-connections=200");
        assertThat(line).contains("message-time-to-live=120");
        assertThat(line).contains("socket-buffer-size=8192");
        assertThat(line).contains("maximum-message-count=500");
      }
    }

    assertThat(configurationLineFound).isTrue();
  }
}
