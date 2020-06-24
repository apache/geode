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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Start server parameters should take precedence over cache.xml file and cluster configuration
 * service. See GEODE-5256.
 */
public class StartServerCommandAcceptanceTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void startStandaloneServerWithParametersShouldOverrideCacheXmlConfiguration()
      throws IOException {
    File logFile = temporaryFolder.newFile(testName.getMethodName() + ".log");
    File cacheXmlFile = temporaryFolder.newFile(testName.getMethodName() + "Cache.xml");

    CacheCreation creation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    CacheServer server = creation.addCacheServer();
    server.setPort(40404);
    server.setBindAddress(null);
    server.setHostnameForClients(null);
    server.setMaxConnections(800);
    server.setMaxThreads(0);
    server.setMaximumMessageCount(230000);
    server.setMessageTimeToLive(180);
    server.setSocketBufferSize(32768);
    PrintWriter pw = new PrintWriter(new FileWriter(cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw);
    pw.close();

    Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    String startServerCommand =
        "start server --max-threads=100 --max-connections=1200 --max-message-count=5000 --message-time-to-live=360 --socket-buffer-size=16384 --server-port="
            + serverPort + " --name=" + testName.getMethodName() + " --cache-xml-file="
            + cacheXmlFile.getAbsolutePath() + " --J=-Dgemfire.log-file=" + logFile
                .getAbsolutePath();

    GfshExecution execution = GfshScript.of(startServerCommand).execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("Server .* " + testName.getMethodName() + " is currently online.");

    // Assert Server Properties.
    Boolean configurationLineFound = Boolean.FALSE;
    LineIterator lineIterator = FileUtils.lineIterator(logFile);
    while (lineIterator.hasNext()) {
      String line = lineIterator.nextLine();
      if (line.contains("CacheServer Configuration:")) {
        configurationLineFound = Boolean.TRUE;
        assertThat(line.contains("max-threads=100")).isTrue();
        assertThat(line.contains("port=" + serverPort)).isTrue();
        assertThat(line.contains("max-connections=1200")).isTrue();
        assertThat(line.contains("message-time-to-live=360")).isTrue();
        assertThat(line.contains("socket-buffer-size=16384")).isTrue();
        assertThat(line.contains("maximum-message-count=5000")).isTrue();
      }
    }

    assertThat(configurationLineFound).isTrue();
  }


  @Test
  public void startServerWithParametersWhenClusterConfigurationServiceIsEnabledShouldOverrideDefaults()
      throws IOException {
    Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    File logFile = temporaryFolder.newFile(testName.getMethodName() + ".log");

    String startServerCommand =
        "start server --max-threads=50 --max-connections=200 --max-message-count=500 --message-time-to-live=120 --socket-buffer-size=8192 --server-port="
            + serverPort + " --use-cluster-configuration=true --name=" + testName.getMethodName()
            + " --J=-Dgemfire.log-file=" + logFile.getAbsolutePath();

    // Start Locator, configure PDX (just to have a non-empty cluster-configuration) and start
    // server.
    GfshExecution startClusterExecution = GfshScript
        .of("start locator --name=locator1 --connect=true --enable-cluster-configuration=true",
            "configure pdx --read-serialized=true", startServerCommand)
        .execute(gfshRule);

    assertThat(startClusterExecution.getOutputText())
        .contains("Successfully connected to: JMX Manager")
        .contains("Cluster configuration for group 'cluster' is updated")
        .containsPattern("Server .* " + testName.getMethodName() + " is currently online.");

    // Assert Server Properties.
    Boolean configurationLineFound = Boolean.FALSE;
    LineIterator lineIterator = FileUtils.lineIterator(logFile);
    while (lineIterator.hasNext()) {
      String line = lineIterator.nextLine();
      if (line.contains("CacheServer Configuration:")) {
        configurationLineFound = Boolean.TRUE;
        assertThat(line.contains("max-threads=50")).isTrue();
        assertThat(line.contains("port=" + serverPort)).isTrue();
        assertThat(line.contains("max-connections=200")).isTrue();
        assertThat(line.contains("message-time-to-live=120")).isTrue();
        assertThat(line.contains("socket-buffer-size=8192")).isTrue();
        assertThat(line.contains("maximum-message-count=500")).isTrue();
      }
    }

    assertThat(configurationLineFound).isTrue();
  }
}
