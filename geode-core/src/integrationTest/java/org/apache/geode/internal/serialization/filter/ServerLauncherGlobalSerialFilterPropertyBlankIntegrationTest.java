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
package org.apache.geode.internal.serialization.filter;

import static java.lang.String.valueOf;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.test.junit.rules.CloseableReference;

public class ServerLauncherGlobalSerialFilterPropertyBlankIntegrationTest {

  private static final String NAME = "server";
  private static final String PROPERTY_NAME = "jdk.serialFilter";

  private Path workingDirectory;
  private int jmxPort;
  private Path logFile;

  @Rule
  public CloseableReference<ServerLauncher> server = new CloseableReference<>();
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUpFiles() {
    workingDirectory = temporaryFolder.getRoot().toPath().toAbsolutePath();
    logFile = workingDirectory.resolve(NAME + ".log").toAbsolutePath();
  }

  @Before
  public void setUpPorts() {
    jmxPort = getRandomAvailableTCPPort();
  }

  @Test
  public void startDoesNotConfigureGlobalSerialFilter_whenPropertyIsBlank() {
    System.setProperty(PROPERTY_NAME, " ");

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setDisableDefaultServer(true)
        .setWorkingDirectory(valueOf(workingDirectory))
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, valueOf(logFile))
        .build())
        .get()
        .start();

    assertThat(System.getProperty(PROPERTY_NAME))
        .as(PROPERTY_NAME)
        .isEqualTo(" ");
  }
}
