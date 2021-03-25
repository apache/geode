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
package org.apache.geode.management.internal;

import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.management.internal.OpenJmxTypesSerialFilter.PROPERTY_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;

import org.apache.commons.lang3.JavaVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.test.junit.rules.CloseableReference;

public class ServerManagerSerialFilterIntegrationTest {

  private static final String NAME = "server";

  private File workingDirectory;
  private int jmxPort;
  private String expectedSerialFilter;

  @Rule
  public CloseableReference<ServerLauncher> server = new CloseableReference<>();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    workingDirectory = temporaryFolder.newFolder(NAME);
    jmxPort = getRandomAvailableTCPPort();
    expectedSerialFilter = new OpenJmxTypesSerialFilter().createSerialFilterPattern();
  }

  @After
  public void tearDown() {
    System.clearProperty(PROPERTY_NAME);
  }

  @Test
  public void startingServerWithJmxManager_configuresSerialFilter_atLeastJava9() {
    assumeThat(isJavaVersionAtLeast(JavaVersion.JAVA_9)).isTrue();

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setDisableDefaultServer(true)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    String serialFilter = System.getProperty(PROPERTY_NAME);
    assertThat(serialFilter).isEqualTo(expectedSerialFilter);
  }

  @Test
  public void startingServerWithJmxManager_changesEmptySerialFilter_atLeastJava9() {
    assumeThat(isJavaVersionAtLeast(JavaVersion.JAVA_9)).isTrue();

    System.setProperty(PROPERTY_NAME, "");

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setDisableDefaultServer(true)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    String serialFilter = System.getProperty(PROPERTY_NAME);
    assertThat(serialFilter).isEqualTo(expectedSerialFilter);
  }

  @Test
  public void startingServerWithJmxManager_skipsNonEmptySerialFilter_atLeastJava9() {
    assumeThat(isJavaVersionAtLeast(JavaVersion.JAVA_9)).isTrue();

    String existingSerialFilter = "!*";
    System.setProperty(PROPERTY_NAME, existingSerialFilter);

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setDisableDefaultServer(true)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    String serialFilter = System.getProperty(PROPERTY_NAME);
    assertThat(serialFilter).isEqualTo(existingSerialFilter);
  }

  @Test
  public void startingServerWithJmxManager_skipsSerialFilter_atMostJava8() {
    assumeThat(isJavaVersionAtMost(JavaVersion.JAVA_1_8)).isTrue();

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setDisableDefaultServer(true)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    String serialFilter = System.getProperty(PROPERTY_NAME);
    assertThat(serialFilter).isNull();
  }

  @Test
  public void startingServerWithJmxManager_skipsEmptySerialFilter_atMostJava8() {
    assumeThat(isJavaVersionAtMost(JavaVersion.JAVA_1_8)).isTrue();

    System.setProperty(PROPERTY_NAME, "");

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setDisableDefaultServer(true)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    String serialFilter = System.getProperty(PROPERTY_NAME);
    assertThat(serialFilter).isEqualTo("");
  }

  @Test
  public void startingServerWithJmxManager_skipsNonEmptySerialFilter_atMostJava8() {
    assumeThat(isJavaVersionAtMost(JavaVersion.JAVA_1_8)).isTrue();

    String existingSerialFilter = "!*";
    System.setProperty(PROPERTY_NAME, existingSerialFilter);

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setDisableDefaultServer(true)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    String serialFilter = System.getProperty(PROPERTY_NAME);
    assertThat(serialFilter).isEqualTo(existingSerialFilter);
  }
}
