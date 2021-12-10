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

import static org.apache.commons.lang3.JavaVersion.JAVA_1_8;
import static org.apache.commons.lang3.JavaVersion.JAVA_9;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.serialization.filter.SerialFilterAssertions.assertThatSerialFilterIsSameAs;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.test.junit.rules.CloseableReference;

public class ServerLauncherGlobalSerialFilterExistsIntegrationTest {

  private static final String NAME = "server";
  private static final ObjectInputFilterApi OBJECT_INPUT_FILTER_API =
      new ReflectionObjectInputFilterApiFactory().createObjectInputFilterApi();

  private File workingDirectory;
  private int jmxPort;

  @Rule
  public CloseableReference<ServerLauncher> server = new CloseableReference<>();
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    workingDirectory = temporaryFolder.newFolder(NAME);
    jmxPort = getRandomAvailableTCPPort();
  }

  @Test
  public void doesNotConfigureGlobalSerialFilter_whenFilterExists_onJava9orGreater()
      throws InvocationTargetException, IllegalAccessException {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    Object existingGlobalSerialFilter = OBJECT_INPUT_FILTER_API.createFilter("!*");
    OBJECT_INPUT_FILTER_API.setSerialFilter(existingGlobalSerialFilter);

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

    assertThatSerialFilterIsSameAs(existingGlobalSerialFilter);
  }

  @Test
  public void doesNotConfigureGlobalSerialFilter_whenFilterExists_onJava8()
      throws InvocationTargetException, IllegalAccessException {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    Object existingGlobalSerialFilter = OBJECT_INPUT_FILTER_API.createFilter("!*");
    OBJECT_INPUT_FILTER_API.setSerialFilter(existingGlobalSerialFilter);

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

    assertThatSerialFilterIsSameAs(existingGlobalSerialFilter);
  }
}
