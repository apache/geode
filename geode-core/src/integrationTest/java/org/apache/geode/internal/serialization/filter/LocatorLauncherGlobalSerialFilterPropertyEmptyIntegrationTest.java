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
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.serialization.filter.SerialFilterAssertions.assertThatSerialFilterIsNull;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.test.junit.rules.CloseableReference;

public class LocatorLauncherGlobalSerialFilterPropertyEmptyIntegrationTest {

  private static final String NAME = "locator";
  private static final String PROPERTY_NAME = "jdk.serialFilter";

  private Path workingDirectory;
  private int locatorPort;
  private int jmxPort;
  private Path logFile;

  @Rule
  public CloseableReference<LocatorLauncher> locator = new CloseableReference<>();
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
    int[] ports = getRandomAvailableTCPPorts(2);
    jmxPort = ports[0];
    locatorPort = ports[1];
  }

  @Test
  public void startDoesNotConfigureGlobalSerialFilter_whenPropertyIsEmpty()
      throws InvocationTargetException, IllegalAccessException {
    System.setProperty(PROPERTY_NAME, "");

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
        .setWorkingDirectory(valueOf(workingDirectory))
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, valueOf(logFile))
        .build())
        .get()
        .start();

    assertThatSerialFilterIsNull();
  }
}
