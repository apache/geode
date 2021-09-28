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
package org.apache.geode.internal.io;

import static org.apache.commons.lang3.JavaVersion.JAVA_1_8;
import static org.apache.commons.lang3.JavaVersion.JAVA_9;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.distributed.ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.junit.rules.CloseableReference;

public class ServerSerializableObjectFilterIntegrationTest {

  private static final String NAME = "server";

  private File workingDirectory;
  private int jmxPort;

  @Rule
  public CloseableReference<ServerLauncher> server = new CloseableReference<>();
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    workingDirectory = temporaryFolder.newFolder(NAME);
    jmxPort = getRandomAvailableTCPPort();
  }

  @Test
  public void doesNotConfigureValidateSerializableObjects_onJava9orGreater() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    assertThat(isJmxManagerStarted())
        .isTrue();
    assertThat(isValidateSerializableObjectsConfigured())
        .as(VALIDATE_SERIALIZABLE_OBJECTS)
        .isFalse();
  }

  @Test
  public void doesNotConfigureSerializableObjectFilter_onJava9orGreater() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    assertThat(isJmxManagerStarted())
        .isTrue();
    assertThat(getSerializableObjectFilter())
        .as(SERIALIZABLE_OBJECT_FILTER)
        .isEqualTo("!*");
  }

  @Test
  public void doesNotConfigureValidateSerializableObjects_onJava8() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    assertThat(isJmxManagerStarted())
        .isTrue();
    assertThat(isValidateSerializableObjectsConfigured())
        .as(VALIDATE_SERIALIZABLE_OBJECTS)
        .isFalse();
  }

  @Test // TODO:KIRK: need test with user specified object filter
  public void doesNotConfigureSerializableObjectFilter_onJava8() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    server.set(new ServerLauncher.Builder()
        .setMemberName(NAME)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .build())
        .get()
        .start();

    assertThat(isJmxManagerStarted())
        .isTrue();
    assertThat(getSerializableObjectFilter())
        .as(SERIALIZABLE_OBJECT_FILTER)
        .isEqualTo("!*");
  }

  private DistributionConfig getDistributionConfig() {
    InternalCache cache = (InternalCache) server.get().getCache();
    return cache.getInternalDistributedSystem().getConfig();
  }

  private SystemManagementService getSystemManagementService() {
    Cache cache = server.get().getCache();
    return (SystemManagementService) ManagementService.getManagementService(cache);
  }

  private boolean isValidateSerializableObjectsConfigured() {
    return getDistributionConfig().getValidateSerializableObjects();
  }

  private boolean isJmxManagerStarted() {
    return getSystemManagementService().isManager();
  }

  private String getSerializableObjectFilter() {
    return getDistributionConfig().getSerializableObjectFilter();
  }

  private static class SerializableClass implements Serializable {

    private final String value;

    private SerializableClass(String value) {
      this.value = value;
    }

    private String getValue() {
      return value;
    }
  }
}
