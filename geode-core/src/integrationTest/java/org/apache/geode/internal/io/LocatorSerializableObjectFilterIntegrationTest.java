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
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.filter.SanctionedSerializablesFilterPattern;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.junit.rules.CloseableReference;

public class LocatorSerializableObjectFilterIntegrationTest {

  private static final String NAME = "locator";

  private File workingDirectory;
  private int locatorPort;
  private int jmxPort;
  private String sanctionedSerializablesFilterPattern;

  @Rule
  public CloseableReference<LocatorLauncher> locator = new CloseableReference<>();
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    workingDirectory = temporaryFolder.newFolder(NAME);
    int[] ports = getRandomAvailableTCPPorts(2);
    locatorPort = ports[0];
    jmxPort = ports[1];
    sanctionedSerializablesFilterPattern = new SanctionedSerializablesFilterPattern().pattern();
  }

  @Test
  public void doesNotConfigureValidateSerializableObjects_onJava9orGreater() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
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

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
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
    assertThat(getDistributionConfig().getSerializableObjectFilter())
        .as(SERIALIZABLE_OBJECT_FILTER)
        .isEqualTo(DEFAULT_SERIALIZABLE_OBJECT_FILTER);
  }

  @Test
  public void doesNotChangeEmptySerializableObjectFilter_onJava9orGreater() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .set(SERIALIZABLE_OBJECT_FILTER, "")
        .set(VALIDATE_SERIALIZABLE_OBJECTS, "true")
        .build())
        .get()
        .start();

    assertThat(isJmxManagerStarted())
        .isTrue();
    assertThat(getDistributionConfig().getSerializableObjectFilter())
        .as(SERIALIZABLE_OBJECT_FILTER)
        .isEqualTo("");
  }

  @Test
  public void doesNotChangeNonEmptySerializableObjectFilter_onJava9orGreater() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    String existingSerializableObjectFilter = "!foo.Bar;*";

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .set(SERIALIZABLE_OBJECT_FILTER, existingSerializableObjectFilter)
        .set(VALIDATE_SERIALIZABLE_OBJECTS, "true")
        .build())
        .get()
        .start();

    assertThat(isJmxManagerStarted())
        .isTrue();
    assertThat(getDistributionConfig().getSerializableObjectFilter())
        .as(SERIALIZABLE_OBJECT_FILTER)
        .isEqualTo(existingSerializableObjectFilter);
  }

  @Test
  public void configuresValidateSerializableObjects_onJava8() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
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
        .isTrue(); // ??? why not false
  }

  @Test
  public void usesDefaultSerializableObjectFilter_onJava8() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
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
    assertThat(getDistributionConfig().getSerializableObjectFilter())
        .as(SERIALIZABLE_OBJECT_FILTER)
        .isEqualTo(DEFAULT_SERIALIZABLE_OBJECT_FILTER);
  }

  @Test
  public void doesNotChangeEmptySerializableObjectFilter_onJava8() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .set(SERIALIZABLE_OBJECT_FILTER, "")
        .set(VALIDATE_SERIALIZABLE_OBJECTS, "true")
        .build())
        .get()
        .start();

    assertThat(isJmxManagerStarted())
        .isTrue();
    assertThat(getDistributionConfig().getSerializableObjectFilter())
        .as(SERIALIZABLE_OBJECT_FILTER)
        .isEqualTo("");
  }

  @Test
  public void doesNotChangeNonEmptySerializableObjectFilter_onJava8() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    String existingSerializableObjectFilter = "!foo.Bar;*";

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, NAME + ".log").getAbsolutePath())
        .set(SERIALIZABLE_OBJECT_FILTER, existingSerializableObjectFilter)
        .set(VALIDATE_SERIALIZABLE_OBJECTS, "true")
        .build())
        .get()
        .start();

    assertThat(isJmxManagerStarted())
        .isTrue();
    assertThat(getDistributionConfig().getSerializableObjectFilter())
        .as(SERIALIZABLE_OBJECT_FILTER)
        .isEqualTo(existingSerializableObjectFilter);
  }

  private DistributionConfig getDistributionConfig() {
    InternalLocator locator = (InternalLocator) this.locator.get().getLocator();
    InternalCache cache = locator.getCache();
    return cache.getInternalDistributedSystem().getConfig();
  }

  private SystemManagementService getSystemManagementService() {
    InternalLocator locator = (InternalLocator) this.locator.get().getLocator();
    InternalCache cache = locator.getCache();
    return (SystemManagementService) ManagementService.getManagementService(cache);
  }

  private boolean isValidateSerializableObjectsConfigured() {
    return getDistributionConfig().getValidateSerializableObjects();
  }

  private boolean isJmxManagerStarted() {
    return getSystemManagementService().isManager();
  }
}
