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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.DistributedSystem.PROPERTIES_FILE_PROPERTY;
import static org.apache.geode.distributed.DistributedSystem.SECURITY_PROPERTIES_FILE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Integration tests for DistributedSystem class. These tests require file system I/O.
 */
@Category(MembershipTest.class)
public class DistributedSystemIntegrationTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Test
  public void getPropertiesFileShouldUsePathInSystemProperty() throws Exception {
    File propertiesFile = temporaryFolder.newFile("test.properties");
    System.setProperty(PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    new Properties().store(new FileWriter(propertiesFile, false), testName.getMethodName());

    String value = DistributedSystem.getPropertiesFile();

    assertThat(value).isEqualTo(propertiesFile.getCanonicalPath());
  }

  @Test
  public void getPropertiesFileUrlShouldUsePathInSystemProperty() throws Exception {
    File propertiesFile = temporaryFolder.newFile("test.properties");
    System.setProperty(PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    new Properties().store(new FileWriter(propertiesFile, false), testName.getMethodName());
    URL expectedPropertiesURL = propertiesFile.getCanonicalFile().toURI().toURL();

    URL value = DistributedSystem.getPropertiesFileURL();

    assertThat(value).isEqualTo(expectedPropertiesURL);
  }

  @Test
  public void getSecurityPropertiesFileShouldUsePathInSystemProperty() throws Exception {
    File propertiesFile = temporaryFolder.newFile("testsecurity.properties");
    System.setProperty(SECURITY_PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    new Properties().store(new FileWriter(propertiesFile, false), testName.getMethodName());

    String value = DistributedSystem.getSecurityPropertiesFile();

    assertThat(value).isEqualTo(propertiesFile.getCanonicalPath());
  }

  @Test
  public void getSecurityPropertiesFileUrlShouldUsePathInSystemProperty() throws Exception {
    File propertiesFile = temporaryFolder.newFile("testsecurity.properties");
    System.setProperty(SECURITY_PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    new Properties().store(new FileWriter(propertiesFile, false), testName.getMethodName());
    URL expectedPropertiesURL = propertiesFile.getCanonicalFile().toURI().toURL();

    URL value = DistributedSystem.getSecurityPropertiesFileURL();

    assertThat(value).isEqualTo(expectedPropertiesURL);
  }
}
