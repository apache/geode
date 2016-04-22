/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed;

import static org.assertj.core.api.Assertions.*;

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

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for DistributedSystem class. These tests require file system I/O.
 */
@Category(IntegrationTest.class)
public class DistributedSystemIntegrationJUnitTest {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  @Rule
  public final TestName testName = new TestName();
  
  @Test
  public void getPropertiesFileShouldUsePathInSystemProperty() throws Exception {
    File propertiesFile = this.temporaryFolder.newFile("test.properties");
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    Properties properties = new Properties();
    properties.store(new FileWriter(propertiesFile, false), this.testName.getMethodName());

    assertThat(DistributedSystem.getPropertiesFile()).isEqualTo(propertiesFile.getCanonicalPath());
  }
  
  @Test
  public void getPropertiesFileUrlShouldUsePathInSystemProperty() throws Exception {
    File propertiesFile = this.temporaryFolder.newFile("test.properties");
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    Properties properties = new Properties();
    properties.store(new FileWriter(propertiesFile, false), this.testName.getMethodName());

    URL propertiesURL = propertiesFile.getCanonicalFile().toURI().toURL();
    assertThat(DistributedSystem.getPropertiesFileURL()).isEqualTo(propertiesURL);
  }
  
  @Test
  public void getSecurityPropertiesFileShouldUsePathInSystemProperty() throws Exception {
    File propertiesFile = this.temporaryFolder.newFile("testsecurity.properties");
    System.setProperty(DistributedSystem.SECURITY_PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    Properties properties = new Properties();
    properties.store(new FileWriter(propertiesFile, false), this.testName.getMethodName());

    assertThat(DistributedSystem.getSecurityPropertiesFile()).isEqualTo(propertiesFile.getCanonicalPath());
  }
  
  @Test
  public void getSecurityPropertiesFileUrlShouldUsePathInSystemProperty() throws Exception {
    File propertiesFile = this.temporaryFolder.newFile("testsecurity.properties");
    System.setProperty(DistributedSystem.SECURITY_PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    Properties properties = new Properties();
    properties.store(new FileWriter(propertiesFile, false), this.testName.getMethodName());

    URL propertiesURL = propertiesFile.getCanonicalFile().toURI().toURL();
    assertThat(DistributedSystem.getSecurityPropertiesFileURL()).isEqualTo(propertiesURL);
  }
}
