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
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for AbstractLauncher class. These tests require file system I/O.
 */
@Category(IntegrationTest.class)
public class AbstractLauncherIntegrationJUnitTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  @Rule
  public final TestName testName = new TestName();
  
  private File gemfirePropertiesFile;
  private Properties expectedGemfireProperties;
  
  @Before
  public void setUp() throws Exception {
    this.gemfirePropertiesFile = this.temporaryFolder.newFile("gemfire.properties");
    
    this.expectedGemfireProperties = new Properties();
    this.expectedGemfireProperties.setProperty(DistributionConfig.NAME_NAME, "memberOne");
    this.expectedGemfireProperties.setProperty(DistributionConfig.GROUPS_NAME, "groupOne, groupTwo");
    this.expectedGemfireProperties.store(new FileWriter(this.gemfirePropertiesFile, false), this.testName.getMethodName());

    assertThat(this.gemfirePropertiesFile).isNotNull();
    assertThat(this.gemfirePropertiesFile.exists()).isTrue();
    assertThat(this.gemfirePropertiesFile.isFile()).isTrue();
  }
  
  @Test
  public void testLoadGemFirePropertiesFromFile() throws Exception {
    final Properties actualGemFireProperties = AbstractLauncher.loadGemFireProperties(this.gemfirePropertiesFile.toURI().toURL());

    assertThat(actualGemFireProperties).isNotNull();
    assertThat(actualGemFireProperties).isEqualTo(this.expectedGemfireProperties);
  }
}
