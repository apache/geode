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

import static org.apache.geode.distributed.AbstractLauncher.loadGemFireProperties;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Integration tests for {@link AbstractLauncher} that require file system I/O.
 */
public class AbstractLauncherIntegrationTest {

  private File propertiesFile;
  private Properties expectedProperties;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    propertiesFile = temporaryFolder.newFile(GEMFIRE_PREFIX + "properties");

    expectedProperties = new Properties();
    expectedProperties.setProperty(NAME, "memberOne");
    expectedProperties.setProperty(GROUPS, "groupOne, groupTwo");
    expectedProperties.store(new FileWriter(propertiesFile, false), testName.getMethodName());

    assertThat(propertiesFile).exists().isFile();
  }

  @Test
  public void loadGemFirePropertiesFromFile() throws Exception {
    Properties loadedProperties = loadGemFireProperties(propertiesFile.toURI().toURL());

    assertThat(loadedProperties).isEqualTo(expectedProperties);
  }
}
