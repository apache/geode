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
package org.apache.geode.internal.security.shiro;

import static org.assertj.core.api.Assertions.*;

import org.apache.commons.io.FileUtils;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.UnavailableSecurityManagerException;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.config.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;

@Category(UnitTest.class)
public class ConfigInitializerIntegrationTest {

  private static final String SHIRO_INI_FILE = "ConfigInitializerIntegrationTest.ini";

  private String shiroIniInClasspath;
  private ConfigInitializer configInitializer;
  private String shiroIniInFilesystem;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    assertThat(getClass().getResource(SHIRO_INI_FILE)).isNotNull();

    this.configInitializer = new ConfigInitializer();

    this.shiroIniInClasspath = getResourcePackage(getClass()) + SHIRO_INI_FILE;

    File shiroIniFile = this.temporaryFolder.newFile(SHIRO_INI_FILE);
    FileUtils.copyURLToFile(getClass().getResource(SHIRO_INI_FILE), shiroIniFile);
    this.shiroIniInFilesystem = shiroIniFile.getAbsolutePath();

    assertThatThrownBy(() -> SecurityUtils.getSecurityManager())
        .isInstanceOf(UnavailableSecurityManagerException.class);
  }

  @After
  public void after() throws Exception {
    ThreadContext.remove();
    SecurityUtils.setSecurityManager(null);
  }

  @Test
  public void initialize_fileInClasspath() throws Exception {
    this.configInitializer.initialize(this.shiroIniInClasspath);
    assertThat(SecurityUtils.getSecurityManager()).isNotNull();
  }

  @Test
  public void initialize_null_throws_ConfigurationException() throws Exception {
    assertThatThrownBy(() -> this.configInitializer.initialize(null))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Resource [classpath:null] could not be found");
  }

  @Test
  public void initialize_fileInFilesystem() throws Exception {
    assertThatThrownBy(() -> this.configInitializer.initialize(this.shiroIniInFilesystem))
        .isInstanceOf(ConfigurationException.class).hasMessageContaining("Resource [classpath:")
        .hasMessageContaining("ConfigInitializerIntegrationTest.ini] could not be found");
  }

  private String getResourcePackage(Class classInPackage) {
    return classInPackage.getName().replace(classInPackage.getSimpleName(), "").replace(".", "/");
  }
}
