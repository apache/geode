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

package org.apache.geode.management.internal.configuration.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConfigurationTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private Configuration configuration;

  @Before
  public void before() throws Exception {
    configuration = new Configuration("cluster");
  }

  @Test
  public void name() throws Exception {}

  @Test
  public void setInvalidCacheXmlFile() throws IOException {
    File file = folder.newFile("test.xml");
    FileUtils.writeStringToFile(file, "invalid xml content", "UTF-8");
    assertThatThrownBy(() -> configuration.setCacheXmlFile(file)).isInstanceOf(IOException.class)
        .hasMessageContaining("Unable to parse");
  }

  @Test
  public void addJarNames() throws Exception {
    configuration.addJarNames(getSet("abc.jar"));
    assertThat(configuration.getJarNames()).containsExactly("abc.jar");

    configuration.addJarNames(getSet("def.jar"));
    assertThat(configuration.getJarNames()).containsExactlyInAnyOrder("abc.jar", "def.jar");

    configuration.addJarNames(getSet("abc-1.0.jar", "def.1.0.jar"));
    assertThat(configuration.getJarNames()).containsExactlyInAnyOrder("abc-1.0.jar", "def.1.0.jar");

    configuration.addJarNames(getSet("abc-2.0.jar", "def.jar"));
    assertThat(configuration.getJarNames()).containsExactlyInAnyOrder("abc-2.0.jar", "def.jar");

    configuration.addJarNames(getSet("abc.jar"));
    assertThat(configuration.getJarNames()).containsExactlyInAnyOrder("abc.jar", "def.jar");
  }

  private Set<String> getSet(String... values) {
    return new HashSet<>(Arrays.asList(values));
  }
}
