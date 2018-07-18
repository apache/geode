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
package org.apache.geode.internal.cache.backup;

import static org.apache.geode.internal.cache.backup.FileSystemBackupWriterConfig.BASELINE_DIR;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriterConfig.TARGET_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;


public class FileSystemBackupWriterConfigTest {

  private Properties properties;
  private String targetDir;
  private String baselineDir;
  private FileSystemBackupWriterConfig config;

  @Before
  public void setUp() {
    properties = new Properties();
    config = new FileSystemBackupWriterConfig(properties);
    targetDir = "relative/path";
    baselineDir = "baseline/directory";
  }

  @Test
  public void getTargetDirectoryThrowsIfMissing() {
    assertThatThrownBy(() -> config.getTargetDirectory()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getTargetDirectoryThrowsIfBlank() {
    properties.setProperty(TARGET_DIR, "");
    assertThatThrownBy(() -> config.getTargetDirectory()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getTargetDirectoryReturnsCorrectString() {
    properties.setProperty(TARGET_DIR, targetDir);
    assertThat(config.getTargetDirectory()).isEqualTo(targetDir);
  }

  @Test
  public void getBaselineDirectoryReturnsCorrectString() {
    properties.setProperty(BASELINE_DIR, baselineDir);
    assertThat(config.getBaselineDirectory()).isEqualTo(baselineDir);
  }

  @Test
  public void getBaselineDirectoryIsOptional() {
    assertThatCode(() -> config.getBaselineDirectory()).doesNotThrowAnyException();
  }
}
