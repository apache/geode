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

import static org.apache.geode.internal.cache.backup.AbstractBackupWriterConfig.TIMESTAMP;
import static org.apache.geode.internal.cache.backup.AbstractBackupWriterConfig.TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;


public class AbstractBackupWriterConfigTest {

  private Properties properties;
  private String timestamp;
  private String type;
  private AbstractBackupWriterConfig config;

  @Before
  public void setUp() {
    properties = new Properties();
    timestamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());
    type = "FileSystem";
    config = new AbstractBackupWriterConfig(properties) {};
  }

  @Test
  public void getTimestampThrowsIfMissing() {
    assertThatThrownBy(() -> config.getTimestamp()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getTimestampThrowsIfBlank() {
    properties.setProperty(TIMESTAMP, "");
    assertThatThrownBy(() -> config.getTimestamp()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getTimestampReturnsCorrectString() {
    properties.setProperty(TIMESTAMP, timestamp);
    assertThat(config.getTimestamp()).isEqualTo(timestamp);
  }

  @Test
  public void getBackupTypeThrowsIfMissing() {
    assertThatThrownBy(() -> config.getBackupType()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getBackupTypeThrowsIfBlank() {
    properties.setProperty(TYPE, "");
    assertThatThrownBy(() -> config.getBackupType()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getBackupTypeReturnsCorrectString() {
    properties.setProperty(TYPE, type);
    assertThat(config.getBackupType()).isEqualTo(type);
  }

  @Test
  public void getProperties() {
    assertThat(config.getProperties()).isSameAs(properties);
  }
}
