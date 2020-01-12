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

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

abstract class AbstractBackupWriterConfig {

  static final String TIMESTAMP = "TIMESTAMP";
  static final String TYPE = "TYPE";

  private final Properties properties;

  AbstractBackupWriterConfig(Properties properties) {
    this.properties = properties;
  }

  String getTimestamp() {
    String value = properties.getProperty(TIMESTAMP);
    if (StringUtils.isBlank(value)) {
      throw new IllegalStateException("Timestamp is missing");
    }
    return value;
  }

  String getBackupType() {
    String value = properties.getProperty(TYPE);
    if (StringUtils.isBlank(value)) {
      throw new IllegalStateException("Type is missing");
    }
    return value;
  }

  Properties getProperties() {
    return properties;
  }
}
