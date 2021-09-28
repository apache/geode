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
package org.apache.geode.internal.serialization.filter;

import static java.util.Objects.requireNonNull;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.distributed.ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS;

import java.util.Properties;

public class DistributedSerializableObjectConfig implements SerializableObjectConfig {

  private final Properties config;

  public DistributedSerializableObjectConfig(Properties config) {
    this.config = requireNonNull(config);
  }

  @Override
  public boolean isValidateSerializableObjectsEnabled() {
    return "true".equalsIgnoreCase(config.getProperty(VALIDATE_SERIALIZABLE_OBJECTS));
  }

  @Override
  public String getSerializableObjectFilter() {
    return config.getProperty(SERIALIZABLE_OBJECT_FILTER);
  }
}
