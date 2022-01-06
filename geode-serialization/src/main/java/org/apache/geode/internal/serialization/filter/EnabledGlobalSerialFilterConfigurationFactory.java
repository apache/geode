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

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.apache.geode.internal.lang.SystemProperty;

/**
 * Creates an instance of {@code GlobalSerialFilterConfiguration} that is enabled only if certain
 * conditions are met. The system property {@code jdk.serialFilter} must be blank, and the system
 * property {@code geode.enableGlobalSerialFilter} must be set to true.
 */
public class EnabledGlobalSerialFilterConfigurationFactory
    implements GlobalSerialFilterConfigurationFactory {

  private final boolean enabled;

  public EnabledGlobalSerialFilterConfigurationFactory() {
    // enable GlobalSerialFilter only under these conditions:
    // (1) jdk.serialFilter must be blank
    // (2) geode.enableGlobalSerialFilter must be set "true"
    this(isBlank(System.getProperty("jdk.serialFilter")) &&
        SystemProperty
            .getProductBooleanProperty("enableGlobalSerialFilter")
            .orElse(false));
  }

  private EnabledGlobalSerialFilterConfigurationFactory(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public FilterConfiguration create(SerializableObjectConfig serializableObjectConfig) {
    if (enabled) {
      return new GlobalSerialFilterConfiguration(serializableObjectConfig);
    }
    return () -> false;
  }
}
