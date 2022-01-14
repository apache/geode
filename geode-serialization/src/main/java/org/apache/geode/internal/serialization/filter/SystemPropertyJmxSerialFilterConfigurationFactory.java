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

import static org.apache.commons.lang3.JavaVersion.JAVA_9;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * Creates an instance of {@code JmxSerialFilterConfiguration} that is enabled only if certain
 * conditions are met. The system property {@code jmx.remote.rmi.server.serial.filter.pattern} must
 * be blank, and the JRE must be Java 9 or greater.
 */
public class SystemPropertyJmxSerialFilterConfigurationFactory
    implements JmxSerialFilterConfigurationFactory {

  private static final String PROPERTY_NAME = "jmx.remote.rmi.server.serial.filter.pattern";

  private final boolean enabled;
  private final String pattern;

  public SystemPropertyJmxSerialFilterConfigurationFactory() {
    // JmxSerialFilter requires Java 9 or greater
    this(isJavaVersionAtLeast(JAVA_9) && isBlank(System.getProperty(PROPERTY_NAME)),
        new OpenMBeanFilterPattern().pattern());
  }

  @VisibleForTesting
  SystemPropertyJmxSerialFilterConfigurationFactory(boolean enabled, String pattern) {
    this.enabled = enabled;
    this.pattern = pattern;
  }

  @Override
  public FilterConfiguration create() {
    if (enabled) {
      return new JmxSerialFilterConfiguration(PROPERTY_NAME, pattern);
    }
    return () -> false;
  }
}
