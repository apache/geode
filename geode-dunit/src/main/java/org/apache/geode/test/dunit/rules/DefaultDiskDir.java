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
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.TestContext.contextDirectory;
import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;

import java.io.Serializable;

/**
 * Manages the default disk directory system property.
 */
public class DefaultDiskDir implements Serializable {
  private static final String PROPERTY_KEY = GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY;
  boolean ownsDefaultDiskDirProperty;

  /**
   * Sets the default disk directory system property to a safe value if not already set.
   */
  public void set() {
    // If the property has no value, assume ownership of it
    ownsDefaultDiskDirProperty = System.getProperty(PROPERTY_KEY) == null;
    if (ownsDefaultDiskDirProperty) {
      System.setProperty(PROPERTY_KEY, contextDirectory().toString());
    }
  }

  /**
   * Clears the default disk directory system property if set by this {@code DefaultDiskDir}.
   */
  public void clear() {
    if (ownsDefaultDiskDirProperty) {
      System.clearProperty(PROPERTY_KEY);
    }
  }
}
