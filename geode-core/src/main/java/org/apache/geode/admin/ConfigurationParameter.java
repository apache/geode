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

package org.apache.geode.admin;

/**
 * A single configuration parameter of a {@link SystemMember}.
 *
 * @since GemFire 3.5
 *
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
@Deprecated
public interface ConfigurationParameter {

  /**
   * Gets the identifying name of this configuration parameter.
   *
   * @return the identifying name of this configuration parameter
   */
  String getName();

  /**
   * Gets the full description of this configuration parameter
   *
   * @return the full description of this configuration parameter
   */
  String getDescription();

  /**
   * Gets the current value
   *
   * @return the current value
   */
  Object getValue();

  /**
   * Gets the current value as a string
   *
   * @return the current value as a string
   */
  String getValueAsString();

  /**
   * Gets the class type of the value
   *
   * @return the class type of the value
   */
  Class getValueType();

  /**
   * True if this is modifiable; false if read-only
   *
   * @return whether this is modifiable
   */
  boolean isModifiable();

  /**
   * Returns true if this config parameter uses a string array for value.
   *
   * @return whether this config parameter uses a string array for value
   */
  boolean isArray();

  /**
   * Returns true if this config parameter represents an InetAddress value.
   *
   * @return whether this config parameter represents an InetAddress value
   */
  boolean isInetAddress();

  /**
   * Returns true if this config parameter represents a File value.
   *
   * @return whether this config parameter represents a File value
   */
  boolean isFile();

  /**
   * Returns true if this config parameter represents an octal value.
   *
   * @return whether this config parameter represents an octal value
   */
  boolean isOctal();

  /**
   * Returns true if this config parameter represents a string value
   *
   * @return whether this config parameter represents a string value
   */
  boolean isString();

  /**
   * Sets a new value for this configuration parameter.
   *
   * @param value the new value which must be of type {@link #getValueType}
   * @throws IllegalArgumentException if value type does not match {@link #getValueType}
   * @throws UnmodifiableConfigurationException if attempting to set value when isModifiable is
   *         false
   */
  void setValue(Object value) throws UnmodifiableConfigurationException;
}
