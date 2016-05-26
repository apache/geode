/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.admin;

/**
* A single configuration parameter of a {@link SystemMember}.
 *
 * @since GemFire     3.5
 *
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public interface ConfigurationParameter {
  
  /** Gets the identifying name of this configuration parameter. */
  public String getName();
  
  /** Gets the full description of this configuration parameter */
  public String getDescription();
  
  /** Gets the current value */
  public Object getValue();
  
  /** Gets the current value as a string */
  public String getValueAsString();
  
  /** Gets the class type of the value */
  public Class getValueType();
  
  /** True if this is modifiable; false if read-only */
  public boolean isModifiable();
  
  /** Returns true if this config parameter uses a string array for value. */
  public boolean isArray();
  
  /** Returns true if this config parameter represents an InetAddress value. */
  public boolean isInetAddress();
  
  /** Returns true if this config parameter represents a File value. */
  public boolean isFile();
  
  /** Returns true if this config parameter represents an octal value. */
  public boolean isOctal();
    
  /** Returns true if this config parameter represents a string value. */
  public boolean isString();
  
  /**
   * Sets a new value for this configuration parameter.
   *
   * @param value   the new value which must be of type {@link #getValueType}
   * @throws IllegalArgumentException
   *         if value type does not match {@link #getValueType}
   * @throws UnmodifiableConfigurationException
   *         if attempting to set value when isModifiable is false
   */
  public void setValue(Object value) throws UnmodifiableConfigurationException;
}

