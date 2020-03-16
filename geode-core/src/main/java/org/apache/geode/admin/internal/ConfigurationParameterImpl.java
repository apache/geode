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

package org.apache.geode.admin.internal;

import static org.apache.geode.admin.internal.InetAddressUtils.toHostString;
import static org.apache.geode.admin.internal.InetAddressUtilsWithLogging.toInetAddress;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.geode.admin.ConfigurationParameter;
import org.apache.geode.admin.UnmodifiableConfigurationException;

/**
 * A single configuration parameter of a system member.
 *
 * @since GemFire 3.5
 *
 */
public class ConfigurationParameterImpl implements org.apache.geode.admin.ConfigurationParameter {

  /** Identifying name of this configuration parameter */
  protected String name;
  /** Full description of this configuration parameter */
  protected String description;
  /** The current value */
  protected Object value;
  /** Class type of the value */
  protected Class type;
  /** True if this is modifiable; false if read-only */
  protected boolean userModifiable;
  /** List of listeners to notify when value changes */
  private final List listeners = new ArrayList();

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Constructs new <code>ConfigurationParameterImpl</code>.
   *
   * @param name the name of this parameter which cannot change
   * @param description full description to use
   * @param value the value of this parameter
   * @param type the class type of the value
   * @param userModifiable true if this is modifiable; false if read-only
   */
  protected ConfigurationParameterImpl(String name, String description, Object value, Class type,
      boolean userModifiable) {
    if (name == null || name.length() == 0) {
      throw new IllegalArgumentException(
          "ConfigurationParameter name must be specified");
    }

    this.name = name;
    setInternalState(description, value, type, userModifiable);
  }

  /**
   * Constructs new <code>ConfigurationParameterImpl</code>.
   *
   * @param name the name of this parameter which cannot change
   * @param value the value of this parameter
   */
  protected ConfigurationParameterImpl(String name, Object value) {
    if (name == null || name.length() == 0) {
      throw new IllegalArgumentException(
          "ConfigurationParameter name must be specified");
    }

    this.name = name;
    setInternalState(name, value, value.getClass(), true);
  }

  /** Constructor to allow serialization by subclass */
  protected ConfigurationParameterImpl() {}

  // -------------------------------------------------------------------------
  // Attribute accessors and mutators
  // -------------------------------------------------------------------------

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getDescription() {
    return this.description;
  }

  @Override
  public Object getValue() {
    return this.value;
  }

  @Override
  public String getValueAsString() {
    if (isString()) {
      return (String) this.value;
    } else if (isInetAddress()) {
      return toHostString(this.value);
    } else if (isFile()) {
      return this.value.toString();
    } else if (isOctal()) {
      String strVal = Integer.toOctalString(((Integer) this.value).intValue());
      if (!strVal.startsWith("0")) {
        strVal = "0" + strVal;
      }
      return strVal;
    } else if (isArray()) {
      List list = Arrays.asList((Object[]) this.value);
      return list.toString();
    } else {
      return this.value.toString();
    }
  }

  @Override
  public Class getValueType() {
    return this.type;
  }

  @Override
  public boolean isModifiable() {
    return this.userModifiable;
  }

  @Override
  public boolean isArray() {
    return "manager-parameters".equals(this.name) || "manager-classpaths".equals(this.name);
  }

  @Override
  public boolean isInetAddress() {
    return java.net.InetAddress.class.isAssignableFrom(this.type);
  }

  @Override
  public boolean isFile() {
    return java.io.File.class.equals(this.type);
  }

  @Override
  public boolean isOctal() {
    return "shared-memory-permissions".equals(this.name);
  }

  @Override
  public boolean isString() {
    return java.lang.String.class.equals(this.type);
  }

  @Override
  public void setValue(Object value) throws UnmodifiableConfigurationException {
    if (!isModifiable()) {
      throw new UnmodifiableConfigurationException(
          String.format("%s is not a modifiable configuration parameter",
              getName()));
    }
    if (value == null) {
      throw new IllegalArgumentException(
          String.format("Unable to set %s to null value",
              getName()));
    }
    if (!getValueType().equals(value.getClass())) {
      throw new IllegalArgumentException(
          String.format("Unable to set type %s with type %s",

              new Object[] {getValueType().getName(), value.getClass().getName()}));
    }

    if (value instanceof String && !isString()) {
      // we need to check what the type should be and convert to it...
      setValueFromString((String) value);
    } else {
      this.value = value;
    }
    fireConfigurationParameterValueChanged(this);
  }

  // -------------------------------------------------------------------------
  // Operations for handling the registration of listeners
  // Note: this is only for use within impl pkg and subclass pkgs
  // -------------------------------------------------------------------------

  /** Adds the listener for any changes to this configuration parameter. */
  public void addConfigurationParameterListener(ConfigurationParameterListener listener) {
    if (!this.listeners.contains(listener)) {
      this.listeners.add(listener);
    }
  }

  /** Removes the listener if it's currently registered. */
  public void removeConfigurationParameterListener(ConfigurationParameterListener listener) {
    if (this.listeners.contains(listener)) {
      this.listeners.remove(listener);
    }
  }

  // -------------------------------------------------------------------------
  // Implementation methods
  // -------------------------------------------------------------------------

  protected void setValueFromString(String newValue) {
    if (newValue == null) {
      throw new IllegalArgumentException(
          String.format("Unable to set %s to null value",
              getName()));
    }

    if (isInetAddress()) {
      this.value = toInetAddress(newValue);
    } else if (isFile()) {
      this.value = new File(newValue);
    } else if (isOctal()) {
      if (!newValue.startsWith("0")) {
        newValue = "0" + newValue;
      }
      this.value = Integer.valueOf(Integer.parseInt(newValue, 8));
    } else if (isArray()) {
      // parse it TODO
      throw new IllegalArgumentException(
          "Setting array value from delimited string is not supported");
    } else {
      this.value = newValue;
    }
  }

  /**
   * Fires changed configuration parameter to registered listeners.
   *
   * @param parm the configuration parameter the changed
   */
  protected void fireConfigurationParameterValueChanged(ConfigurationParameter parm) {
    ConfigurationParameterListener[] listeners = (ConfigurationParameterListener[]) this.listeners
        .toArray(new ConfigurationParameterListener[0]);
    for (int i = 0; i < listeners.length; i++) {
      listeners[i].configurationParameterValueChanged(parm);
    }
  }

  /**
   * Sets the internal state of this configuration parameter.
   *
   * @param description full description to use
   * @param value the value of this parameter
   * @param type the class type of the value
   * @param userModifiable true if this is modifiable; false if read-only
   */
  protected void setInternalState(String description, Object value, Class type,
      boolean userModifiable) {
    this.description = description;
    this.type = type;
    this.userModifiable = userModifiable;
    this.value = value;
  }

  @Override
  public String toString() {
    return this.name;
  }

}
