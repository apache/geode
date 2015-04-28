/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.admin;

/**
* A single configuration parameter of a {@link SystemMember}.
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
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

