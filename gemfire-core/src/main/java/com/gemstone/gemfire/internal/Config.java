/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal;

import java.util.Properties;
import java.io.File;
import java.io.IOException;

/**
 * Provides generic configuration features.
 */
public interface Config {
  /**
   * Given the attribute's name return its value as a string.
   * This method is used as a way to get command line arguments that
   * specify the attribute name and fetch them safely from
   * a Config instance.
   * @throws IllegalArgumentException if the specified name is illegal
   */
  public String getAttribute(String attName);
  /**
   * Given the attribute's name return its value as an Object.
   * @throws IllegalArgumentException if the specified name is illegal
   */
  public Object getAttributeObject(String attName);
  /**
   * Given the attribute's name set its value.
   * This method is used as a way to get command line arguments that
   * specify the attribute name and value and get them safely into
   * a Config instance.
   * @throws IllegalArgumentException if the specified name or value are illegal
   * @throws com.gemstone.gemfire.UnmodifiableException if the attribute can not be modified.
   */
  public void setAttribute(String attName, String attValue, ConfigSource source);
  /**
   * Given the attribute's name set its value to the specified Object.
   * @throws IllegalArgumentException if the specified name is unknown
   * @throws com.gemstone.gemfire.InvalidValueException if the specified value is not compatible
   *                               with the attributes type
   * @throws com.gemstone.gemfire.UnmodifiableException if the attribute can not be modified.
   */
  public void setAttributeObject(String attName, Object attValue, ConfigSource source);
  /**
   * Returns true if the named configuration attribute can be modified.
   * @throws IllegalArgumentException if the specified name is illegal
   */
  public boolean isAttributeModifiable(String attName);
  /**
   * Returns the source of the given attribute. This lets you figure out where an attribute's value came from.
   * @param attName the name of the attribute whose source is returned
   * @return null if the attribute has not been configured; otherwise returns the source of the given attribute
   */
  public ConfigSource getAttributeSource(String attName);
  /**
   * Returns a description of the named configuration attribute.
   * @throws IllegalArgumentException if the specified name is illegal
   */
  public String getAttributeDescription(String attName);
  /**
   * Returns the class that defines the type of this attribute.
   * The attribute's values will be constrained to be instances of
   * this type.
   * @throws IllegalArgumentException if the specified name is illegal
   */
  public Class getAttributeType(String attName);
  /**
   * Returns an array containing the names of all the attributes.
   */
  public String[] getAttributeNames();
  /**
   * Gets the attributes names of just this config; does not include inherited
   * attributes.
   */
  public String[] getSpecificAttributeNames();
  /**
   * Returns whether or not this configuration is the same as another
   * configuration.
   */
  public boolean sameAs(Config v);
  /**
   * Converts the contents of this config to a property instance.
   * @since 3.5
   */
  public Properties toProperties();
  /**
   * Writes this config to the specified file.
   * @since 3.5
   */
  public void toFile(File f) throws IOException;
  /**
   * Returns a formatted string suitable for logging.
   * @since 7.0
   */
  public String toLoggerString();

}
