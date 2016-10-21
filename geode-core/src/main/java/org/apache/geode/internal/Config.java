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
package org.apache.geode.internal;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Provides generic configuration features.
 */
public interface Config {
  /**
   * Given the attribute's name return its value as a string. This method is used as a way to get
   * command line arguments that specify the attribute name and fetch them safely from a Config
   * instance.
   * 
   * @throws IllegalArgumentException if the specified name is illegal
   */
  String getAttribute(String attName);

  /**
   * Given the attribute's name return its value as an Object.
   * 
   * @throws IllegalArgumentException if the specified name is illegal
   */
  Object getAttributeObject(String attName);

  /**
   * Given the attribute's name set its value. This method is used as a way to get command line
   * arguments that specify the attribute name and value and get them safely into a Config instance.
   * 
   * @throws IllegalArgumentException if the specified name or value are illegal
   * @throws org.apache.geode.UnmodifiableException if the attribute can not be modified.
   */
  void setAttribute(String attName, String attValue, ConfigSource source);

  /**
   * Given the attribute's name set its value to the specified Object.
   * 
   * @throws IllegalArgumentException if the specified name is unknown
   * @throws org.apache.geode.InvalidValueException if the specified value is not compatible with
   *         the attributes type
   * @throws org.apache.geode.UnmodifiableException if the attribute can not be modified.
   */
  void setAttributeObject(String attName, Object attValue, ConfigSource source);

  /**
   * Returns true if the named configuration attribute can be modified.
   * 
   * @throws IllegalArgumentException if the specified name is illegal
   */
  boolean isAttributeModifiable(String attName);

  /**
   * Returns the source of the given attribute. This lets you figure out where an attribute's value
   * came from.
   * 
   * @param attName the name of the attribute whose source is returned
   * @return null if the attribute has not been configured; otherwise returns the source of the
   *         given attribute
   */
  ConfigSource getAttributeSource(String attName);

  /**
   * Returns a description of the named configuration attribute.
   * 
   * @throws IllegalArgumentException if the specified name is illegal
   */
  String getAttributeDescription(String attName);

  /**
   * Returns the class that defines the type of this attribute. The attribute's values will be
   * constrained to be instances of this type.
   * 
   * @throws IllegalArgumentException if the specified name is illegal
   */
  Class getAttributeType(String attName);

  /**
   * Returns an array containing the names of all the attributes.
   */
  String[] getAttributeNames();

  /**
   * Gets the attributes names of just this config; does not include inherited attributes.
   */
  String[] getSpecificAttributeNames();

  /**
   * Returns whether or not this configuration is the same as another configuration.
   */
  boolean sameAs(Config v);

  /**
   * Converts the contents of this config to a property instance.
   * 
   * @since GemFire 3.5
   */
  Properties toProperties();

  /**
   * Writes this config to the specified file.
   * 
   * @since GemFire 3.5
   */
  void toFile(File f) throws IOException;

  /**
   * Returns a formatted string suitable for logging.
   * 
   * @since GemFire 7.0
   */
  String toLoggerString();

}
