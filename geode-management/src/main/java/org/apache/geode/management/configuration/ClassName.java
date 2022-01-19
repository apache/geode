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

package org.apache.geode.management.configuration;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.util.internal.GeodeJsonMapper;

/**
 * Used to configure an attribute that represents an instance of a java class.
 * If the class implements the org.apache.geode.cache.Declarable interface from the
 * geode-core module then properties can also be configured that will be
 * used to initialize the instance.
 */
public class ClassName implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final ObjectMapper mapper = GeodeJsonMapper.getMapper();

  private final String className;
  private final Properties initProperties;

  /**
   * Can be used when updating an attribute to configure
   * it with no class name.
   */
  public static final ClassName EMPTY = new ClassName("");

  /**
   * Object to be instantiated using the empty param constructor of the className
   *
   * @param className this class needs a no-arg constructor.
   */
  public ClassName(String className) {
    this(className, "{}");
  }

  /**
   * this is a convenient way to create a ClassName object using json represented properties
   *
   * @param className this class needs to have a no-arg constructor
   * @param jsonInitProperties a json representation of the initialization properties
   *        that will be passed to {@code org.apache.geode.cache.Declarable#initialize}
   *        in the geode-core module.
   *        If the className is not Declarable, then these properties will be ignored
   * @throws IllegalArgumentException if the class name is not valid
   * @throws IllegalArgumentException if jsonInitProperties is invalid JSON
   */
  public ClassName(String className, String jsonInitProperties) {
    this(className, createProperties(jsonInitProperties));
  }

  private static Properties createProperties(String jsonInitProperties) {
    if (StringUtils.isBlank(jsonInitProperties)) {
      return new Properties();
    }
    try {
      return mapper.readValue(jsonInitProperties, Properties.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON: " + jsonInitProperties, e);
    }
  }

  /**
   * @param className the name of the class to be instantiated. This class needs to have
   *        a no-arg constructor.
   * @param properties the initialization properties
   *        that will be passed to {@code org.apache.geode.cache.Declarable#initialize}
   *        in the geode-core module.
   *        If the className is not Declarable, then these properties will be ignored
   *
   * @throws IllegalArgumentException if classname contains illegal classname characters
   */
  @JsonCreator
  public ClassName(@JsonProperty("className") String className,
      @JsonProperty("initProperties") Properties properties) {
    if (StringUtils.isBlank(className)) {
      this.className = "";
      initProperties = new Properties();
      return;
    }
    // validate the className
    if (!isClassNameValid(className)) {
      throw new IllegalArgumentException("Invalid className");
    }
    this.className = className;
    initProperties = properties == null ? new Properties() : properties;
  }

  /**
   * @return the name of the class
   */
  public String getClassName() {
    return className;
  }

  /**
   * @return the properties that will be used when an instance is created.
   */
  public Properties getInitProperties() {
    return initProperties;
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, initProperties);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (!(o instanceof ClassName)) {
      return false;
    }

    ClassName that = (ClassName) o;

    return className.equals(that.getClassName())
        && getInitProperties().equals(that.getInitProperties());
  }

  /**
   * this provides a convenient method to validate if the given name is a valid classname
   *
   * @return false if classname is blank or has invalid classname characters
   */
  public static boolean isClassNameValid(String className) {
    if (StringUtils.isBlank(className)) {
      return false;
    }
    String regex = "([\\p{L}_$][\\p{L}\\p{N}_$]*\\.)*[\\p{L}_$][\\p{L}\\p{N}_$]*";
    return Pattern.matches(regex, className);
  }

}
