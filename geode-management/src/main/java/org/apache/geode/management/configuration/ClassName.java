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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.util.internal.GeodeJsonMapper;

/**
 * This is used by gfsh and ClusterManagementService client to represent a class that can be
 * instantiated on the server using DeclarableTypeInstantiator. It defines a class name and
 * a set of initialization properties.
 */
public class ClassName implements Serializable {
  private static final long serialVersionUID = 1L;

  private String className = "";
  private Properties initProperties = new Properties();
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();

  // used to remove a Declarable through gfsh command
  // e.g. alter region --name=regionA --cache-loader=''
  public static ClassName EMPTY = new ClassName("");

  /**
   * Default constructor used for serialization.
   */
  public ClassName() {}

  /**
   * Object to be instantiated using the empty param constructor of the className
   *
   * @param className this class needs to have an empty param constructor
   */
  public ClassName(String className) {
    this(className, "{}");
  }

  /**
   * @param className this class needs to have an empty param constructor
   * @param jsonInitProperties a json representation of the initialization properties if this class
   *        is to be initialized as Declarable type
   */
  public ClassName(String className, String jsonInitProperties) {
    if (StringUtils.isBlank(className)) {
      return;
    }
    if (!isClassNameValid(className)) {
      throw new IllegalArgumentException("Invalid className");
    }
    this.className = className;
    try {
      initProperties = mapper.readValue(jsonInitProperties, Properties.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON: " + jsonInitProperties, e);
    }
  }

  public ClassName(String className, Properties properties) {
    this.className = className;
    this.initProperties = properties;
  }

  public String getClassName() {
    return className;
  }

  public Properties getInitProperties() {
    return initProperties;
  }

  public static boolean isClassNameValid(String fqcn) {
    if (StringUtils.isBlank(fqcn)) {
      return false;
    }
    String regex = "([\\p{L}_$][\\p{L}\\p{N}_$]*\\.)*[\\p{L}_$][\\p{L}\\p{N}_$]*";
    return Pattern.matches(regex, fqcn);
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

    return this.className.equals(that.getClassName())
        && this.getInitProperties().equals(that.getInitProperties());
  }

}
