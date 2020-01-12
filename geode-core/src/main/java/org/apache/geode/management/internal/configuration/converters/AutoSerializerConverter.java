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
 *
 */

package org.apache.geode.management.internal.configuration.converters;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.ParameterType;
import org.apache.geode.management.configuration.AutoSerializer;

/**
 * this converts between AutoSerializer and DeclarableType (xml domain object). Only string type
 * parameter in the DeclarableType will be converted.
 */
public class AutoSerializerConverter
    extends ConfigurationConverter<AutoSerializer, DeclarableType> {
  private static final String AUTO_SERIALIZER_CLASS_NAME =
      "org.apache.geode.pdx.ReflectionBasedAutoSerializer";
  private static final String PATTERNS_PROPERTY_NAME = "classes";
  private static final String PORTABLE_PROPERTY_NAME = "check-portability";

  @Override
  protected AutoSerializer fromNonNullXmlObject(DeclarableType xmlObject) {
    if (!xmlObject.getClassName().equals(AUTO_SERIALIZER_CLASS_NAME)) {
      return null;
    }
    Boolean portable = null;
    List<String> patterns = null;
    for (ParameterType parameter : xmlObject.getParameters()) {
      if (PATTERNS_PROPERTY_NAME.equals(parameter.getName())) {
        if (parameter.getString() != null) {
          patterns = Arrays.asList(parameter.getString().split("\\s*,\\s*"));
        }
      } else if (PORTABLE_PROPERTY_NAME.equals(parameter.getName())) {
        portable = Boolean.parseBoolean(parameter.getString());
      } else {
        return null;
      }
    }
    return new AutoSerializer(portable, patterns);
  }

  @Override
  protected DeclarableType fromNonNullConfigObject(AutoSerializer configObject) {
    Properties properties = new Properties();
    List<String> patterns = configObject.getPatterns();
    if (patterns != null && !patterns.isEmpty()) {
      properties.setProperty(PATTERNS_PROPERTY_NAME, StringUtils.join(patterns, ','));
    }
    if (configObject.isPortable()) {
      properties.setProperty(PORTABLE_PROPERTY_NAME, "true");
    }
    return new DeclarableType(AUTO_SERIALIZER_CLASS_NAME, properties);
  }
}
