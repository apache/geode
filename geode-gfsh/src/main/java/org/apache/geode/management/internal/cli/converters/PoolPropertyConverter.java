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
package org.apache.geode.management.internal.cli.converters;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import org.apache.geode.management.internal.cli.domain.PoolProperty;

/**
 * Converter for Spring Shell 3.x to parse pool-properties parameter.
 *
 * <p>
 * Parses JSON-style pool property syntax into PoolProperty array:
 *
 * <pre>
 * --pool-properties={'name':'name1','value':'value1'},{'name':'name2','value':'value2'}
 * </pre>
 *
 * <p>
 * Format rules:
 * <ul>
 * <li>Each property is enclosed in curly braces: {...}</li>
 * <li>Properties are separated by commas</li>
 * <li>Each property has 'name' and 'value' fields</li>
 * <li>Field names and values are enclosed in single quotes</li>
 * </ul>
 *
 * @since Geode 1.0
 */
@Component
public class PoolPropertyConverter implements Converter<String, PoolProperty[]> {

  // Pattern to match {'name':'value1','value':'value2'}
  private static final Pattern PROPERTY_PATTERN =
      Pattern.compile("\\{\\s*'name'\\s*:\\s*'([^']*)'\\s*,\\s*'value'\\s*:\\s*'([^']*)'\\s*\\}");

  /**
   * Converts a JSON-style string to an array of PoolProperty objects.
   *
   * @param source the string to parse (e.g.,
   *        "{'name':'n1','value':'v1'},{'name':'n2','value':'v2'}")
   * @return array of PoolProperty objects
   * @throws IllegalArgumentException if the string format is invalid
   */
  @Override
  public PoolProperty[] convert(@NonNull String source) {
    if (source == null || source.trim().isEmpty()) {
      return new PoolProperty[0];
    }

    List<PoolProperty> properties = new ArrayList<>();
    Matcher matcher = PROPERTY_PATTERN.matcher(source);

    while (matcher.find()) {
      PoolProperty property = new PoolProperty();
      property.setName(matcher.group(1));
      property.setValue(matcher.group(2));
      properties.add(property);
    }

    if (properties.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid pool-properties format. Expected: {'name':'name1','value':'value1'},{'name':'name2','value':'value2'}. Got: "
              + source);
    }

    return properties.toArray(new PoolProperty[0]);
  }
}
