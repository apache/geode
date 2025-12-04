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

import org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding.ConfigProperty;

/**
 * Spring Shell 3.x converter for datasource configuration properties.
 *
 * <p>
 * Parses JSON-style configuration property syntax into ConfigProperty array for JNDI binding
 * commands:
 *
 * <pre>
 * --datasource-config-properties={'name':'prop1','type':'java.lang.String','value':'value1'}
 * --datasource-config-properties={'name':'prop1','value':'value1','type':'java.lang.String'}
 * --datasource-config-properties={'name':'prop1','value':'value1'}
 * </pre>
 *
 * <p>
 * <b>Spring Shell 3.x Migration Context (GEODE-10466):</b><br>
 * This converter replaces the Spring Shell 1.x ConfigPropertyConverter that was removed in commit
 * 67a7086cce. The old converter used {@code org.springframework.shell.core.Converter} with Jackson
 * JSON parsing. Spring Shell 3.x requires
 * {@code org.springframework.core.convert.converter.Converter}
 * with manual regex-based parsing.
 *
 * <p>
 * <b>Key differences from Shell 1.x:</b>
 * <ul>
 * <li>Shell 1.x: Single object conversion with Jackson ObjectMapper</li>
 * <li>Shell 3.x: Array conversion with regex pattern matching</li>
 * <li>Shell 1.x: Auto-discovery via META-INF/services</li>
 * <li>Shell 3.x: Auto-registration via {@code @Component} annotation</li>
 * </ul>
 *
 * <p>
 * Format rules:
 * <ul>
 * <li>Each property is enclosed in curly braces: {...}</li>
 * <li>Multiple properties are separated by commas (no spaces after comma)</li>
 * <li>Required fields: 'name', 'value'</li>
 * <li>Optional field: 'type' (defaults to null if not specified)</li>
 * <li>Field order is flexible (name/type/value, name/value/type, etc.)</li>
 * <li>Field names and values are enclosed in single quotes</li>
 * </ul>
 *
 * <p>
 * <b>Example usage:</b>
 *
 * <pre>
 * create jndi-binding --name=myds --type=SIMPLE \
 *   --connection-url="jdbc:derby:newDB" \
 *   --datasource-config-properties={'name':'prop1','type':'java.lang.String','value':'value1'},\
 * {'name':'prop2','value':'value2'}
 * </pre>
 *
 * @see PoolPropertyConverter Similar converter for pool properties (2-field pattern)
 * @see CreateJndiBindingCommand Uses this converter for --datasource-config-properties parameter
 * @since Geode 2.0 (Spring Shell 3.x migration)
 */
@Component
public class ConfigPropertyConverter implements Converter<String, ConfigProperty[]> {

  // Regex to match entire object: {...}
  private static final Pattern OBJECT_PATTERN = Pattern.compile("\\{([^}]*)\\}");

  // Regex patterns to extract individual fields (order-independent)
  private static final Pattern NAME_PATTERN = Pattern.compile("'name'\\s*:\\s*'([^']*)'");
  private static final Pattern TYPE_PATTERN = Pattern.compile("'type'\\s*:\\s*'([^']*)'");
  private static final Pattern VALUE_PATTERN = Pattern.compile("'value'\\s*:\\s*'([^']*)'");

  /**
   * Converts a JSON-style string to an array of ConfigProperty objects.
   *
   * <p>
   * Parsing strategy:
   * <ol>
   * <li>Extract each object using OBJECT_PATTERN: {...}</li>
   * <li>For each object, extract 'name', 'type' (optional), and 'value' fields</li>
   * <li>Validate required fields (name, value)</li>
   * <li>Construct ConfigProperty using 3-arg or 2-arg constructor based on type presence</li>
   * </ol>
   *
   * @param source the string to parse (e.g.,
   *        "{'name':'n1','type':'t1','value':'v1'},{'name':'n2','value':'v2'}")
   * @return array of ConfigProperty objects with parsed fields
   * @throws IllegalArgumentException if the string format is invalid or required fields are missing
   */
  @Override
  public ConfigProperty[] convert(@NonNull String source) {
    if (source == null || source.trim().isEmpty()) {
      return new ConfigProperty[0];
    }

    List<ConfigProperty> properties = new ArrayList<>();
    Matcher objectMatcher = OBJECT_PATTERN.matcher(source);

    while (objectMatcher.find()) {
      String objectContent = objectMatcher.group(1);

      // Extract fields using order-independent pattern matching
      String name = extractField(NAME_PATTERN, objectContent);
      String type = extractField(TYPE_PATTERN, objectContent); // optional
      String value = extractField(VALUE_PATTERN, objectContent);

      // Validate required fields
      if (name == null || value == null) {
        throw new IllegalArgumentException(
            "Invalid config property format. Required fields: 'name', 'value'. "
                + "Optional field: 'type'. Got: {" + objectContent + "}");
      }

      // Use appropriate constructor based on type presence
      ConfigProperty property = (type != null)
          ? new ConfigProperty(name, type, value)
          : new ConfigProperty(name, value);

      properties.add(property);
    }

    if (properties.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid datasource-config-properties format. "
              + "Expected: {'name':'name1','type':'type1','value':'value1'},{'name':'name2','type':'type2','value':'value2'}. "
              + "Got: " + source);
    }

    return properties.toArray(new ConfigProperty[0]);
  }

  /**
   * Extracts a field value using the provided regex pattern.
   *
   * @param pattern the regex pattern to match the field
   * @param source the source string to search
   * @return the extracted field value, or null if not found
   */
  private String extractField(Pattern pattern, String source) {
    Matcher matcher = pattern.matcher(source);
    return matcher.find() ? matcher.group(1) : null;
  }
}
