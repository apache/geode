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

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import org.apache.geode.management.configuration.ClassName;

/**
 * Spring Shell 3.x converter for ClassName objects.
 *
 * <p>
 * Converts a string to a ClassName object. The string can be:
 * <ul>
 * <li>Just a class name: "my.app.CacheLoader"</li>
 * <li>Class name with JSON properties:
 * "my.app.CacheLoader{'param1':'value1','param2':'value2'}"</li>
 * <li>Class name with JSON properties (double quotes):
 * "my.app.CacheLoader{\"param1\":\"value1\"}"</li>
 * <li>Empty string or just "{}" returns ClassName.EMPTY</li>
 * </ul>
 *
 * <p>
 * Used by Gfsh command options that specify cache loaders, cache writers, cache listeners, etc.
 *
 * <p>
 * Example usage:
 *
 * <pre>
 * --cache-loader=my.app.CacheLoader
 * --cache-loader=my.app.CacheLoader{'param1':'value1','param2':'value2'}
 * </pre>
 *
 * <p>
 * Note: If JSON properties are specified, the class should implement Declarable for proper
 * initialization. Otherwise, the properties may be ignored.
 *
 * @since GemFire 1.0
 */
@Component
public class ClassNameConverter implements Converter<String, ClassName> {

  /**
   * Converts a string to a ClassName object.
   *
   * @param source the string to convert (e.g., "my.app.CacheLoader" or
   *        "my.app.CacheLoader{'k':'v'}")
   * @return the ClassName object with parsed class name and initialization properties
   * @throws IllegalArgumentException if the class name contains invalid characters
   */
  @Override
  public ClassName convert(@NonNull String source) {
    // Handle empty/null input
    if (source == null || source.trim().isEmpty()) {
      return ClassName.EMPTY;
    }

    // Handle just delimiter "{}"
    if (source.trim().equals("{}")) {
      return ClassName.EMPTY;
    }

    int index = source.indexOf('{');
    if (index < 0) {
      // Just class name, no properties
      return new ClassName(source);
    } else {
      // Class name with JSON properties
      String className = source.substring(0, index);
      String json = source.substring(index);
      return new ClassName(className, json);
    }
  }
}
