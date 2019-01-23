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

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.domain.ClassName;

/**
 * Used by Gfsh command options that converts a string to a ClassName Object
 *
 * User can specify either a classname alone or a className followed by a "?" and json properties to
 * initialize the object
 *
 * e.g. --cache-loader=my.app.CacheLoader
 * --cache-loader=my.app.CacheLoader?{"param1":"value1","param2":"value2"}
 *
 * Currently, if you specify a json properties after the className, the class needs to be a
 * Declarable for it to be initialized, otherwise, the properties are ignored.
 *
 */
public class ClassNameConverter implements Converter<ClassName> {
  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return ClassName.class.isAssignableFrom(type);
  }

  @Override
  public ClassName convertFromText(String value, Class<?> targetType, String optionContext) {
    int index = value.indexOf('{');
    if (index < 0) {
      return new ClassName(value);
    } else {
      String className = value.substring(0, index);
      String json = value.substring(index);
      return new ClassName(className, json);
    }
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    return false;
  }
}
