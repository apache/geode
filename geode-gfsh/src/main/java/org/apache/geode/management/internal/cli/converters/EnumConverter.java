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

import org.apache.geode.management.cli.ConverterHint;

/**
 * we can not directly use Spring's EnumConverter because we have some Enum types that does not
 * directly translate from name to the enum type, e.g IndexType. IndexType will use a special
 * converter to convert to enum type, so we need a way to disable this converter
 *
 * This needs to implement the interface, instead of extend the EnumConverter directly because the
 * FastPathScanner can only find classes directly implement an interface
 *
 * Our EnumConverter also has the extra functionality of converting dash into underscore and auto
 * upper-case to try to match the Enum defined.
 */
public class EnumConverter implements Converter<Enum<?>> {
  private final org.springframework.shell.converters.EnumConverter delegate;

  public EnumConverter() {
    delegate = new org.springframework.shell.converters.EnumConverter();
  }

  @Override
  public boolean supports(final Class<?> requiredType, final String optionContext) {
    return Enum.class.isAssignableFrom(requiredType)
        && !optionContext.contains(ConverterHint.DISABLE_ENUM_CONVERTER);
  }

  @Override
  public Enum<?> convertFromText(String value, Class<?> targetType, String optionContext) {
    // defined enum value can not have "-" in them, but the values passed in from gfsh command
    // would usually use "-" instead of "_";
    value = value.replace("-", "_");
    Enum<?> result = null;
    try {
      result = delegate.convertFromText(value, targetType, optionContext);
    } catch (Exception e) {
      // try using upper case again
      result = delegate.convertFromText(value.toUpperCase(), targetType, optionContext);
    }
    return result;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    return delegate.getAllPossibleValues(completions, targetType, existingData, optionContext,
        target);
  }
}
