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

import java.io.File;
import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.cli.ConverterHint;

/**
 *
 * @since GemFire 7.0
 */
public class FilePathConverter implements Converter<File> {
  private FilePathStringConverter delegate;

  public FilePathConverter() {
    this.delegate = new FilePathStringConverter();
  }

  public void setDelegate(FilePathStringConverter delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return File.class.equals(type) && optionContext.contains(ConverterHint.FILE);
  }

  @Override
  public File convertFromText(String value, Class<?> targetType, String optionContext) {
    return new File(value);
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    return delegate.getAllPossibleValues(completions, targetType, existingData, optionContext,
        target);
  }


}
