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

import static org.apache.geode.management.internal.cli.converters.JarDirPathConverter.isDirWithDirsOrDirWithJars;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.cli.ConverterHint;

public class JarFilesPathConverter implements Converter<String[]> {
  private FilePathStringConverter delegate;

  public JarFilesPathConverter() {
    delegate = new FilePathStringConverter();
  }

  public void setDelegate(FilePathStringConverter delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String[].class.equals(type) && optionContext.contains(ConverterHint.JARFILES);
  }

  @Override
  public String[] convertFromText(String value, Class<?> targetType, String optionContext) {
    return value.split(",");
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    // remove anything before ,
    int comma = existingData.lastIndexOf(',') + 1;
    String otherJars = existingData.substring(0, comma);
    existingData = existingData.substring(comma);
    List<Completion> allCompletions = new ArrayList<>();
    delegate.getAllPossibleValues(allCompletions, targetType, existingData, optionContext, target);
    completions.addAll(allCompletions.stream()
        .map(Completion::getValue)
        .filter(JarFilesPathConverter::isDirOrJar)
        .map(s -> new Completion(otherJars + s))
        .collect(Collectors.toList()));
    return allAreJars(completions);
  }

  private static boolean isDirOrJar(String file) {
    return isDirWithDirsOrDirWithJars(file) || file.endsWith(".jar");
  }

  private static boolean allAreJars(List<Completion> completions) {
    return completions.stream().allMatch(c -> c.getValue().endsWith(".jar"));
  }
}
