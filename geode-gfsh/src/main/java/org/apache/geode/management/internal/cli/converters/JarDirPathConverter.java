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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.cli.ConverterHint;

public class JarDirPathConverter implements Converter<String> {
  private FilePathStringConverter delegate;

  public JarDirPathConverter() {
    delegate = new FilePathStringConverter();
  }

  public void setDelegate(FilePathStringConverter delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && optionContext.contains(ConverterHint.JARDIR);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType, String optionContext) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    List<Completion> allCompletions = new ArrayList<>();
    delegate.getAllPossibleValues(allCompletions, targetType, existingData, optionContext, target);
    completions.addAll(allCompletions.stream()
        .filter(JarDirPathConverter::isDirWithDirsOrDirWithJars)
        .collect(Collectors.toList()));
    return notAllAreJars(completions);
  }

  static boolean isDirWithDirsOrDirWithJars(Completion dir) {
    return isDirWithDirsOrDirWithJars(dir.getValue());
  }

  static boolean isDirWithDirsOrDirWithJars(String dir) {
    File d = new File(dir);
    if (!d.isDirectory()) {
      return false;
    }

    File[] listing = d.listFiles();
    if (listing == null) {
      return false;
    }
    return hasSubdirs(listing) || hasJars(listing);
  }

  private static boolean hasSubdirs(File[] listing) {
    return Arrays.stream(listing).anyMatch(File::isDirectory);
  }

  private static boolean hasJars(File[] listing) {
    return Arrays.stream(listing).anyMatch(f -> f.getName().endsWith(".jar"));
  }

  private static boolean notAllAreJars(List<Completion> completions) {
    return completions.stream().anyMatch(c -> !c.getValue().endsWith(".jar"));
  }
}
