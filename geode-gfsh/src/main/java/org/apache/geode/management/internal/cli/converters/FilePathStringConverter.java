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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.cli.ConverterHint;

/**
 *
 * @since GemFire 7.0
 */
public class FilePathStringConverter implements Converter<String> {
  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && optionContext.contains(ConverterHint.FILE_PATH);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType, String optionContext) {
    return value;
  }

  public List<String> getRoots() {
    File[] roots = File.listRoots();
    return Arrays.stream(roots).map(File::getAbsolutePath).collect(Collectors.toList());
  }

  /**
   * if path is a dir, it will return the list of files under this dir. if path is a filename, it
   * will return all the siblings of this file
   */
  public List<String> getSiblings(String path) {
    File currentFile = new File(path);

    // if currentFile is not a dir, convert currentFile to it's parent dir
    if (!currentFile.isDirectory()) {
      currentFile = currentFile.getParentFile();
      // a file needs to be in a directory, if the file's parent is null, that means user
      // typed a filename without "./" prefix, but meant to find the file in the current dir.
      if (currentFile == null) {
        currentFile = new File("./");
        path = null;
      } else {
        path = currentFile.getPath();
      }
    }

    // at this point, currentFile should be a directory, we need to return all the files
    // under this directory
    String prefix;
    if (path == null) {
      prefix = "";
    } else {
      prefix = path.endsWith(File.separator) ? path : path + File.separator;
    }

    return Stream.of(currentFile)
        .map(File::list)
        .filter(Objects::nonNull)
        .flatMap(Stream::of)
        .map(s -> prefix + s)
        .collect(Collectors.toList());
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    if (StringUtils.isBlank(existingData)) {
      getRoots().forEach(path -> completions.add(new Completion(path)));
      return !completions.isEmpty();
    }

    getSiblings(existingData).stream().filter(string -> string.startsWith(existingData))
        .forEach(path -> completions.add(new Completion(path)));

    return !completions.isEmpty();
  }

}
