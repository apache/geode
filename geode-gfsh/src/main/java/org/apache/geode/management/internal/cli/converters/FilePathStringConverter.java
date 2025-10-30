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

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * Spring Shell 3.x converter for file path strings.
 *
 * <p>
 * Converts a file path string to itself (passthrough conversion).
 * Used by commands with file path options (e.g., --cache-xml-file).
 *
 * <p>
 * This converter provides utility methods for file system navigation:
 * <ul>
 * <li>{@link #getRoots()} - Returns all file system roots</li>
 * <li>{@link #getSiblings(String)} - Returns files in the same directory</li>
 * </ul>
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * - Spring Shell 1.x: Used supports(), convertFromText(), getAllPossibleValues()
 * - Spring Shell 3.x: Simple Converter<String, String> for conversion only
 * - Completion logic (getRoots, getSiblings) preserved for ValueProvider use
 * - Auto-completion should be implemented via ValueProvider (separate concern)
 *
 * @since GemFire 7.0
 */
@Component
public class FilePathStringConverter implements Converter<String, String> {

  /**
   * Converts a file path string (passthrough conversion).
   *
   * @param source the file path string
   * @return the same file path string
   */
  @Override
  public String convert(@NonNull String source) {
    return source;
  }

  /**
   * Gets all file system roots (e.g., "/" on Unix, "C:\", "D:\" on Windows).
   *
   * <p>
   * This method is preserved for potential use by a ValueProvider in Spring Shell 3.x.
   *
   * @return list of absolute paths to all file system roots
   */
  public List<String> getRoots() {
    File[] roots = File.listRoots();
    return Arrays.stream(roots).map(File::getAbsolutePath).collect(Collectors.toList());
  }

  /**
   * Gets sibling files for completion purposes.
   *
   * <p>
   * If path is a directory, returns files under that directory.
   * If path is a filename, returns all siblings of that file (files in the same directory).
   *
   * <p>
   * This method is preserved for potential use by a ValueProvider in Spring Shell 3.x.
   *
   * @param path the current file path (may be directory or filename)
   * @return list of files in the target directory, with appropriate path prefix
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
}
