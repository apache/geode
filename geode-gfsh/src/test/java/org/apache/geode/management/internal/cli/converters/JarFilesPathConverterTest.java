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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * Unit tests for {@link JarFilesPathConverter}.
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * - Spring Shell 1.x tests verified both conversion AND file system auto-completion
 * - Spring Shell 3.x separates concerns: conversion vs completion (ValueProvider)
 * - These tests focus on conversion logic: splitting comma-separated paths
 * - File system completion tests should move to a ValueProvider test class
 *
 * <p>
 * REMOVED TESTS (file system completion):
 * - itFindsJarDirs() - file system traversal to find directories containing JARs
 * - itFindsJarFiles() - file system traversal to find JAR files
 * - itFindsDirsWithSubdirs() - directory scanning for subdirectories
 * - itFindsDirsWithSubdirsAndJars() - combined directory and JAR file scanning
 * - itFindsNothingWithBadSearch() - file system search validation
 * - itFindsNothing() - empty result validation for file system search
 *
 * <p>
 * These tests relied on Spring Shell 1.x's {@code getAllPossibleValues()} method for
 * auto-completion, which is replaced by ValueProvider in Spring Shell 3.x.
 */
public class JarFilesPathConverterTest {

  @Test
  public void testSinglePath() {
    JarFilesPathConverter converter = new JarFilesPathConverter();
    String[] result = converter.convert("/path/to/app.jar");

    assertThat(result).containsExactly("/path/to/app.jar");
  }

  @Test
  public void testMultiplePaths() {
    JarFilesPathConverter converter = new JarFilesPathConverter();
    String[] result = converter.convert("/path/to/app.jar,/path/to/lib.jar");

    assertThat(result).containsExactly("/path/to/app.jar", "/path/to/lib.jar");
  }

  @Test
  public void testEmptyString() {
    JarFilesPathConverter converter = new JarFilesPathConverter();
    String[] result = converter.convert("");

    assertThat(result).isEmpty();
  }

  @Test
  public void testWhitespaceString() {
    JarFilesPathConverter converter = new JarFilesPathConverter();
    String[] result = converter.convert("   ");

    assertThat(result).isEmpty();
  }

  @Test
  public void testPathsWithSpaces() {
    JarFilesPathConverter converter = new JarFilesPathConverter();
    String[] result = converter.convert("/path/with spaces/app.jar,/another path/lib.jar");

    assertThat(result).containsExactly("/path/with spaces/app.jar", "/another path/lib.jar");
  }

  @Test
  public void testThreePaths() {
    JarFilesPathConverter converter = new JarFilesPathConverter();
    String[] result =
        converter.convert("/path/one.jar,/path/two.jar,/path/three.jar");

    assertThat(result).containsExactly("/path/one.jar", "/path/two.jar", "/path/three.jar");
  }
}
