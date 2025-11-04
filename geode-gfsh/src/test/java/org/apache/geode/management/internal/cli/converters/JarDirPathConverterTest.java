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
 * Unit tests for {@link JarDirPathConverter}.
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * - Spring Shell 1.x tests verified both conversion AND file system auto-completion
 * - Spring Shell 3.x separates concerns: conversion vs completion (ValueProvider)
 * - These tests focus on conversion logic: passthrough String → String
 * - File system completion tests should move to a ValueProvider test class
 *
 * <p>
 * REMOVED TESTS (file system completion):
 * - itFindsJarDirs() - file system traversal to find directories containing JARs
 * - itFindsDirsWithSubdirs() - directory scanning for subdirectories
 * - itFindsDirsWithSubdirsAndJars() - combined directory and JAR file scanning
 * - itFindsNothingWithBadSearch() - file system search validation
 * - itFindsNothing() - empty result validation for file system search
 *
 * <p>
 * These tests relied on Spring Shell 1.x's {@code getAllPossibleValues()} method for
 * auto-completion, which is replaced by ValueProvider in Spring Shell 3.x.
 */
public class JarDirPathConverterTest {

  @Test
  public void testConvertPath() {
    JarDirPathConverter converter = new JarDirPathConverter();
    String result = converter.convert("/path/to/lib");

    assertThat(result).isEqualTo("/path/to/lib");
  }

  @Test
  public void testConvertRelativePath() {
    JarDirPathConverter converter = new JarDirPathConverter();
    String result = converter.convert("../lib");

    assertThat(result).isEqualTo("../lib");
  }

  @Test
  public void testConvertPathWithSpaces() {
    JarDirPathConverter converter = new JarDirPathConverter();
    String result = converter.convert("/path/with spaces/lib");

    assertThat(result).isEqualTo("/path/with spaces/lib");
  }

  @Test
  public void testConvertCurrentDirectory() {
    JarDirPathConverter converter = new JarDirPathConverter();
    String result = converter.convert(".");

    assertThat(result).isEqualTo(".");
  }
}
