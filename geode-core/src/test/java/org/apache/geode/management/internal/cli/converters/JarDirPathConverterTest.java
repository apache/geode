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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.shell.core.Completion;

import org.apache.geode.management.cli.ConverterHint;

public class JarDirPathConverterTest {
  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Test
  public void itSupportsString() {
    JarDirPathConverter jarDirPathConverter = new JarDirPathConverter();
    assertThat(jarDirPathConverter.supports(String.class, ConverterHint.JARDIR)).isTrue();
    assertThat(jarDirPathConverter.supports(String.class, ConverterHint.JARFILES)).isFalse();
    assertThat(jarDirPathConverter.supports(Integer.class, ConverterHint.JARDIR)).isFalse();
  }

  @Test
  public void itFindsJarDirs() throws Exception {
    File testDir = createFileOrDir(tmpDir.getRoot(), "testOne", true);
    File libDir = createFileOrDir(testDir, "lib", true);
    createFileOrDir(testDir, "empty", true);
    createFileOrDir(libDir, "jar_one.jar", false);
    createFileOrDir(libDir, "jar_two.jar", false);

    JarDirPathConverter jarDirPathConverter = new JarDirPathConverter();
    List<Completion> completions = new ArrayList<>();
    jarDirPathConverter.getAllPossibleValues(completions, null, testDir.getPath(), null, null);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).endsWith("lib");
  }

  @Test
  public void itFindsDirsWithSubdirs() throws Exception {
    File testDir = createFileOrDir(tmpDir.getRoot(), "testTwo", true);
    File libDir = createFileOrDir(testDir, "lib", true);
    createFileOrDir(libDir, "file.txt", false);
    File nonEmptyDir = createFileOrDir(testDir, "nonEmpty", true);
    createFileOrDir(nonEmptyDir, "subDirOne", true);
    createFileOrDir(nonEmptyDir, "subDirTwo", true);

    JarDirPathConverter jarDirPathConverter = new JarDirPathConverter();
    List<Completion> completions = new ArrayList<>();
    jarDirPathConverter.getAllPossibleValues(completions, null, testDir.getPath(), null, null);

    assertThat(completions).hasSize(1);
    assertThat(completions.get(0).getValue()).endsWith("nonEmpty");
  }

  @Test
  public void itFindsDirsWithSubdirsAndJars() throws Exception {
    File testDir = createFileOrDir(tmpDir.getRoot(), "testTwo", true);
    File libDir = createFileOrDir(testDir, "lib", true);
    createFileOrDir(libDir, "file.jar", false);
    File nonEmptyDir = createFileOrDir(testDir, "nonEmpty", true);
    createFileOrDir(nonEmptyDir, "subDirOne", true);
    createFileOrDir(nonEmptyDir, "subDirTwo", true);

    JarDirPathConverter jarDirPathConverter = new JarDirPathConverter();
    List<Completion> completions = new ArrayList<>();
    jarDirPathConverter.getAllPossibleValues(completions, null, testDir.getPath(), null, null);

    assertThat(completions).hasSize(2);
    assertThat(completions).containsExactlyInAnyOrder(new Completion(nonEmptyDir.getPath()),
        new Completion(libDir.getPath()));
  }

  @Test
  public void itFindsNothingWithBadSearch() throws Exception {
    File testDir = createFileOrDir(tmpDir.getRoot(), "testTwo", true);
    File libDir = createFileOrDir(testDir, "lib", true);
    createFileOrDir(libDir, "file.txt", false);
    File nonEmptyDir = createFileOrDir(testDir, "nonEmpty", true);
    createFileOrDir(nonEmptyDir, "subDirOne", true);
    createFileOrDir(nonEmptyDir, "subDirTwo", true);

    JarDirPathConverter jarDirPathConverter = new JarDirPathConverter();
    List<Completion> completions = new ArrayList<>();
    jarDirPathConverter.getAllPossibleValues(completions, null, "garbage", null, null);

    assertThat(completions).isEmpty();
  }

  @Test
  public void itFindsNothing() throws Exception {
    File testDir = createFileOrDir(tmpDir.getRoot(), "testTwo", true);
    File libDir = createFileOrDir(testDir, "lib", true);
    createFileOrDir(libDir, "file.txt", false);
    createFileOrDir(testDir, "empty", true);

    JarDirPathConverter jarDirPathConverter = new JarDirPathConverter();
    List<Completion> completions = new ArrayList<>();
    jarDirPathConverter.getAllPossibleValues(completions, null, testDir.getPath(), null, null);

    assertThat(completions).isEmpty();
  }

  private File createFileOrDir(File parent, String fileOrDirName, boolean isDir) throws Exception {
    File fileOrDir = new File(parent, fileOrDirName);
    boolean success = fileOrDir.exists() || (isDir ? fileOrDir.mkdir() : fileOrDir.createNewFile());
    assertThat(success).isTrue();

    return fileOrDir;
  }
}
