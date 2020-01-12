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
package org.apache.geode.internal.cache.backup;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class UnixScriptGeneratorTest {
  private ScriptGenerator scriptGenerator;
  private File outputFile;
  private BufferedWriter writer;

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    scriptGenerator = new UnixScriptGenerator();
    outputFile = tempDir.newFile();
    writer = Files.newBufferedWriter(outputFile.toPath());
  }

  @Test
  public void writePreambleTest() throws Exception {
    scriptGenerator.writePreamble(writer);
    writer.flush();
    List<String> output = Files.readAllLines(outputFile.toPath());
    assertThat(output).hasSize(2);
    assertThat(output).containsExactly("#!/bin/bash -e", "cd `dirname $0`");
  }

  @Test
  public void writeCommentTest() throws Exception {
    scriptGenerator.writeComment(writer, "comment1");
    scriptGenerator.writeComment(writer, "comment2");
    writer.flush();
    List<String> output = Files.readAllLines(outputFile.toPath());
    assertThat(output).hasSize(2);
    assertThat(output).containsExactly("# comment1", "# comment2");
  }

  @Test
  public void writeCopyDirectoryContentsTest() throws Exception {
    File dirWithoutBackups = tempDir.newFolder("empty");
    scriptGenerator.writeCopyDirectoryContents(writer, null, dirWithoutBackups, false);
    File dirWithBackups = tempDir.newFolder("hasContents");
    File backupDirToCopy = tempDir.newFolder("backup");
    scriptGenerator.writeCopyDirectoryContents(writer, backupDirToCopy, dirWithBackups, true);
    writer.flush();
    List<String> output = Files.readAllLines(outputFile.toPath());
    assertThat(output).hasSize(2);
    assertThat(output).containsExactly("mkdir -p '" + dirWithBackups + "'",
        "cp -rp '" + backupDirToCopy + "'/* '" + dirWithBackups + "'");
  }

  @Test
  public void writeCopyFileTest() throws Exception {
    File source = tempDir.newFile("backup");
    File destinaiton = tempDir.newFile("original");
    scriptGenerator.writeCopyFile(writer, source, destinaiton);
    writer.flush();
    List<String> output = Files.readAllLines(outputFile.toPath());
    assertThat(output).hasSize(2);
    assertThat(output).containsExactly("mkdir -p '" + source.getParent() + "'",
        "cp -p '" + source + "' '" + destinaiton + "'");
  }

  @Test
  public void writeExistenceTest() throws Exception {
    File file = tempDir.newFile("testFile");
    scriptGenerator.writeExistenceTest(writer, file);
    writer.flush();
    List<String> output = Files.readAllLines(outputFile.toPath());
    assertThat(output).hasSize(1);
    assertThat(output).containsExactly("test -e '" + file + "' && echo '"
        + RestoreScript.REFUSE_TO_OVERWRITE_MESSAGE + file + "' && exit 1 ");
  }

  @Test
  public void writeExitTest() throws Exception {
    scriptGenerator.writeExit(writer);
    writer.flush();
    List<String> output = Files.readAllLines(outputFile.toPath());
    assertThat(output).hasSize(0);
  }

  @Test
  public void getScriptNameTest() throws Exception {
    assertThat(scriptGenerator.getScriptName()).isEqualTo("restore.sh");
  }

}
