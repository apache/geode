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

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class StartMemberUtilsTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void workingDirDefaultsToMemberName() {
    String workingDir = StartMemberUtils.resolveWorkingDir(null, "server1");
    assertThat(new File(workingDir)).exists();
    assertThat(workingDir).endsWith("server1");
  }

  @Test
  public void workingDirGetsCreatedIfNecessary() throws Exception {
    File workingDir = temporaryFolder.newFolder("foo");
    FileUtils.deleteQuietly(workingDir);
    String workingDirString = workingDir.getAbsolutePath();
    String resolvedWorkingDir = StartMemberUtils.resolveWorkingDir(workingDirString, "server1");
    assertThat(new File(resolvedWorkingDir)).exists();
    assertThat(workingDirString).endsWith("foo");
  }

  @Test
  public void testWorkingDirWithRelativePath() throws Exception {
    Path relativePath = Paths.get("some").resolve("relative").resolve("path");
    assertThat(relativePath.isAbsolute()).isFalse();
    String resolvedWorkingDir =
        StartMemberUtils.resolveWorkingDir(relativePath.toString(), "server1");
    assertThat(resolvedWorkingDir).isEqualTo(relativePath.toAbsolutePath().toString());
  }

  @Test
  public void testReadPid() throws IOException {
    final int expectedPid = 12345;
    File folder = temporaryFolder.newFolder();
    File pidFile = new File(folder, "my.pid");

    assertTrue(pidFile.createNewFile());

    pidFile.deleteOnExit();
    writePid(pidFile, expectedPid);

    final int actualPid = StartMemberUtils.readPid(pidFile);
    assertEquals(expectedPid, actualPid);
  }

  private void writePid(final File pidFile, final int pid) throws IOException {
    final FileWriter fileWriter = new FileWriter(pidFile, false);
    fileWriter.write(String.valueOf(pid));
    fileWriter.write("\n");
    fileWriter.flush();
    IOUtils.close(fileWriter);
  }
}
