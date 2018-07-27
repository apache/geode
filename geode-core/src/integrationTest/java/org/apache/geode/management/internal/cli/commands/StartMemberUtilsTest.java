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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.util.IOUtils;

public class StartMemberUtilsTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restorer = new RestoreSystemProperties();

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

  @Test
  public void testGeodeOnClasspathIsFirst() {
    String currentClasspath = System.getProperty("java.class.path");
    String customGeodeCore = FileSystems
        .getDefault().getPath("/custom/geode-core-" + GemFireVersion.getGemFireVersion() + ".jar")
        .toAbsolutePath().toString();
    System.setProperty("java.class.path", currentClasspath + File.pathSeparator + customGeodeCore);

    String[] otherJars = new String[] {
        FileSystems.getDefault().getPath("/other/one.jar").toAbsolutePath().toString(),
        FileSystems.getDefault().getPath("/other/two.jar").toAbsolutePath().toString()};

    String gemfireClasspath = StartMemberUtils.toClasspath(true, otherJars);
    assertThat(gemfireClasspath).startsWith(customGeodeCore);

    gemfireClasspath = StartMemberUtils.toClasspath(false, otherJars);
    assertThat(gemfireClasspath).startsWith(customGeodeCore);
  }

  @Test
  public void testExtensionsJars() throws IOException {

    // when there is no `extensions` directory
    String gemfireClasspath = StartMemberUtils.toClasspath(true, new String[0]);
    assertThat(gemfireClasspath).doesNotContain("extensions");

    // when there is a `test.jar` in `extensions` directory
    File folder = temporaryFolder.newFolder("extensions");
    File jarFile = new File(folder, "test.jar");
    jarFile.createNewFile();
    gemfireClasspath = StartMemberUtils.toClasspath(true, new String[] {jarFile.getAbsolutePath()});
    assertThat(gemfireClasspath).contains(jarFile.getName());

  }

  @Test
  public void testAddMaxHeap() {
    List<String> baseCommandLine = new ArrayList<>();

    // Empty Max Heap Option
    StartMemberUtils.addMaxHeap(baseCommandLine, null);
    assertThat(baseCommandLine.size()).isEqualTo(0);

    StartMemberUtils.addMaxHeap(baseCommandLine, "");
    assertThat(baseCommandLine.size()).isEqualTo(0);

    // Only Max Heap Option Set
    StartMemberUtils.addMaxHeap(baseCommandLine, "32g");
    assertThat(baseCommandLine.size()).isEqualTo(3);
    assertThat(baseCommandLine).containsExactly("-Xmx32g", "-XX:+UseConcMarkSweepGC",
        "-XX:CMSInitiatingOccupancyFraction=" + StartMemberUtils.CMS_INITIAL_OCCUPANCY_FRACTION);

    // All Options Set
    List<String> customCommandLine = new ArrayList<>(
        Arrays.asList("-XX:+UseConcMarkSweepGC", "-XX:CMSInitiatingOccupancyFraction=30"));
    StartMemberUtils.addMaxHeap(customCommandLine, "16g");
    assertThat(customCommandLine.size()).isEqualTo(3);
    assertThat(customCommandLine).containsExactly("-XX:+UseConcMarkSweepGC",
        "-XX:CMSInitiatingOccupancyFraction=30", "-Xmx16g");
  }

  private void writePid(final File pidFile, final int pid) throws IOException {
    final FileWriter fileWriter = new FileWriter(pidFile, false);
    fileWriter.write(String.valueOf(pid));
    fileWriter.write("\n");
    fileWriter.flush();
    IOUtils.close(fileWriter);
  }
}
