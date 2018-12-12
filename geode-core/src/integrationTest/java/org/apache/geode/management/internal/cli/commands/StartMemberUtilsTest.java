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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
  public void workingDirCantBeCreatedThrowsException() {
    File userSpecifiedDir = spy(new File("cantCreateDir"));
    when(userSpecifiedDir.exists()).thenReturn(false);
    when(userSpecifiedDir.mkdirs()).thenReturn(false);
    assertThatThrownBy(
        () -> StartMemberUtils.resolveWorkingDir(userSpecifiedDir, new File("server1")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Could not create directory");
  }

  @Test
  public void workingDirDefaultsToMemberName() {
    String workingDir = StartMemberUtils.resolveWorkingDir(null, new File("server1"));
    assertThat(new File(workingDir)).exists();
    assertThat(workingDir).endsWith("server1");
  }

  @Test
  public void workingDirGetsCreatedIfNecessary() throws Exception {
    File workingDir = temporaryFolder.newFolder("foo");
    FileUtils.deleteQuietly(workingDir);
    String workingDirString = workingDir.getAbsolutePath();
    String resolvedWorkingDir =
        StartMemberUtils.resolveWorkingDir(new File(workingDirString), new File("server1"));
    assertThat(new File(resolvedWorkingDir)).exists();
    assertThat(workingDirString).endsWith("foo");
  }

  @Test
  public void testWorkingDirWithRelativePath() throws Exception {
    Path relativePath = Paths.get("some").resolve("relative").resolve("path");
    assertThat(relativePath.isAbsolute()).isFalse();
    String resolvedWorkingDir =
        StartMemberUtils.resolveWorkingDir(new File(relativePath.toString()), new File("server1"));
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

  /**
   * Verify that the classpath for Geode always has the geode-core jar first in the list of paths.
   * On entry to the test there may (depending test platform) be a version of geode-core-*.jar on
   * the system classpath, but at an unknown position in the list. To make the test deterministic, a
   * dummy geode-core-*.jar path is inserted in the system classpath at a known, but not the first,
   * position.
   */
  @Test
  public void testGeodeOnClasspathIsFirst() {
    String currentClasspath = System.getProperty("java.class.path");
    final Path customGeodeJarPATH =
        Paths.get("/custom", "geode-core-" + GemFireVersion.getGemFireVersion() + ".jar");
    final Path prependJar1 = Paths.get("/prepend", "pre1.jar");
    final Path prependJar2 = Paths.get("/prepend", "pre2.jar");
    final Path otherJar1 = Paths.get("/other", "one.jar");
    final Path otherJar2 = Paths.get("/other", "two.jar");
    final String customGeodeCore = FileSystems.getDefault().getPath(customGeodeJarPATH.toString())
        .toAbsolutePath().toString();

    currentClasspath = prependToClasspath("java.class.path", customGeodeCore, currentClasspath);
    currentClasspath =
        prependToClasspath("java.class.path", prependJar2.toString(), currentClasspath);
    prependToClasspath("java.class.path", prependJar1.toString(), currentClasspath);

    String[] otherJars = new String[] {
        FileSystems.getDefault().getPath(otherJar1.toString()).toAbsolutePath().toString(),
        FileSystems.getDefault().getPath(otherJar2.toString()).toAbsolutePath().toString()};

    String gemfireClasspath = StartMemberUtils.toClasspath(true, otherJars);
    assertThat(gemfireClasspath).startsWith(customGeodeCore);

    gemfireClasspath = StartMemberUtils.toClasspath(false, otherJars);
    assertThat(gemfireClasspath).startsWith(customGeodeCore);
  }

  private String prependToClasspath(String systemPropertyKey, String prependJarPath,
      String currentClasspath) {
    System.setProperty(systemPropertyKey, prependJarPath + File.pathSeparator + currentClasspath);
    return System.getProperty(systemPropertyKey);
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
