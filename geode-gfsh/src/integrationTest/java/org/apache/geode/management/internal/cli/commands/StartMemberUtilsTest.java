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
import static org.junit.jupiter.api.condition.JRE.JAVA_13;
import static org.junit.jupiter.api.condition.JRE.JAVA_14;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.util.IOUtils;

@EnableRuleMigrationSupport
public class StartMemberUtilsTest {

  @TempDir
  private File temporaryFolder;

  @Rule
  public RestoreSystemProperties restorer = new RestoreSystemProperties();

  @Test
  public void workingDirCantBeCreatedThrowsException() {
    File userSpecifiedDir = spy(new File("cantCreateDir"));
    when(userSpecifiedDir.exists()).thenReturn(false);
    when(userSpecifiedDir.mkdirs()).thenReturn(false);
    assertThatThrownBy(
        () -> StartMemberUtils.resolveWorkingDirectory(userSpecifiedDir))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Could not create directory");
  }

  @Test
  public void workingDirDefaultsToMemberName() {
    String workingDir = StartMemberUtils.resolveWorkingDirectory(null, "server1");
    assertThat(new File(workingDir)).exists();
    assertThat(workingDir).endsWith("server1");
  }

  @Test
  public void workingDirGetsCreatedIfNecessary() {
    File workingDir = new File(temporaryFolder, "foo");
    FileUtils.deleteQuietly(workingDir);
    String workingDirString = workingDir.getAbsolutePath();
    String resolvedWorkingDir =
        StartMemberUtils.resolveWorkingDirectory(workingDirString, "server1");
    assertThat(new File(resolvedWorkingDir)).exists();
    assertThat(workingDirString).endsWith("foo");
  }

  @Test
  public void testWorkingDirWithRelativePath() {
    Path relativePath = Paths.get("some").resolve("relative").resolve("path");
    assertThat(relativePath.isAbsolute()).isFalse();
    String resolvedWorkingDir =
        StartMemberUtils.resolveWorkingDirectory(relativePath.toString(), "server1");
    assertThat(resolvedWorkingDir).isEqualTo(relativePath.toAbsolutePath().toString());
  }

  private static final int EXPECTED_PID = 12345;

  @Test
  public void testReadPid() throws IOException {
    File pidFile = new File(temporaryFolder, "my.pid");

    assertThat(pidFile.createNewFile()).isTrue();

    pidFile.deleteOnExit();
    writePid(pidFile);

    final int actualPid = StartMemberUtils.readPid(pidFile);
    assertThat(actualPid).isEqualTo(EXPECTED_PID);
  }

  private void writePid(final File pidFile) throws IOException {
    final FileWriter fileWriter = new FileWriter(pidFile, false);
    fileWriter.write(String.valueOf(EXPECTED_PID));
    fileWriter.write("\n");
    fileWriter.flush();
    IOUtils.close(fileWriter);
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

    currentClasspath = prependToClasspath(customGeodeCore, currentClasspath);
    currentClasspath =
        prependToClasspath(prependJar2.toString(), currentClasspath);
    prependToClasspath(prependJar1.toString(), currentClasspath);

    String[] otherJars = new String[] {
        FileSystems.getDefault().getPath(otherJar1.toString()).toAbsolutePath().toString(),
        FileSystems.getDefault().getPath(otherJar2.toString()).toAbsolutePath().toString()};

    String gemfireClasspath = StartMemberUtils.toClasspath(true, otherJars);
    assertThat(gemfireClasspath).startsWith(customGeodeCore);

    gemfireClasspath = StartMemberUtils.toClasspath(false, otherJars);
    assertThat(gemfireClasspath).startsWith(customGeodeCore);
  }

  private String prependToClasspath(String prependJarPath,
      String currentClasspath) {
    System.setProperty("java.class.path", prependJarPath + File.pathSeparator + currentClasspath);
    return System.getProperty("java.class.path");
  }

  @Test
  public void testExtensionsJars() throws IOException {

    // when there is no `extensions` directory
    String gemfireClasspath = StartMemberUtils.toClasspath(true, new String[0]);
    assertThat(gemfireClasspath).doesNotContain("extensions");

    // when there is a `test.jar` in `extensions` directory
    File folder = new File(temporaryFolder, "extensions");
    Files.createDirectories(folder.toPath());
    File jarFile = new File(folder, "test.jar");
    assertThat(jarFile.createNewFile()).isTrue();
    gemfireClasspath = StartMemberUtils.toClasspath(true, new String[] {jarFile.getAbsolutePath()});
    assertThat(gemfireClasspath).contains(jarFile.getName());

  }

  @EnabledForJreRange(max = JAVA_13)
  @Test
  public void testAddMaxHeapWithCMS() {
    List<String> baseCommandLine = new ArrayList<>();

    // Empty Max Heap Option
    StartMemberUtils.addMaxHeap(baseCommandLine, null);
    assertThat(baseCommandLine).isEmpty();

    StartMemberUtils.addMaxHeap(baseCommandLine, "");
    assertThat(baseCommandLine).isEmpty();

    // Only Max Heap Option Set
    StartMemberUtils.addMaxHeap(baseCommandLine, "32g");
    assertThat(baseCommandLine).containsExactly("-Xmx32g", "-XX:+UseConcMarkSweepGC",
        "-XX:CMSInitiatingOccupancyFraction=" + MemberJvmOptions.CMS_INITIAL_OCCUPANCY_FRACTION);

    // All Options Set
    List<String> customCommandLine = new ArrayList<>(
        Arrays.asList("-XX:+UseConcMarkSweepGC", "-XX:CMSInitiatingOccupancyFraction=30"));
    StartMemberUtils.addMaxHeap(customCommandLine, "16g");
    assertThat(customCommandLine).containsExactly("-XX:+UseConcMarkSweepGC",
        "-XX:CMSInitiatingOccupancyFraction=30", "-Xmx16g");
  }

  @EnabledForJreRange(min = JAVA_14)
  @Test
  public void testAddMaxHeapWithoutCMS() {
    List<String> baseCommandLine = new ArrayList<>();

    // Empty Max Heap Option
    StartMemberUtils.addMaxHeap(baseCommandLine, null);
    assertThat(baseCommandLine).isEmpty();

    StartMemberUtils.addMaxHeap(baseCommandLine, "");
    assertThat(baseCommandLine).isEmpty();

    // Only Max Heap Option Set
    StartMemberUtils.addMaxHeap(baseCommandLine, "32g");
    assertThat(baseCommandLine).containsExactly("-Xmx32g");
  }

  @Test
  public void whenResolveWorkingDirectoryIsCalledWithTwoNonEmptyStrings_thenTheUserSpecifiedDirectoryIsReturned() {
    String userSpecifiedDir = "locator1Directory";
    String memberNameDir = "member1";
    assertThat(StartMemberUtils.resolveWorkingDirectory(userSpecifiedDir, memberNameDir))
        .isEqualTo(new File(userSpecifiedDir).getAbsolutePath());
  }

  @Test
  public void whenResolveWorkingDirectoryIsCalledWithNullUserSpecifiedDir_thenTheMemberNameDirectoryIsReturned() {
    String memberNameDir = "member1";
    assertThat(StartMemberUtils.resolveWorkingDirectory(null, memberNameDir))
        .isEqualTo(new File(memberNameDir).getAbsolutePath());
  }

  @Test
  public void whenResolveWorkingDirectoryIsCalledWithEmptyUserSpecifiedDir_thenTheMemberNameDirectoryIsReturned() {
    String userSpecifiedDir = "";
    String memberNameDir = "member1";
    assertThat(StartMemberUtils.resolveWorkingDirectory(userSpecifiedDir, memberNameDir))
        .isEqualTo(new File(memberNameDir).getAbsolutePath());
  }

  @Test
  public void whenResolveWorkingDirectoryIsCalledWithUserSpecifiedDirAsDot_thenTheUserSpecifiedDirectoryIsReturned() {
    String userSpecifiedDir = ".";
    String memberNameDir = "member1";
    assertThat(StartMemberUtils.resolveWorkingDirectory(userSpecifiedDir, memberNameDir))
        .isEqualTo(
            StartMemberUtils.resolveWorkingDirectory(new File(userSpecifiedDir)));
  }

  @Test
  public void whenResolveWorkingDirectoryIsCalledWithUserSpecifiedDirAsAbsolutePath_thenTheUserSpecifiedDirectoryIsReturned() {
    String userSpecifiedDir = new File(System.getProperty("user.dir")).getAbsolutePath();
    String memberNameDir = "member1";
    assertThat(StartMemberUtils.resolveWorkingDirectory(userSpecifiedDir, memberNameDir))
        .isEqualTo(new File(userSpecifiedDir).getAbsolutePath());
  }
}
