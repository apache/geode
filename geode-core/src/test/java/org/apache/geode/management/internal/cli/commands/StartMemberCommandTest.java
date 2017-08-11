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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class StartMemberCommandTest {
  private StartMemberCommand startMemberCommand;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void before() {
    startMemberCommand = new StartMemberCommand();
  }

  @Test
  public void testAddGemFirePropertyFileToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addGemFirePropertyFile(commandLine, null);
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addGemFirePropertyFile(commandLine, null);
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addGemFirePropertyFile(commandLine, null);
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addGemFirePropertyFile(commandLine, new File("/path/to/gemfire.properties"));
    assertFalse(commandLine.isEmpty());
    assertTrue(commandLine.contains("-DgemfirePropertyFile=/path/to/gemfire.properties"));
  }

  @Test
  public void testAddGemFireSystemPropertiesToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addGemFireSystemProperties(commandLine, new Properties());
    assertTrue(commandLine.isEmpty());

    final Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(LOCATORS, "localhost[11235]");
    gemfireProperties.setProperty(LOG_LEVEL, "config");
    gemfireProperties.setProperty(LOG_FILE, org.apache.commons.lang.StringUtils.EMPTY);
    gemfireProperties.setProperty(MCAST_PORT, "0");
    gemfireProperties.setProperty(NAME, "machine");
    startMemberCommand.addGemFireSystemProperties(commandLine, gemfireProperties);

    assertFalse(commandLine.isEmpty());
    assertEquals(4, commandLine.size());

    for (final String propertyName : gemfireProperties.stringPropertyNames()) {
      final String propertyValue = gemfireProperties.getProperty(propertyName);
      if (org.apache.commons.lang.StringUtils.isBlank(propertyValue)) {
        for (final String systemProperty : commandLine) {
          assertFalse(systemProperty.startsWith(
              "-D" + DistributionConfig.GEMFIRE_PREFIX + "".concat(propertyName).concat("=")));
        }
      } else {
        assertTrue(commandLine.contains("-D" + DistributionConfig.GEMFIRE_PREFIX
            + "".concat(propertyName).concat("=").concat(propertyValue)));
      }
    }
  }

  @Test
  public void testAddGemFireSystemPropertiesToCommandLineWithRestAPI() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addGemFireSystemProperties(commandLine, new Properties());
    assertTrue(commandLine.isEmpty());
    final Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(LOCATORS, "localhost[11235]");
    gemfireProperties.setProperty(LOG_LEVEL, "config");
    gemfireProperties.setProperty(LOG_FILE, "");
    gemfireProperties.setProperty(MCAST_PORT, "0");
    gemfireProperties.setProperty(NAME, "machine");
    gemfireProperties.setProperty(START_DEV_REST_API, "true");
    gemfireProperties.setProperty(HTTP_SERVICE_PORT, "8080");
    gemfireProperties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");

    startMemberCommand.addGemFireSystemProperties(commandLine, gemfireProperties);

    assertFalse(commandLine.isEmpty());
    assertEquals(7, commandLine.size());

    for (final String propertyName : gemfireProperties.stringPropertyNames()) {
      final String propertyValue = gemfireProperties.getProperty(propertyName);
      if (StringUtils.isBlank(propertyValue)) {
        for (final String systemProperty : commandLine) {
          assertFalse(systemProperty.startsWith(
              "-D" + DistributionConfig.GEMFIRE_PREFIX + "".concat(propertyName).concat("=")));
        }
      } else {
        assertTrue(commandLine.contains("-D" + DistributionConfig.GEMFIRE_PREFIX
            + "".concat(propertyName).concat("=").concat(propertyValue)));
      }
    }
  }

  @Test
  public void testAddInitialHeapToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addInitialHeap(commandLine, null);
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addInitialHeap(commandLine, org.apache.commons.lang.StringUtils.EMPTY);
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addInitialHeap(commandLine, " ");
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addInitialHeap(commandLine, "512M");
    assertFalse(commandLine.isEmpty());
    assertEquals("-Xms512M", commandLine.get(0));
  }

  @Test
  public void testAddJvmArgumentsAndOptionsToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addJvmArgumentsAndOptions(commandLine, null);
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addJvmArgumentsAndOptions(commandLine, new String[] {});
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addJvmArgumentsAndOptions(commandLine,
        new String[] {"-DmyProp=myVal", "-d64", "-server", "-Xprof"});
    assertFalse(commandLine.isEmpty());
    assertEquals(4, commandLine.size());
    assertEquals("-DmyProp=myVal", commandLine.get(0));
    assertEquals("-d64", commandLine.get(1));
    assertEquals("-server", commandLine.get(2));
    assertEquals("-Xprof", commandLine.get(3));
  }

  @Test
  public void testAddMaxHeapToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addMaxHeap(commandLine, null);
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addMaxHeap(commandLine, org.apache.commons.lang.StringUtils.EMPTY);
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addMaxHeap(commandLine, "  ");
    assertTrue(commandLine.isEmpty());
    startMemberCommand.addMaxHeap(commandLine, "1024M");
    assertFalse(commandLine.isEmpty());
    assertEquals(3, commandLine.size());
    assertEquals("-Xmx1024M", commandLine.get(0));
    assertEquals("-XX:+UseConcMarkSweepGC", commandLine.get(1));
    assertEquals(
        "-XX:CMSInitiatingOccupancyFraction=" + StartMemberCommand.CMS_INITIAL_OCCUPANCY_FRACTION,
        commandLine.get(2));
  }

  @Test(expected = AssertionError.class)
  public void testReadPidWithNull() {
    try {
      startMemberCommand.readPid(null);
    } catch (AssertionError expected) {
      assertEquals("The file from which to read the process ID (pid) cannot be null!",
          expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testReadPidWithNonExistingFile() {
    assertEquals(StartMemberCommand.INVALID_PID,
        startMemberCommand.readPid(new File("/path/to/non_existing/pid.file")));
  }

  @Test
  public void testGetSystemClasspath() {
    assertEquals(System.getProperty("java.class.path"), startMemberCommand.getSystemClasspath());
  }

  @Test
  public void testToClasspath() {
    final boolean EXCLUDE_SYSTEM_CLASSPATH = false;
    final boolean INCLUDE_SYSTEM_CLASSPATH = true;
    String[] jarFilePathnames =
        {"/path/to/user/libs/A.jar", "/path/to/user/libs/B.jar", "/path/to/user/libs/C.jar"};
    String[] userClasspaths = {"/path/to/classes:/path/to/libs/1.jar:/path/to/libs/2.jar",
        "/path/to/ext/libs/1.jar:/path/to/ext/classes:/path/to/ext/lib/10.jar"};
    String expectedClasspath = StartMemberCommand.GEODE_JAR_PATHNAME.concat(File.pathSeparator)
        .concat(toClasspath(userClasspaths)).concat(File.pathSeparator)
        .concat(toClasspath(jarFilePathnames));
    assertEquals(expectedClasspath,
        startMemberCommand.toClasspath(EXCLUDE_SYSTEM_CLASSPATH, jarFilePathnames, userClasspaths));
    expectedClasspath = StartMemberCommand.GEODE_JAR_PATHNAME.concat(File.pathSeparator)
        .concat(toClasspath(userClasspaths)).concat(File.pathSeparator)
        .concat(System.getProperty("java.class.path")).concat(File.pathSeparator)
        .concat(toClasspath(jarFilePathnames));
    assertEquals(expectedClasspath,
        startMemberCommand.toClasspath(INCLUDE_SYSTEM_CLASSPATH, jarFilePathnames, userClasspaths));
    expectedClasspath = StartMemberCommand.GEODE_JAR_PATHNAME.concat(File.pathSeparator)
        .concat(System.getProperty("java.class.path"));
    assertEquals(expectedClasspath,
        startMemberCommand.toClasspath(INCLUDE_SYSTEM_CLASSPATH, null, (String[]) null));
    assertEquals(StartMemberCommand.GEODE_JAR_PATHNAME,
        startMemberCommand.toClasspath(EXCLUDE_SYSTEM_CLASSPATH, null, (String[]) null));
    assertEquals(StartMemberCommand.GEODE_JAR_PATHNAME,
        startMemberCommand.toClasspath(EXCLUDE_SYSTEM_CLASSPATH, new String[0], ""));
  }

  @Test
  public void testToClassPathOrder() {
    String userClasspathOne = "/path/to/user/lib/a.jar:/path/to/user/classes";
    String userClasspathTwo =
        "/path/to/user/lib/x.jar:/path/to/user/lib/y.jar:/path/to/user/lib/z.jar";

    String expectedClasspath = startMemberCommand.getGemFireJarPath().concat(File.pathSeparator)
        .concat(userClasspathOne).concat(File.pathSeparator).concat(userClasspathTwo)
        .concat(File.pathSeparator).concat(System.getProperty("java.class.path"))
        .concat(File.pathSeparator).concat(StartMemberCommand.CORE_DEPENDENCIES_JAR_PATHNAME)
        .concat(File.pathSeparator).concat(StartMemberCommand.CORE_DEPENDENCIES_JAR_PATHNAME);

    String actualClasspath =
        startMemberCommand.toClasspath(true,
            new String[] {StartMemberCommand.CORE_DEPENDENCIES_JAR_PATHNAME,
                StartMemberCommand.CORE_DEPENDENCIES_JAR_PATHNAME},
            userClasspathOne, userClasspathTwo);

    assertEquals(expectedClasspath, actualClasspath);
  }

  @Test
  public void workingDirDefaultsToMemberName() {
    String workingDir = startMemberCommand.resolveWorkingDir(null, "server1");
    assertThat(new File(workingDir)).exists();
    assertThat(workingDir).endsWith("server1");
  }

  @Test
  public void workingDirGetsCreatedIfNecessary() throws Exception {
    File workingDir = temporaryFolder.newFolder("foo");
    FileUtils.deleteQuietly(workingDir);
    String workingDirString = workingDir.getAbsolutePath();
    String resolvedWorkingDir = startMemberCommand.resolveWorkingDir(workingDirString, "server1");
    assertThat(new File(resolvedWorkingDir)).exists();
    assertThat(workingDirString).endsWith("foo");
  }

  @Test
  public void testWorkingDirWithRelativePath() throws Exception {
    Path relativePath = Paths.get("some").resolve("relative").resolve("path");
    assertThat(relativePath.isAbsolute()).isFalse();
    String resolvedWorkingDir =
        startMemberCommand.resolveWorkingDir(relativePath.toString(), "server1");
    assertThat(resolvedWorkingDir).isEqualTo(relativePath.toAbsolutePath().toString());
  }


  @Test
  public void testReadPid() throws IOException {
    final int expectedPid = 12345;
    File folder = temporaryFolder.newFolder();
    File pidFile =
        new File(folder, getClass().getSimpleName() + "_" + testName.getMethodName() + ".pid");
    assertTrue(pidFile.createNewFile());
    pidFile.deleteOnExit();
    writePid(pidFile, expectedPid);

    final int actualPid = startMemberCommand.readPid(pidFile);

    assertEquals(expectedPid, actualPid);
  }

  private String toClasspath(final String... jarFilePathnames) {
    StringBuilder classpath = new StringBuilder(StringUtils.EMPTY);
    if (jarFilePathnames != null) {
      for (final String jarFilePathname : jarFilePathnames) {
        classpath.append((classpath.length() == 0) ? StringUtils.EMPTY : File.pathSeparator);
        classpath.append(jarFilePathname);
      }
    }
    return classpath.toString();
  }

  private void writePid(final File pidFile, final int pid) throws IOException {
    final FileWriter fileWriter = new FileWriter(pidFile, false);
    fileWriter.write(String.valueOf(pid));
    fileWriter.write("\n");
    fileWriter.flush();
    IOUtils.close(fileWriter);
  }
}
