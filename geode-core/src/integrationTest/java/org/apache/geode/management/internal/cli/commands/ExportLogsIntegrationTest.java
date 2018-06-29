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

import java.io.File;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

@Category({IntegrationTest.class, LoggingTest.class})
public class ExportLogsIntegrationTest {
  @ClassRule
  public static LocatorStarterRule locator =
      new LocatorStarterRule().withWorkingDir().withLogFile().withAutoStart();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void connect() throws Exception {
    gfsh.connectAndVerify(locator);
  }

  public File getWorkingDirectory() throws Exception {
    return locator.getWorkingDir();
  }

  @Test
  public void testInvalidMember() throws Exception {
    gfsh.executeCommand("export logs --member=member1,member2");
    assertThat(gfsh.getGfshOutput()).contains("No Members Found");
  }

  @Test
  public void testNothingToExport() throws Exception {
    gfsh.executeCommand("export logs --stats-only");
    assertThat(gfsh.getGfshOutput()).contains("No files to be exported.");
  }

  @Test
  public void withFiles_savedToLocatorWorkingDir() throws Exception {
    String[] extensions = {"zip"};
    // Expects locator to produce file in own working directory when connected via JMX
    gfsh.executeCommand("export logs");
    assertThat(FileUtils.listFiles(getWorkingDirectory(), extensions, false)).isNotEmpty();
  }

  @Test
  public void withFiles_savedToLocatorSpecifiedRelativeDir() throws Exception {
    String[] extensions = {"zip"};
    Path workingDirPath = getWorkingDirectory().toPath();
    Path subdirPath = workingDirPath.resolve("some").resolve("test").resolve("directory");
    Path relativeDir = workingDirPath.relativize(subdirPath);
    // Expects locator to produce file in own working directory when connected via JMX
    gfsh.executeCommand("export logs --dir=" + relativeDir.toString());
    assertThat(FileUtils.listFiles(getWorkingDirectory(), extensions, false)).isEmpty();
    assertThat(FileUtils.listFiles(getWorkingDirectory(), extensions, true)).isNotEmpty();
    assertThat(FileUtils.listFiles(subdirPath.toFile(), extensions, false)).isNotEmpty();
  }

  @Test
  public void withFiles_savedToLocatorSpecifiedAbsoluteDir() throws Exception {
    String[] extensions = {"zip"};
    Path workingDirPath = getWorkingDirectory().toPath();
    Path absoluteDirPath =
        workingDirPath.resolve("some").resolve("test").resolve("directory").toAbsolutePath();
    // Expects locator to produce file in own working directory when connected via JMX
    gfsh.executeCommand("export logs --dir=" + absoluteDirPath.toString());
    assertThat(FileUtils.listFiles(getWorkingDirectory(), extensions, false)).isEmpty();
    assertThat(FileUtils.listFiles(getWorkingDirectory(), extensions, true)).isNotEmpty();
    assertThat(FileUtils.listFiles(absoluteDirPath.toFile(), extensions, false)).isNotEmpty();
  }
}
