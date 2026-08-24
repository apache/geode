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

import static org.apache.geode.management.internal.cli.functions.ExportDataFunction.EXPORT_DATA_DIRS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * End to end tests of the directories {@code export data} writes into: a live server exports into
 * the directories it is configured to permit, and refuses paths that resolve outside them.
 */
public class ExportDataPathValidationIntegrationTest {
  private static final String TEST_REGION_NAME = "testRegion";
  private static final int DATA_POINTS = 10;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withRegion(RegionShortcut.PARTITION, TEST_REGION_NAME).withEmbeddedLocator();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  /** The directory an operator has permitted this member to export into. */
  private Path permittedDir;

  /** Any other location on the server host. */
  private Path otherDir;

  private Region<String, Object> region;

  @Before
  public void setup() throws Exception {
    gfsh.connectAndVerify(server.getEmbeddedLocatorPort(), GfshCommandRule.PortType.locator);
    region = server.getCache().getRegion(TEST_REGION_NAME);
    IntStream.range(0, DATA_POINTS).forEach(i -> region.put("key" + i, "value" + i));

    permittedDir = temporaryFolder.newFolder("permitted").toPath();
    otherDir = temporaryFolder.newFolder("other").toPath();
    System.setProperty(EXPORT_DATA_DIRS_PROPERTY, permittedDir.toString());
  }

  /** The gfsh table wraps long messages, so compare against whitespace normalized output. */
  private String normalizedOutput() {
    return gfsh.getGfshOutput().replaceAll("\\s+", " ");
  }

  /**
   * Exports into a permitted directory work normally.
   */
  @Test
  public void exportIntoThePermittedDirectorySucceeds() {
    Path target = permittedDir.resolve("snapshot.gfd");

    gfsh.executeAndAssertThat(baseCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, target.toString())
        .getCommandString()).statusIsSuccess();

    assertThat(target).exists();
    assertThat(target.toFile().length()).isGreaterThan(0L);
  }

  /** The --dir form works the same way. */
  @Test
  public void exportIntoThePermittedDirectoryWithDirOptionSucceeds() {
    gfsh.executeAndAssertThat(baseCommand()
        .addOption(CliStrings.EXPORT_DATA__DIR, permittedDir.toString())
        .getCommandString()).statusIsSuccess();

    assertThat(permittedDir.resolve(TEST_REGION_NAME + ".gfd")).exists();
  }

  /**
   * An absolute path outside the permitted directories does not produce a file.
   */
  @Test
  public void exportToAnAbsolutePathOutsideThePermittedDirectoryIsRefused() {
    Path target = otherDir.resolve("snapshot.gfd");

    gfsh.executeAndAssertThat(baseCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, target.toString())
        .getCommandString()).statusIsError();

    assertThat(normalizedOutput()).contains("export directories configured for this member");
    assertThat(target).doesNotExist();
  }

  /**
   * A "../" in --dir is refused, and nothing is written at the location it points to.
   */
  @Test
  public void exportWithParentReferenceInDirOptionIsRefused() {
    String dirWithParentReference = permittedDir.resolve("..").resolve("other").toString();

    gfsh.executeAndAssertThat(baseCommand()
        .addOption(CliStrings.EXPORT_DATA__DIR, dirWithParentReference)
        .getCommandString()).statusIsError();

    assertThat(normalizedOutput()).contains("path segment");
    assertThat(otherDir.resolve(TEST_REGION_NAME + ".gfd")).doesNotExist();
  }

  /**
   * The same for --file: it is caught before the export is sent to the member.
   */
  @Test
  public void exportWithParentReferenceInFileOptionIsRefused() {
    String fileWithParentReference = permittedDir.resolve("..").resolve("other")
        .resolve("snapshot.gfd").toString();

    gfsh.executeAndAssertThat(baseCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, fileWithParentReference)
        .getCommandString()).statusIsError();

    assertThat(normalizedOutput()).contains("path segment");
    assertThat(otherDir.resolve("snapshot.gfd")).doesNotExist();
  }

  /**
   * An existing file outside the permitted directories keeps its contents.
   */
  @Test
  public void existingFileOutsideThePermittedDirectoryIsNotOverwritten() throws Exception {
    Path existingFile = otherDir.resolve("existing.gfd");
    String originalContent = "existing content";
    Files.write(existingFile, originalContent.getBytes(StandardCharsets.UTF_8));

    gfsh.executeAndAssertThat(baseCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, existingFile.toString())
        .getCommandString()).statusIsError();

    assertThat(new String(Files.readAllBytes(existingFile), StandardCharsets.UTF_8))
        .isEqualTo(originalContent);
  }

  /**
   * A parallel export does not create a directory tree outside the permitted directories.
   */
  @Test
  public void parallelExportOutsideThePermittedDirectoryCreatesNoDirectories() {
    Path newTree = otherDir.resolve("created/by/export");

    gfsh.executeAndAssertThat(baseCommand()
        .addOption(CliStrings.EXPORT_DATA__DIR, newTree.toString())
        .addOption(CliStrings.EXPORT_DATA__PARALLEL, "true")
        .getCommandString()).statusIsError();

    assertThat(newTree).doesNotExist();
  }

  /**
   * Without configuration the member permits only its own working directory.
   */
  @Test
  public void withoutConfigurationExportOutsideTheWorkingDirectoryIsRefused() {
    System.clearProperty(EXPORT_DATA_DIRS_PROPERTY);
    Path target = otherDir.resolve("snapshot.gfd");

    gfsh.executeAndAssertThat(baseCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, target.toString())
        .getCommandString()).statusIsError();

    assertThat(target).doesNotExist();
    assertThat(normalizedOutput())
        .contains(new File(System.getProperty("user.dir")).getName());
  }

  private CommandStringBuilder baseCommand() {
    return new CommandStringBuilder(CliStrings.EXPORT_DATA)
        .addOption(CliStrings.MEMBER, server.getName())
        .addOption(CliStrings.EXPORT_DATA__REGION, TEST_REGION_NAME);
  }
}
