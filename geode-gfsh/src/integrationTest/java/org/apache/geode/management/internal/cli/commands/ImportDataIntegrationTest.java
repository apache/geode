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
 *
 */

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class ImportDataIntegrationTest {
  private static final String TEST_REGION_NAME = "testRegion";
  private static final String SNAPSHOT_FILE = "snapshot.gfd";
  private static final String SNAPSHOT_DIR = "snapshots";
  private static final int DATA_POINTS = 10;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withRegion(RegionShortcut.PARTITION, TEST_REGION_NAME).withEmbeddedLocator();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private Region<String, String> region;
  private Path snapshotFile;
  private Path snapshotDir;

  @Before
  public void setup() throws Exception {
    gfsh.connectAndVerify(server.getEmbeddedLocatorPort(), GfshCommandRule.PortType.locator);
    region = server.getCache().getRegion(TEST_REGION_NAME);
    loadRegion("value");
    Path basePath = tempDir.getRoot().toPath();
    snapshotFile = basePath.resolve(SNAPSHOT_FILE);
    snapshotDir = basePath.resolve(SNAPSHOT_DIR);
  }

  @Test
  public void testExportImport() {
    String exportCommand = buildBaseExportCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, snapshotFile.toString()).getCommandString();
    gfsh.executeAndAssertThat(exportCommand).statusIsSuccess();

    loadRegion("");

    String importCommand = buildBaseImportCommand()
        .addOption(CliStrings.IMPORT_DATA__FILE, snapshotFile.toString()).getCommandString();
    gfsh.executeAndAssertThat(importCommand).statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Data imported from file");
    validateImport("value");
  }

  @Test
  public void testExportImportRelativePath() throws IOException {
    String exportCommand = buildBaseExportCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, SNAPSHOT_FILE).getCommandString();
    gfsh.executeAndAssertThat(exportCommand).statusIsSuccess();

    loadRegion("");

    String importCommand = buildBaseImportCommand()
        .addOption(CliStrings.IMPORT_DATA__FILE, SNAPSHOT_FILE).getCommandString();
    gfsh.executeAndAssertThat(importCommand).statusIsSuccess();
    Files.deleteIfExists(Paths.get(SNAPSHOT_FILE));
    assertThat(gfsh.getGfshOutput()).contains("Data imported from file");
    validateImport("value");
  }

  @Test
  public void testParallelExportImport() {
    String exportCommand =
        buildBaseExportCommand().addOption(CliStrings.EXPORT_DATA__DIR, snapshotDir.toString())
            .addOption(CliStrings.EXPORT_DATA__PARALLEL, "true").getCommandString();
    gfsh.executeAndAssertThat(exportCommand).statusIsSuccess();

    loadRegion("");

    String importCommand =
        buildBaseImportCommand().addOption(CliStrings.IMPORT_DATA__DIR, snapshotDir.toString())
            .addOption(CliStrings.IMPORT_DATA__PARALLEL, "true").getCommandString();
    gfsh.executeAndAssertThat(importCommand).statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Data imported from file");

    validateImport("value");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testInvalidMember() {
    String invalidMemberName = "invalidMember";
    String invalidMemberCommand = new CommandStringBuilder(CliStrings.EXPORT_DATA)
        .addOption(CliStrings.MEMBER, invalidMemberName)
        .addOption(CliStrings.IMPORT_DATA__REGION, TEST_REGION_NAME)
        .addOption(CliStrings.IMPORT_DATA__FILE, snapshotFile.toString()).getCommandString();
    gfsh.executeCommand(invalidMemberCommand);
    assertThat(gfsh.getGfshOutput())
        .contains("Member " + invalidMemberName + " could not be found");
  }

  @Test
  public void testNonExistentRegion() {
    String nonExistentRegionCommand = new CommandStringBuilder(CliStrings.EXPORT_DATA)
        .addOption(CliStrings.MEMBER, server.getName())
        .addOption(CliStrings.IMPORT_DATA__REGION, "/nonExistentRegion")
        .addOption(CliStrings.IMPORT_DATA__FILE, snapshotFile.toString()).getCommandString();
    gfsh.executeAndAssertThat(nonExistentRegionCommand).statusIsError()
        .hasTableSection().hasColumn("Message")
        .containsExactlyInAnyOrder("Region : /nonExistentRegion not found");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testInvalidFile() {
    String invalidFileCommand = buildBaseImportCommand()
        .addOption(CliStrings.IMPORT_DATA__FILE, snapshotFile.toString() + ".invalid")
        .getCommandString();
    gfsh.executeCommand(invalidFileCommand);
    assertThat(gfsh.getGfshOutput())
        .contains("Invalid file type, the file extension must be \".gfd\"");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMissingFileAndDirectory() {
    String missingFileAndDirCommand = buildBaseImportCommand().getCommandString();
    gfsh.executeCommand(missingFileAndDirCommand);
    assertThat(gfsh.getGfshOutput()).contains("Must specify a location to load snapshot from");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testParallelWithOnlyFile() {
    String importCommand =
        buildBaseImportCommand().addOption(CliStrings.IMPORT_DATA__FILE, snapshotFile.toString())
            .addOption(CliStrings.IMPORT_DATA__PARALLEL, "true").getCommandString();
    gfsh.executeCommand(importCommand);
    assertThat(gfsh.getGfshOutput())
        .contains("Must specify a directory to load snapshot files from");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSpecifyingDirectoryAndFileCommands() {
    String importCommand =
        buildBaseImportCommand().addOption(CliStrings.IMPORT_DATA__FILE, snapshotFile.toString())
            .addOption(CliStrings.IMPORT_DATA__DIR, snapshotDir.toString()).getCommandString();
    gfsh.executeCommand(importCommand);
    assertThat(gfsh.getGfshOutput())
        .contains("Options \"file\" and \"dir\" cannot be specified at the same time");
  }

  private void validateImport(String value) {
    IntStream.range(0, DATA_POINTS).forEach(i -> assertEquals(value, region.get("key" + i)));
  }

  private void loadRegion(String value) {
    IntStream.range(0, DATA_POINTS).forEach(i -> region.put("key" + i, value));
  }

  private CommandStringBuilder buildBaseImportCommand() {
    return new CommandStringBuilder(CliStrings.IMPORT_DATA)
        .addOption(CliStrings.MEMBER, server.getName())
        .addOption(CliStrings.IMPORT_DATA__REGION, TEST_REGION_NAME);
  }

  private CommandStringBuilder buildBaseExportCommand() {
    return new CommandStringBuilder(CliStrings.EXPORT_DATA)
        .addOption(CliStrings.MEMBER, server.getName())
        .addOption(CliStrings.EXPORT_DATA__REGION, TEST_REGION_NAME);
  }
}
