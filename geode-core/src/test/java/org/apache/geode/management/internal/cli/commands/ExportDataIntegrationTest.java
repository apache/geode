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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class ExportDataIntegrationTest extends SnapshotDataIntegrationTest {
  @Test
  public void testExport() throws Exception {
    String exportCommand = buildBaseExportCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, getSnapshotFile().toString()).getCommandString();
    gfsh.executeCommand(exportCommand);
    assertThat(gfsh.getGfshOutput()).contains("Data successfully exported ");
  }

  @Test
  public void testParallelExport() throws Exception {
    String exportCommand =
        buildBaseExportCommand().addOption(CliStrings.EXPORT_DATA__DIR, getSnapshotDir().toString())
            .addOption(CliStrings.EXPORT_DATA__PARALLEL, "true").getCommandString();
    gfsh.executeCommand(exportCommand);
    assertThat(gfsh.getGfshOutput()).contains("Data successfully exported ");
  }

  @Test
  public void testInvalidMember() throws Exception {
    String invalidMemberName = "invalidMember";
    String invalidMemberCommand = new CommandStringBuilder(CliStrings.EXPORT_DATA)
        .addOption(CliStrings.MEMBER, invalidMemberName)
        .addOption(CliStrings.EXPORT_DATA__REGION, TEST_REGION_NAME)
        .addOption(CliStrings.EXPORT_DATA__FILE, getSnapshotFile().toString()).getCommandString();
    gfsh.executeCommand(invalidMemberCommand);
    assertThat(gfsh.getGfshOutput()).contains("Member " + invalidMemberName + " not found");
  }

  @Test
  public void testNonExistentRegion() throws Exception {
    String nonExistentRegionCommand = new CommandStringBuilder(CliStrings.EXPORT_DATA)
        .addOption(CliStrings.MEMBER, server.getName())
        .addOption(CliStrings.EXPORT_DATA__REGION, "/nonExistentRegion")
        .addOption(CliStrings.EXPORT_DATA__FILE, getSnapshotFile().toString()).getCommandString();
    gfsh.executeCommand(nonExistentRegionCommand);
    assertThat(gfsh.getGfshOutput()).contains("Could not process command due to error. Region");
  }

  @Test
  public void testInvalidFile() throws Exception {
    String invalidFileCommand = buildBaseExportCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, getSnapshotFile().toString() + ".invalid")
        .getCommandString();
    gfsh.executeCommand(invalidFileCommand);
    assertThat(gfsh.getGfshOutput())
        .contains("Invalid file type, the file extension must be \".gfd\"");
  }

  @Test
  public void testMissingRegion() throws Exception {
    String missingRegionCommand = new CommandStringBuilder(CliStrings.EXPORT_DATA)
        .addOption(CliStrings.MEMBER, server.getName())
        .addOption(CliStrings.EXPORT_DATA__FILE, getSnapshotFile().toString()).getCommandString();
    gfsh.executeCommand(missingRegionCommand);
    assertThat(gfsh.getGfshOutput()).contains("You should specify option");
  }

  @Test
  public void testMissingMember() throws Exception {
    String missingMemberCommand = new CommandStringBuilder(CliStrings.EXPORT_DATA)
        .addOption(CliStrings.EXPORT_DATA__REGION, TEST_REGION_NAME)
        .addOption(CliStrings.EXPORT_DATA__FILE, getSnapshotFile().toString()).getCommandString();
    gfsh.executeCommand(missingMemberCommand);
    assertThat(gfsh.getGfshOutput()).contains("You should specify option");
  }

  @Test
  public void testMissingFileAndDirectory() throws Exception {
    String missingFileAndDirCommand = buildBaseExportCommand().getCommandString();
    gfsh.executeCommand(missingFileAndDirCommand);
    assertThat(gfsh.getGfshOutput()).contains("Must specify a location to save snapshot");
  }

  @Test
  public void testParallelExportWithOnlyFile() throws Exception {
    String exportCommand = buildBaseExportCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, getSnapshotFile().toString())
        .addOption(CliStrings.EXPORT_DATA__PARALLEL, "true").getCommandString();
    gfsh.executeCommand(exportCommand);
    assertThat(gfsh.getGfshOutput()).contains("Must specify a directory to save snapshot files");
  }

  @Test
  public void testDirectoryCommandSupersedesFile() throws Exception {
    String exportCommand = buildBaseExportCommand()
        .addOption(CliStrings.EXPORT_DATA__FILE, getSnapshotFile().toString())
        .addOption(CliStrings.EXPORT_DATA__DIR, getSnapshotDir().toString()).getCommandString();
    gfsh.executeCommand(exportCommand);
    assertThat(gfsh.getGfshOutput()).contains("Data successfully exported ");

    assertTrue(Files.exists(getSnapshotDir()));
    assertFalse(Files.exists(getSnapshotFile()));
  }
}
