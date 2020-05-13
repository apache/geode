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

import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class AlterDiskStoreCommandIntegrationTest {

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Rule
  public GfshParserRule gfsh = new GfshParserRule();
  private GfshCommand command;
  private static final String IF_FILE_EXT = ".if";


  @Before
  public void before() {
    command = spy(AlterOfflineDiskStoreCommand.class);
  }

  @Test
  public void removeOptionMustBeUsedAlone() throws IOException {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, "diskStoreName");
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, "regionName");
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, tempDir.getRoot().toString());
    csb.addOption(CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL, "5");
    csb.addOption(CliStrings.ALTER_DISK_STORE__REMOVE, "true");
    String commandString = csb.toString();

    tempDir.newFile("BACKUPdiskStoreName.if");
    gfsh.executeAndAssertThat(command, commandString).statusIsError()
        .containsOutput("Cannot use the --remove=true parameter with any other parameters");
  }

  @Test
  public void testDirValidation() throws IOException {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, "diskStoreName");
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, "regionName");
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, "wrongDiskDir");
    csb.addOption(CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL, "5");
    String commandString = csb.toString();

    File tempFile = tempDir.newFile("BACKUPdiskStoreName" + IF_FILE_EXT);
    gfsh.executeAndAssertThat(command, commandString).statusIsError()
        .containsOutput("Could not find: \"wrongDiskDir" + File.separator + tempFile.getName());
  }

  @Test
  public void testNameValidation() throws IOException {
    String diskStoreName = "diskStoreName";
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, diskStoreName);
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, "regionName");
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, tempDir.getRoot().toString());
    csb.addOption(CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL, "5");
    String commandString = csb.toString();

    gfsh.executeAndAssertThat(command, commandString).statusIsError()
        .containsOutput(
            "Could not find: \"" + tempDir.getRoot().toString() + File.separator + "BACKUP"
                + diskStoreName + IF_FILE_EXT);
  }

}
