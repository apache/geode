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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class AlterDiskStoreJUnitTest {
  @Rule
  public GfshParserRule gfsh = new GfshParserRule();

  private GfshCommand command;

  @Before
  public void before() {
    command = spy(AlterOfflineDiskStoreCommand.class);
  }

  @Test
  public void removeOptionMustBeUsedAlone() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_DISK_STORE);
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKSTORENAME, "diskStoreName");
    csb.addOption(CliStrings.ALTER_DISK_STORE__REGIONNAME, "regionName");
    csb.addOption(CliStrings.ALTER_DISK_STORE__DISKDIRS, "./someDirectory");
    csb.addOption(CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL, "5");
    csb.addOption(CliStrings.ALTER_DISK_STORE__REMOVE, "true");
    String commandString = csb.toString();

    gfsh.executeAndAssertThat(command, commandString).statusIsError()
        .containsOutput("Cannot use the --remove=true parameter with any other parameters");
  }
}
