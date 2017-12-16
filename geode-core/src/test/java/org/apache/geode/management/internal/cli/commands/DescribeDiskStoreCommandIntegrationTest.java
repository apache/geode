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

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(IntegrationTest.class)
public class DescribeDiskStoreCommandIntegrationTest {
  private static final String REGION_NAME = "test-region";
  private static final String MEMBER_NAME = "testServer";
  private static final String DISK_STORE_NAME = "testDiskStore";

  private static final List<String> expectedData = Arrays.asList("Disk Store ID", "Disk Store Name",
      "Member ID", "Member Name", "Allow Force Compaction", "Auto Compaction",
      "Compaction Threshold", "Max Oplog Size", "Queue Size", "Time Interval", "Write Buffer Size",
      "Disk Usage Warning Percentage", "Disk Usage Critical Percentage ",
      "PDX Serialization Meta-Data Stored", "Disk Directory", "Size");

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withRegion(RegionShortcut.REPLICATE, REGION_NAME)
          .withName(MEMBER_NAME).withJMXManager().withAutoStart();

  @BeforeClass
  public static void beforeClass() throws Exception {
    server.getCache().createDiskStoreFactory().create(DISK_STORE_NAME);
    gfsh.connectAndVerify(server.getJmxPort(), PortType.jmxManager);

  }

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule().withTimeout(1);

  @Test
  public void commandFailsWithoutOptions() throws Exception {
    String cmd = "describe disk-store";
    gfsh.executeAndAssertThat(cmd).statusIsError().containsOutput("You should specify option (",
        "--name", "--member", ") for this command");

  }

  @Test
  public void commandFailsWithOnlyMember() throws Exception {
    String cmd = "describe disk-store --member=" + MEMBER_NAME;
    gfsh.executeAndAssertThat(cmd).statusIsError().containsOutput("You should specify option (",
        "--name", ") for this command");
  }

  @Test
  public void commandFailsWithOnlyName() throws Exception {
    String cmd = "describe disk-store --name=" + DISK_STORE_NAME;
    gfsh.executeAndAssertThat(cmd).statusIsError().containsOutput("You should specify option (",
        "--member", ") for this command");
  }

  @Test
  public void commandFailsWithBadMember() throws Exception {
    String cmd = "describe disk-store --member=invalid-member-name --name=" + DISK_STORE_NAME;
    gfsh.executeAndAssertThat(cmd).statusIsError().containsOutput("Member",
        "could not be found.  Please verify the member name or ID and try again.");
  }

  @Test
  public void commandFailsWithBadName() throws Exception {
    String cmd = "describe disk-store --name=invalid-diskstore-name --member=" + MEMBER_NAME;
    gfsh.executeAndAssertThat(cmd).statusIsError().containsOutput("A disk store with name",
        "was not found on member");
  }

  @Test
  public void commandSucceedsWithNameAndMember() throws Exception {
    String cmd = "describe disk-store --name=" + DISK_STORE_NAME + " --member=" + MEMBER_NAME;
    gfsh.executeAndAssertThat(cmd).statusIsSuccess()
        .containsOutput(expectedData.toArray(new String[0]));
  }
}
