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

import java.nio.file.Path;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;

public abstract class SnapshotDataIntegrationTest {
  static final String TEST_REGION_NAME = "testRegion";
  private static final String SNAPSHOT_FILE = "snapshot.gfd";
  private static final String SNAPSHOT_DIR = "snapshots";
  static final int DATA_POINTS = 10;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withRegion(RegionShortcut.PARTITION, TEST_REGION_NAME).withEmbeddedLocator();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private Region<String, String> region;
  private Path snapshotFile;
  private Path snapshotDir;

  @Before
  public void setup() throws Exception {
    gfsh.connectAndVerify(server.getEmbeddedLocatorPort(),
        GfshShellConnectionRule.PortType.locator);
    region = server.getCache().getRegion(TEST_REGION_NAME);
    loadRegion("value");
    Path basePath = tempDir.getRoot().toPath();
    snapshotFile = basePath.resolve(SNAPSHOT_FILE);
    snapshotDir = basePath.resolve(SNAPSHOT_DIR);
  }

  Path getSnapshotFile() {
    return snapshotFile;
  }

  Path getSnapshotDir() {
    return snapshotDir;
  }

  Region<String, String> getRegion() {
    return region;
  }

  void loadRegion(String value) {
    IntStream.range(0, DATA_POINTS).forEach(i -> region.put("key" + i, value));
  }

  CommandStringBuilder buildBaseExportCommand() {
    return new CommandStringBuilder(CliStrings.EXPORT_DATA)
        .addOption(CliStrings.MEMBER, server.getName())
        .addOption(CliStrings.EXPORT_DATA__REGION, TEST_REGION_NAME);
  }
}
