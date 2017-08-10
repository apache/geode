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

package org.apache.geode.management.internal.configuration;

import static org.apache.geode.cache.DataPolicy.PARTITION;
import static org.apache.geode.cache.DataPolicy.REPLICATE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class ImportClusterConfigDistributedTest {

  private static final String EXPORTED_CLUSTER_CONFIG_ZIP_NAME = "clusterConfiguration.zip";

  private File exportedClusterConfig;
  private MemberVM locator, server;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Before
  public void exportClusterConfig() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server = lsRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.executeAndVerifyCommand("create region --name=replicateRegion --type=REPLICATE");
    gfsh.executeAndVerifyCommand("create region --name=partitionRegion --type=PARTITION");


    server.invoke(ImportClusterConfigDistributedTest::validateServerIsUsingClusterConfig);

    // do not create the file yet
    this.exportedClusterConfig = new File(tempFolder.getRoot(), EXPORTED_CLUSTER_CONFIG_ZIP_NAME);

    gfsh.executeAndVerifyCommand(
        "export cluster-configuration --zip-file-name=" + exportedClusterConfig.getCanonicalPath());

    lsRule.stopMember(0);

    lsRule.stopMember(1);

    assertThat(this.exportedClusterConfig).exists();
    assertThat(this.exportedClusterConfig.length()).isGreaterThan(100);
  }


  private static void validateServerIsUsingClusterConfig() {
    Cache cache = CacheFactory.getAnyInstance();
    assertThat(cache).isNotNull();

    Region replicateRegion = cache.getRegion("replicateRegion");
    assertThat(replicateRegion).isNotNull();
    assertThat(replicateRegion.getAttributes().getDataPolicy()).isEqualTo(REPLICATE);

    Region partitionRegion = cache.getRegion("partitionRegion");
    assertThat(partitionRegion).isNotNull();

    assertThat(partitionRegion.getAttributes().getDataPolicy()).isEqualTo(PARTITION);
  }


  /**
   * Start locator, import previously exported cluster-configuration and then start server.
   */
  @Test
  public void startLocatorAndImportOldClusterConfigZip() throws Exception {
    locator = lsRule.startLocatorVM(0);

    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand("import cluster-configuration --zip-file-name="
        + this.exportedClusterConfig.getCanonicalPath());

    server = lsRule.startServerVM(1, locator.getPort());

    server.invoke(ImportClusterConfigDistributedTest::validateServerIsUsingClusterConfig);
  }

}
