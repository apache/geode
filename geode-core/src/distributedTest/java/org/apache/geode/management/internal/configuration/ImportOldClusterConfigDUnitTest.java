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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class ImportOldClusterConfigDUnitTest {
  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private Path zipFile;
  private MemberVM locator, server;

  @Before
  public void before() throws Exception {
    // create the cc.zip that contains the 8.1 version cache.xml
    File ccDir = tempFolder.newFolder("cluster_config");
    File clusterDir = new File(ccDir, "cluster");
    clusterDir.mkdir();

    FileUtils.copyURLToFile(this.getClass().getResource("cluster8.xml"),
        new File(clusterDir, "cluster.xml"));
    zipFile = new File(tempFolder.getRoot(), "cc.zip").toPath();

    ZipUtils.zipDirectory(ccDir.toPath(), zipFile);
  }

  @Test
  public void importOldConfigThenCreateRegionCorruptsCachXml() throws Exception {
    locator = lsRule.startLocatorVM(0);

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat("import cluster-configuration --zip-file-name=" + zipFile.toString())
        .statusIsSuccess();

    server = lsRule.startServerVM(1, locator.getPort());

    server.invoke(ImportOldClusterConfigDUnitTest::regionOneExists);

    gfsh.executeAndAssertThat("create region --name=two --type=REPLICATE").statusIsSuccess();
    server.invoke(ImportOldClusterConfigDUnitTest::regionOneExists);
    server.invoke(ImportOldClusterConfigDUnitTest::regionTwoExists);

    lsRule.stopMember(1);

    server = lsRule.startServerVM(1, locator.getPort());
    server.invoke(ImportOldClusterConfigDUnitTest::regionOneExists);
    server.invoke(ImportOldClusterConfigDUnitTest::regionTwoExists);
  }

  private static void regionOneExists() {
    regionExists("one");
  }

  private static void regionTwoExists() {
    regionExists("two");
  }

  private static void regionExists(String regionName) {
    Cache cache = ClusterStartupRule.getCache();
    assertThat(cache).isNotNull();
    Region<Object, Object> one = cache.getRegion(regionName);
    assertThat(one).isNotNull();
  }
}
