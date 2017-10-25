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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.json.JSONArray;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(DistributedTest.class)
public class CreateRegionCommandDUnitTest {

  MemberVM locator;
  MemberVM server;

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server = lsRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testCreateRegionWithGoodCompressor() throws Exception {
    String regionName = testName.getMethodName();
    gfsh.executeAndVerifyCommand("create region --name=" + regionName
        + " --type=REPLICATE --compressor=" + RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER);

    server.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      Region region = cache.getRegion(regionName);
      assertThat(region).isNotNull();
      assertThat(region.getAttributes().getCompressor())
          .isEqualTo(SnappyCompressor.getDefaultInstance());
    });
  }

  @Test
  public void testCreateRegionWithBadCompressor() throws Exception {
    String regionName = testName.getMethodName();
    gfsh.executeAndVerifyCommandError(
        "create region --name=" + regionName + " --type=REPLICATE --compressor=BAD_COMPRESSOR");

    server.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      Region region = cache.getRegion(regionName);
      assertThat(region).isNull();
    });
  }

  @Test
  public void testCreateRegionWithNoCompressor() throws Exception {
    String regionName = testName.getMethodName();
    gfsh.executeAndVerifyCommand("create region --name=" + regionName + " --type=REPLICATE");

    server.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      Region region = cache.getRegion(regionName);
      assertThat(region).isNotNull();
      assertThat(region.getAttributes().getCompressor()).isNull();
    });
  }

  @Test
  public void testCreateRegionWithPartitionResolver() throws Exception {
    String regionName = testName.getMethodName();
    String PR_STRING = "package io.pivotal; "
        + "public class TestPartitionResolver implements org.apache.geode.cache.PartitionResolver { "
        + "   @Override" + "   public void close() {" + "   }" + "   @Override"
        + "   public Object getRoutingObject(org.apache.geode.cache.EntryOperation opDetails) { "
        + "    return null; " + "   }" + "   @Override" + "   public String getName() { "
        + "    return \"TestPartitionResolver\";" + "   }" + " }";
    final File prJarFile = new File(tmpDir.getRoot(), "myPartitionResolver.jar");
    new JarBuilder().buildJar(prJarFile, PR_STRING);

    gfsh.executeAndVerifyCommand("deploy --jar=" + prJarFile.getAbsolutePath());

    gfsh.executeAndVerifyCommand("create region --name=" + regionName
        + " --type=PARTITION --partition-resolver=io.pivotal.TestPartitionResolver");

    server.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
      PartitionResolver resolver = region.getPartitionAttributes().getPartitionResolver();
      assertThat(resolver).isNotNull();
      assertThat(resolver.getName()).isEqualTo("TestPartitionResolver");
    });
  }

  @Test
  public void testCreateRegionWithInvalidPartitionResolver() throws Exception {
    gfsh.executeAndVerifyCommandError("create region --name=" + testName.getMethodName()
        + " --type=PARTITION --partition-resolver=InvalidPartitionResolver");
  }

  @Test
  public void testCreateRegionForReplicatedRegionWithPartitionResolver() {
    String regionName = testName.getMethodName();
    CommandResult result = gfsh.executeAndVerifyCommandError("create region --name=" + regionName
        + " --type=REPLICATE --partition-resolver=InvalidPartitionResolver");

    assertThat(((String) ((JSONArray) result.getContent().get("message")).get(0)))
        .contains("\"/" + regionName + "\" is not a Partitioned Region");
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    try (OutputStream os = new FileOutputStream(jarFile)) {
      os.write(jarBytes);
    }
  }
}
