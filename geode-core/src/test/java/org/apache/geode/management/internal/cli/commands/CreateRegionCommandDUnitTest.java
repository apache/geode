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

import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class CreateRegionCommandDUnitTest extends CliCommandTestBase {
  private final List<String> filesToBeDeleted = new CopyOnWriteArrayList<>();

  /**
   * Asserts that the "compressor" option for the "create region" command succeeds for a recognized
   * compressor.
   */
  @Test
  public void testCreateRegionWithGoodCompressor() {
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);

    // Create a cache in vm 1
    vm.invoke(() -> assertNotNull(getCache()));

    // Run create region command with compression
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "compressedRegion");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__COMPRESSOR,
        RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER);
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure our region exists with compression enabled
    vm.invoke(() -> {
      Region region = getCache().getRegion("compressedRegion");
      assertNotNull(region);
      assertTrue(
          SnappyCompressor.getDefaultInstance().equals(region.getAttributes().getCompressor()));
    });

    // cleanup
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    commandStringBuilder.addOption(CliStrings.DESTROY_REGION__REGION, "compressedRegion");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }

  /**
   * Asserts that the "compressor" option for the "create region" command fails for an unrecognized
   * compressorc.
   */
  @Test
  public void testCreateRegionWithBadCompressor() {
    setUpJmxManagerOnVm0ThenConnect(null);

    VM vm = Host.getHost(0).getVM(1);

    // Create a cache in vm 1
    vm.invoke(() -> assertNotNull(getCache()));

    // Create a region with an unrecognized compressor
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "compressedRegion");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__COMPRESSOR, "BAD_COMPRESSOR");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    // Assert that our region was not created
    vm.invoke(() -> {
      Region region = getCache().getRegion("compressedRegion");
      assertNull(region);
    });
  }

  /**
   * Asserts that a missing "compressor" option for the "create region" command results in a region
   * with no compression.
   */
  @Test
  public void testCreateRegionWithNoCompressor() {
    setUpJmxManagerOnVm0ThenConnect(null);

    VM vm = Host.getHost(0).getVM(1);

    // Create a cache in vm 1
    vm.invoke(() -> assertNotNull(getCache()));

    // Create a region with no compression
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "testRegion");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Assert that our newly created region has no compression
    vm.invoke(() -> {
      Region region = getCache().getRegion("testRegion");
      assertNotNull(region);
      assertNull(region.getAttributes().getCompressor());
    });

    // Cleanup
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    commandStringBuilder.addOption(CliStrings.DESTROY_REGION__REGION, "testRegion");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }

  @Category(FlakyTest.class) // GEODE-973: random ports, BindException,
  // java.rmi.server.ExportException: Port already in use
  @Test
  public void testCreateRegion46391() throws IOException {
    setUpJmxManagerOnVm0ThenConnect(null); // GEODE-973: getRandomAvailablePort
    String region46391 = "region46391";
    String command = CliStrings.CREATE_REGION + " --" + CliStrings.CREATE_REGION__REGION + "="
        + region46391 + " --" + CliStrings.CREATE_REGION__REGIONSHORTCUT + "=REPLICATE";

    getLogWriter().info("testCreateRegion46391 create region command=" + command);

    CommandResult cmdResult = executeCommand(command);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    command = CliStrings.PUT + " --" + CliStrings.PUT__KEY + "=k1" + " --" + CliStrings.PUT__VALUE
        + "=k1" + " --" + CliStrings.PUT__REGIONNAME + "=" + region46391;

    getLogWriter().info("testCreateRegion46391 put command=" + command);

    CommandResult cmdResult2 = executeCommand(command);
    assertEquals(Result.Status.OK, cmdResult2.getStatus());

    getLogWriter().info("testCreateRegion46391  cmdResult2=" + commandResultToString(cmdResult2));
    String str1 = "Result      : true";
    String str2 = "Key         : k1";
    String str3 = "Key Class   : java.lang.String";
    String str4 = "Value Class : java.lang.String";
    String str5 = "Old Value   : <NULL>";

    assertTrue(
        commandResultToString(cmdResult).contains("Region \"/" + region46391 + "\" created on"));

    assertTrue(commandResultToString(cmdResult2).contains(str1));
    assertTrue(commandResultToString(cmdResult2).contains(str2));
    assertTrue(commandResultToString(cmdResult2).contains(str3));
    assertTrue(commandResultToString(cmdResult2).contains(str4));
    assertTrue(commandResultToString(cmdResult2).contains(str5));
  }

  /**
   * Test Description 1. Deploy a JAR with Custom Partition Resolver 2. Create Region with Partition
   * Resolver 3. Region should get created with no Errors 4. Verify Region Partition Attributes for
   * Partition Resolver
   */
  @Test
  public void testCreateRegionWithPartitionResolver() throws IOException {
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    vm.invoke(() -> assertNotNull(getCache()));

    ClassBuilder classBuilder = new ClassBuilder();
    // classBuilder.addToClassPath(".");
    final File prJarFile = new File(temporaryFolder.getRoot().getCanonicalPath() + File.separator,
        "myPartitionResolver.jar");
    this.filesToBeDeleted.add(prJarFile.getAbsolutePath());
    String PR_STRING = " package com.cadrdunit;"
        + " public class TestPartitionResolver implements org.apache.geode.cache.PartitionResolver { "
        + "   @Override" + "   public void close() {" + "   }" + "   @Override"
        + "   public Object getRoutingObject(org.apache.geode.cache.EntryOperation opDetails) { "
        + "    return null; " + "   }" + "   @Override" + "   public String getName() { "
        + "    return \"TestPartitionResolver\";" + "   }" + " }";
    byte[] jarBytes =
        classBuilder.createJarFromClassContent("com/cadrdunit/TestPartitionResolver", PR_STRING);
    writeJarBytesToFile(prJarFile, jarBytes);

    CommandResult cmdResult = executeCommand("deploy --jar=" + prJarFile.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());


    // Create a region with an unrecognized compressor
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "regionWithPartitionResolver");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__PARTITION_RESOLVER,
        "com.cadrdunit.TestPartitionResolver");
    CommandResult cmdResult1 = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult1.getStatus());

    // Assert that our region was not created
    vm.invoke(() -> {
      Region region = getCache().getRegion("regionWithPartitionResolver");
      assertNotNull(region);

      PartitionedRegion pr = (PartitionedRegion) region;
      PartitionAttributes partitionAttributes = pr.getPartitionAttributes();
      assertNotNull(partitionAttributes);
      PartitionResolver partitionResolver = partitionAttributes.getPartitionResolver();
      assertNotNull(partitionResolver);
      assertEquals("TestPartitionResolver", partitionResolver.getName());
    });

    vm.invoke(() -> getCache().getRegion("regionWithPartitionResolver").destroyRegion());
  }

  @Test
  public void testCreateRegionWithInvalidPartitionResolver() {
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    vm.invoke(() -> assertNotNull(getCache()));

    // Create a region with an unrecognized compressor
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION,
        "testCreateRegionWithInvalidPartitionResolver");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__PARTITION_RESOLVER, "a.b.c.d");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    // Assert that our region was not created
    vm.invoke(() -> {
      Region region = getCache().getRegion("testCreateRegionWithInvalidPartitionResolver");
      assertNull(region);
    });
  }

  /**
   * Test Description Try creating region of type REPLICATED and specify partition resolver Region
   * Creation should fail.
   */
  @Test
  public void testCreateRegionForReplicatedRegionWithParitionResolver() {
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    vm.invoke(() -> assertNotNull(getCache()));

    // Create a region with an unrecognized compressor
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION,
        "testCreateRegionForReplicatedRegionWithParitionResolver");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__PARTITION_RESOLVER, "a.b.c.d");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    // Assert that our region was not created
    vm.invoke(() -> {
      Region region =
          getCache().getRegion("testCreateRegionForReplicatedRegionWithParitionResolver");
      assertNull(region);
    });
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.flush();
    outStream.close();
  }
}
