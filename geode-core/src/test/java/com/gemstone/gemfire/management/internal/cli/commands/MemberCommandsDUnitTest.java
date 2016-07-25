/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.remote.CommandProcessor;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static com.gemstone.gemfire.test.dunit.Assert.assertEquals;
import static com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter;
import static com.gemstone.gemfire.test.dunit.NetworkUtils.getServerHostName;

@Category(DistributedTest.class)
public class MemberCommandsDUnitTest extends JUnit4CacheTestCase {

  private static final long serialVersionUID = 1L;

  private static final Map<String, String> EMPTY_ENV = Collections.emptyMap();
  private static final String REGION1 = "region1";
  private static final String REGION2 = "region2";
  private static final String REGION3 = "region3";
  private static final String SUBREGION1A = "subregion1A";
  private static final String SUBREGION1B = "subregion1B";
  private static final String SUBREGION1C = "subregion1C";
  private static final String PR1 = "PartitionedRegion1";
  private static final String PR2 = "ParitionedRegion2";

  @Override
  public final void postSetUp() throws Exception {
    // This test does not require an actual Gfsh connection to work, however when run as part of a suite, prior tests
    // may mess up the environment causing this test to fail. Setting this prevents false failures.
    CliUtil.isGfshVM = false;
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectFromDS();
    CliUtil.isGfshVM = true;
  }

  private Properties createProperties(String name, String groups) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(NAME, name);
    props.setProperty(GROUPS, groups);
    return props;
  }

  private void createRegionsWithSubRegions() {
    final Cache cache = getCache();

    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    dataRegionFactory.setConcurrencyLevel(3);
    Region<String, Integer> region1 = dataRegionFactory.create(REGION1);
    region1.createSubregion(SUBREGION1C, region1.getAttributes());
    Region<String, Integer> subregion2 = region1.createSubregion(SUBREGION1A, region1.getAttributes());

    subregion2.createSubregion(SUBREGION1B, subregion2.getAttributes());
    dataRegionFactory.create(REGION2);
    dataRegionFactory.create(REGION3);
  }

  private void createPartitionedRegion1() {
    final Cache cache = getCache();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    dataRegionFactory.create(PR1);
  }

  private void createPartitionedRegion(String regionName) {
    final Cache cache = getCache();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    dataRegionFactory.setConcurrencyLevel(4);
    EvictionAttributes ea = EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
    dataRegionFactory.setEvictionAttributes(ea);
    dataRegionFactory.setEnableAsyncConflation(true);

    FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition("Par1", true);
    PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(100).setRecoveryDelay(
        2).setTotalMaxMemory(200).setRedundantCopies(1).addFixedPartitionAttributes(fpa).create();
    dataRegionFactory.setPartitionAttributes(pa);

    dataRegionFactory.create(regionName);
  }


  private void createLocalRegion() {
    final Cache cache = getCache();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.LOCAL);
    dataRegionFactory.create("LocalRegion");
  }

  private void setupSystem() throws IOException {
    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    final VM[] servers = {host.getVM(0), host.getVM(1)};

    final Properties propsMe = createProperties("me", "G1");
    final Properties propsServer1 = createProperties("Server1", "G1");
    final Properties propsServer2 = createProperties("Server2", "G2");


    getSystem(propsMe);
    final Cache cache = getCache();
    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE_PROXY);
    dataRegionFactory.setConcurrencyLevel(5);
    Region<String, Integer> region1 = dataRegionFactory.create(REGION1);


    servers[1].invoke(new SerializableRunnable("Create cache for server1") {
      public void run() {
        getSystem(propsServer2);
        createRegionsWithSubRegions();
        createLocalRegion();
        createPartitionedRegion("ParReg1");
      }
    });
    servers[0].invoke(new SerializableRunnable("Create cache for server0") {
      public void run() {
        getSystem(propsServer1);
        createRegionsWithSubRegions();
        createLocalRegion();
      }
    });
  }

  private Properties createProperties(Host host, int locatorPort) {
    Properties props = new Properties();

    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, getServerHostName(host) + "[" + locatorPort + "]");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.put(ENABLE_NETWORK_PARTITION_DETECTION, "true");

    return props;
  }

  /**
   * Creates the cache.
   */
  private void createCache(Properties props) {
    getSystem(props);
    final Cache cache = getCache();
  }

  /**
   * Tests the execution of "list member" command which should list out all the members in the DS
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testListMemberAll() throws IOException, ClassNotFoundException {
    setupSystem();
    CommandProcessor commandProcessor = new CommandProcessor();
    Result result = commandProcessor.createCommandStatement(CliStrings.LIST_MEMBER, EMPTY_ENV).process();
    getLogWriter().info("#SB" + getResultAsString(result));
    assertEquals(true, result.getStatus().equals(Status.OK));
  }

  /**
   * Tests the execution of "list member" command, when no cache is created
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testListMemberWithNoCache() throws IOException, ClassNotFoundException {
    final Host host = Host.getHost(0);
    final VM[] servers = {host.getVM(0), host.getVM(1)};
    final int openPorts[] = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    final File logFile = new File(getUniqueName() + "-locator" + openPorts[0] + ".log");

    Locator locator = Locator.startLocator(openPorts[0], logFile);
    try {

      final Properties props = createProperties(host, openPorts[0]);
      CommandProcessor commandProcessor = new CommandProcessor();
      Result result = commandProcessor.createCommandStatement(CliStrings.LIST_MEMBER, EMPTY_ENV).process();

      getLogWriter().info("#SB" + getResultAsString(result));
      assertEquals(true, result.getStatus().equals(Status.ERROR));
    } finally {
      locator.stop(); // fix for bug 46562
    }
  }

  /**
   * Tests list member --group=G1
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testListMemberWithGroups() throws IOException, ClassNotFoundException {
    setupSystem();
    CommandProcessor commandProcessor = new CommandProcessor();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_MEMBER);
    csb.addOption(CliStrings.LIST_MEMBER__GROUP, "G1");
    Result result = commandProcessor.createCommandStatement(csb.toString(), EMPTY_ENV).process();
    getLogWriter().info("#SB" + getResultAsString(result));
    assertEquals(true, result.getStatus().equals(Status.OK));
  }

  /**
   * Tests the "describe member" command for all the members in the DS
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testDescribeMember() throws IOException, ClassNotFoundException {
    setupSystem();
    CommandProcessor commandProcessor = new CommandProcessor();
    GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
    Set<DistributedMember> members = cache.getDistributedSystem().getAllOtherMembers();

    Iterator<DistributedMember> iters = members.iterator();

    while (iters.hasNext()) {
      DistributedMember member = iters.next();
      Result result = commandProcessor.createCommandStatement("describe member --name=" + member.getId(),
          EMPTY_ENV).process();
      assertEquals(true, result.getStatus().equals(Status.OK));
      getLogWriter().info("#SB" + getResultAsString(result));
      //assertIndexDetailsEquals(true, result.getStatus().equals(Status.OK));
    }
  }

  private String getResultAsString(Result result) {
    StringBuilder sb = new StringBuilder();
    while (result.hasNextLine()) {
      sb.append(result.nextLine());
    }
    return sb.toString();
  }
}
