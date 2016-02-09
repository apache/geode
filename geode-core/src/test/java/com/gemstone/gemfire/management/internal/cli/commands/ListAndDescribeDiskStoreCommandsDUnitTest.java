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

import java.io.Serializable;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * The ListAndDescribeDiskStoreCommandsDUnitTest class is a test suite of functional tests cases testing the proper
 * functioning of the 'list disk-store' and 'describe disk-store' commands. </p>
 *
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase
 * @see com.gemstone.gemfire.management.internal.cli.commands.DiskStoreCommands
 * @since 7.0
 */
public class ListAndDescribeDiskStoreCommandsDUnitTest extends CliCommandTestBase {

  protected static String toString(final Result result) {
    assert result != null : "The Result object from the command execution cannot be null!";

    final StringBuilder buffer = new StringBuilder(System.getProperty("line.separator"));

    while (result.hasNextLine()) {
      buffer.append(result.nextLine());
      buffer.append(System.getProperty("line.separator"));
    }

    return buffer.toString().trim();
  }

  public ListAndDescribeDiskStoreCommandsDUnitTest(final String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    createDefaultSetup(null);
    setupGemFire();
  }

  protected Peer createPeer(final Properties distributedSystemConfiguration, final VM vm) {
    return new Peer(distributedSystemConfiguration, vm);
  }

  protected void setupGemFire() throws Exception {
    final Host host = Host.getHost(0);

    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);

    final Peer peer1 = createPeer(createDistributedSystemProperties("consumerServer"), vm1);
    final Peer peer2 = createPeer(createDistributedSystemProperties("producerServer"), vm2);

    createPersistentRegion(peer1, "consumers", "consumerData");
    createPersistentRegion(peer1, "observers", "observerData");
    createPersistentRegion(peer2, "producer", "producerData");
    createPersistentRegion(peer2, "producer-factory", "producerData");
  }

  protected Properties createDistributedSystemProperties(final String gemfireName) {
    final Properties distributedSystemProperties = new Properties();

    distributedSystemProperties.setProperty(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
    distributedSystemProperties.setProperty(DistributionConfig.NAME_NAME, gemfireName);

    return distributedSystemProperties;
  }

  protected void createPersistentRegion(final Peer peer, final String regionName, final String diskStoreName) {
    peer.run(new SerializableRunnable("Creating Persistent Region for Member " + peer.getName()) {
      @Override
      public void run() {
        getSystem(peer.getDistributedSystemConfiguration());

        final Cache cache = getCache();

        DiskStore diskStore = cache.findDiskStore(diskStoreName);

        if (diskStore == null) {
          final DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
          diskStoreFactory.setDiskDirs(getDiskDirs());
          diskStore = diskStoreFactory.create(diskStoreName);
        }

        final RegionFactory regionFactory = cache.createRegionFactory();

        regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        regionFactory.setDiskStoreName(diskStore.getName());
        regionFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
        regionFactory.create(regionName);
      }
    });
  }

  public void testListDiskStore() throws Exception {
    final Result result = executeCommand(CliStrings.LIST_DISK_STORE);

    assertNotNull(result);
    LogWriterUtils.getLogWriter().info(toString(result));
    assertEquals(Result.Status.OK, result.getStatus());
  }

  public void testDescribeDiskStore() throws Exception {
    final Result result = executeCommand(
        CliStrings.DESCRIBE_DISK_STORE + " --member=producerServer --name=producerData");

    assertNotNull(result);
    LogWriterUtils.getLogWriter().info(toString(result));
    assertEquals(Result.Status.OK, result.getStatus());
  }

  public void testDescribeDiskStoreWithInvalidMemberName() throws Exception {
    final Result commandResult = executeCommand(
        CliStrings.DESCRIBE_DISK_STORE + " --member=badMemberName --name=producerData");

    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, "badMemberName"),
        toString(commandResult));
  }

  public void testDescribeDiskStoreWithInvalidDiskStoreName() {
    final Result commandResult = executeCommand(
        CliStrings.DESCRIBE_DISK_STORE + " --member=producerServer --name=badDiskStoreName");

    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals("A disk store with name (badDiskStoreName) was not found on member (producerServer).",
        toString(commandResult));
  }

  protected static class Peer implements Serializable {

    private final Properties distributedSystemConfiguration;
    private final VM vm;

    protected Peer(final Properties distributedSystemConfiguration, final VM vm) {
      assert distributedSystemConfiguration != null : "The GemFire distributed system configuration properties cannot be null!";
      this.distributedSystemConfiguration = distributedSystemConfiguration;
      this.vm = vm;
    }

    public Properties getDistributedSystemConfiguration() {
      return distributedSystemConfiguration;
    }

    public String getName() {
      return getDistributedSystemConfiguration().getProperty(DistributionConfig.NAME_NAME);
    }

    public VM getVm() {
      return vm;
    }

    public void run(final SerializableRunnableIF runnable) {
      if (getVm() == null) {
        runnable.run();
      } else {
        getVm().invoke(runnable);
      }
    }
  }
}
