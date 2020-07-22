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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortsForDUnitSite;
import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.version.VersionManager;

/**
 * Tests to verify that {@link PartitionedRegion#clear()} gracefully reject the operation if there
 * are older members in the cluster. The {@link PartitionedRegion#clear()} feature was introduced
 * in Geode 1.14.0, so we test against members using versions older than that.
 */
@RunWith(JUnitParamsRunner.class)
public class PartitionedRegionClearBackwardCompatibilityDUnitTest implements Serializable {
  private static final Integer BUCKETS = 13;
  private static final Integer ENTRY_COUNT = 1500;
  private static final String REGION_NAME = "PartitionedRegion";
  private static final String TEST_CASE_NAME = "[{index}] {method}(Version:{0}, RegionType:{1})";

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public DistributedRule distributedRule = new DistributedRule(3);

  @Rule
  public DistributedDiskDirRule distributedDiskDirRule = new DistributedDiskDirRule();

  private VM client;
  private VM server;
  private VM oldServer;

  static RegionShortcut[] regionTypes() {
    return new RegionShortcut[] {
        PARTITION,
        PARTITION_REDUNDANT,
        PARTITION_PERSISTENT,
        PARTITION_REDUNDANT_PERSISTENT,
    };
  }

  @SuppressWarnings("unused")
  static Object[] versionsAndRegionTypes() {
    ArrayList<Object[]> parameters = new ArrayList<>();
    RegionShortcut[] regionShortcuts = regionTypes();
    // TODO: Change the upper bound once we know which version will contain the clear feature.
    List<String> versions = VersionManager.getInstance()
        .getVersionsWithinRange(Version.GEODE_1_10_0.getName(), Version.GEODE_1_14_0.getName());

    Arrays.stream(regionShortcuts).forEach(regionShortcut -> versions
        .forEach(version -> parameters.add(new Object[] {version, regionShortcut})));

    return parameters.toArray();
  }

  private void initServer(RegionShortcut regionShortcut, int serverPort) throws IOException {
    cacheRule.createCache();

    PartitionAttributes<String, String> attributes =
        new PartitionAttributesFactory<String, String>()
            .setTotalNumBuckets(BUCKETS)
            .create();

    cacheRule.getCache()
        .<String, String>createRegionFactory(regionShortcut)
        .setPartitionAttributes(attributes)
        .create(REGION_NAME);

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(serverPort);
    cacheServer.start();
  }

  private void initClient() {
    clientCacheRule.getClientCache()
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(REGION_NAME);
  }

  private void verifyRegionEntries() {
    asList(server, oldServer).forEach(vm -> vm.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, ENTRY_COUNT)
          .forEach(i -> assertThat(region.get(String.valueOf(i))).isEqualTo("Value_" + i));
    }));
  }

  private void populateRegion() {
    client.invoke(() -> {
      Region<String, String> region = clientCacheRule.getClientCache().getRegion(REGION_NAME);
      IntStream.range(0, ENTRY_COUNT).forEach(i -> region.put(String.valueOf(i), "Value_" + i));
    });

    verifyRegionEntries();
  }

  public void parametrizedSetUp(String oldVersion, RegionShortcut regionShortcut) {
    final Host host = Host.getHost(0);
    int[] ports = getRandomAvailableTCPPortsForDUnitSite(2);

    server = getVM(0);
    client = host.getVM(oldVersion, 1);
    oldServer = host.getVM(oldVersion, 2);

    server.invoke(() -> initServer(regionShortcut, ports[0]));
    oldServer.invoke(() -> initServer(regionShortcut, ports[1]));

    client.invoke(() -> {
      final ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory
          .addPoolServer("localhost", ports[0])
          .addPoolServer("localhost", ports[1]);
      clientCacheRule.createClientCache(clientCacheFactory);
      initClient();
    });
  }

  @Test
  @TestCaseName(TEST_CASE_NAME)
  @Parameters(method = "versionsAndRegionTypes")
  public void clearInitiatedFromOldServerShouldThrowUnsupportedOperationException(String version,
      RegionShortcut regionShortcut) {
    parametrizedSetUp(version, regionShortcut);
    populateRegion();

    oldServer.invoke(() -> {
      assertThatThrownBy(() -> cacheRule.getCache().getRegion(REGION_NAME).clear())
          .isInstanceOf(UnsupportedOperationException.class);
    });

    verifyRegionEntries();
  }

  @Test
  @TestCaseName(TEST_CASE_NAME)
  @Parameters(method = "versionsAndRegionTypes")
  public void clearInitiatedFromClientShouldFailWhenThereIsAtLeastOneServerOlderThanPRClearReleaseVersion(
      String version, RegionShortcut regionShortcut) {
    parametrizedSetUp(version, regionShortcut);
    populateRegion();

    client.invoke(() -> {
      // TODO: Should fail with a new type of ClearException.
      clientCacheRule.getClientCache().getRegion(REGION_NAME).clear();
    });

    verifyRegionEntries();
  }

  @Test
  @TestCaseName(TEST_CASE_NAME)
  @Parameters(method = "versionsAndRegionTypes")
  public void clearInitiatedFromServerShouldFailWhenThereIsAtLeastOneServerOlderThanPRClearReleaseVersion(
      String version, RegionShortcut regionShortcut) {
    parametrizedSetUp(version, regionShortcut);
    populateRegion();

    server.invoke(() -> {
      // TODO: Should fail with a new type of ClearException.
      cacheRule.getCache().getRegion(REGION_NAME).clear();
    });

    verifyRegionEntries();
  }
}
