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
package org.apache.geode.internal.cache.rollingupgrade;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MembershipView;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.VersionManager;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public abstract class RollingUpgradeDistributedTest implements Serializable {

  @Parameters(name = "from_v{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    }
    System.out.println("running against these versions: " + result);
    return result;
  }

  @Parameter
  public String oldVersion;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  void doTestRollAll(String regionType, String objectType, String startingVersion)
      throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(0, startingVersion);
    int locatorPort = locator.getPort();
    MemberVM server1 = clusterStartupRule.startServerVM(1, startingVersion,
        s -> s.withConnectionToLocator(locatorPort));
    MemberVM server2 = clusterStartupRule.startServerVM(2, startingVersion,
        s -> s.withConnectionToLocator(locatorPort));



    String regionName = "aRegion";
    RegionShortcut shortcutName = null;
    switch (regionType) {
      case "replicate":
        shortcutName = RegionShortcut.REPLICATE;
        break;
      case "partitionedRedundant":
        shortcutName = RegionShortcut.PARTITION_REDUNDANT;
        break;
      case "persistentReplicate":
        shortcutName = RegionShortcut.REPLICATE_PERSISTENT;
        break;
      default:
        fail("Invalid region type");
        break;
    }

    // Locators before 1.4 handled configuration asynchronously.
    // We must wait for configuration to be ready, or confirm that it is disabled.
    locator.invoke(
        () -> await()
            .untilAsserted(() -> assertTrue(
                !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                    || InternalLocator.getLocator().isSharedConfigurationRunning())));

    // create region
    RegionShortcut finalShortcutName = shortcutName;
    server1.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(finalShortcutName)
          .create(regionName);
    });
    server2.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(finalShortcutName).create(regionName);
    });

    putAndVerify(objectType, server1, regionName, 0, 10, server2);
    locator.stop(false);
    locator = clusterStartupRule.startLocatorVM(0, l -> l.withPort(locatorPort));
    server1.stop(false);
    server1 = clusterStartupRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
    server1.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(finalShortcutName)
          .create(regionName);
      ClusterStartupRule.getCache().getResourceManager().createRestoreRedundancyOperation().start()
          .get();
      ClusterStartupRule.getCache().getResourceManager().createRebalanceFactory().start()
          .getResults();
    });
    verifyValues(objectType, regionName, 0, 10, server1);
    putAndVerify(objectType, server1, regionName, 5, 15, server2);
    putAndVerify(objectType, server2, regionName, 10, 20, server1);


    server2.stop(false);
    server2 = clusterStartupRule.startServerVM(2, s -> s.withConnectionToLocator(locatorPort));
    server2.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(finalShortcutName)
          .create(regionName);
      ClusterStartupRule.getCache().getResourceManager().createRestoreRedundancyOperation().start()
          .get();
      ClusterStartupRule.getCache().getResourceManager().createRebalanceFactory().start()
          .getResults();
    });
    verifyValues(objectType, regionName, 5, 15, server2);
    putAndVerify(objectType, server2, regionName, 15, 25, server1);

    final short versionOrdinalAfterUpgrade = KnownVersion.getCurrentVersion().ordinal();
    locator.invoke(() -> {
      final DistributionManager distributionManager =
          ClusterStartupRule.getCache().getDistributionManager();
      final MembershipView<InternalDistributedMember> view =
          distributionManager.getDistribution().getView();
      for (final InternalDistributedMember member : view.getMembers()) {
        assertThat(member.getVersionOrdinal()).isEqualTo(versionOrdinalAfterUpgrade);
      }
    });
  }

  // ******** TEST HELPER METHODS ********/
  private void putAndVerify(String objectType, MemberVM putter, String regionName, int start,
      int end,
      MemberVM... checkVMs) throws Exception {
    switch (objectType) {
      case "strings":
        putStringsAndVerify(putter, regionName, start, end, checkVMs);
        break;
      case "serializable":
        putSerializableAndVerify(putter, regionName, start, end, checkVMs);
        break;
      case "dataserializable":
        putDataSerializableAndVerify(putter, regionName, start, end, checkVMs);
        break;
      default:
        fail("Not a valid test object type");
    }
  }

  private void putStringsAndVerify(MemberVM putter, final String regionName, final int start,
      final int end, MemberVM... vms) {

    for (int i = start; i < end; i++) {
      String key = "" + i;
      String value = "VALUE(" + i + ")";
      putter.invoke(() -> {
        Region<String, String> region = ClusterStartupRule.getCache().getRegion(regionName);
        region.put(key, value);
      });
    }

    // verify present in others
    for (MemberVM vm : vms) {
      vm.invoke(() -> {
        for (int i = start; i < end; i++) {
          Region<String, String> region = ClusterStartupRule.getCache().getRegion(regionName);
          Object value = region.get("" + i);
          if (value == null) {
            fail("Region value does not exist for key" + i);
          } else if (!value.equals("VALUE(" + i + ")")) {
            fail(" Region value " + value + " does not match with expected value :" + "VALUE(" + i
                + ")");
          }
        }
      });
    }
  }

  private void putSerializableAndVerify(MemberVM putter, String regionName, int start, int end,
      MemberVM... vms) {
    for (int i = start; i < end; i++) {
      String key = "" + i;
      putter.invoke(() -> {
        Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
        region.put(key, new Properties());
      });
    }

    // verify present in others
    verifyEntryExistsInRegion(vms, regionName, start, end);
  }

  private void verifyEntryExistsInRegion(MemberVM[] vms, String regionName, int start, int end) {
    for (MemberVM vm : vms) {
      vm.invoke(() -> {
        Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
        for (int i = start; i < end; i++) {
          assertThat(region.containsKey("" + i)).withFailMessage(
              "Key:" + i + " does not exist in the region").isTrue();
        }
      });
    }
  }

  private void putDataSerializableAndVerify(MemberVM putter, String regionName, int start, int end,
      MemberVM... vms) throws Exception {
    for (int i = start; i < end; i++) {
      Class<?> aClass = Thread.currentThread().getContextClassLoader()
          .loadClass("org.apache.geode.cache.ExpirationAttributes");
      Constructor<?> constructor = aClass.getConstructor(int.class);
      Object testDataSerializable = constructor.newInstance(i);
      String key = "" + i;
      putter.invoke(() -> {
        Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
        region.put(key, testDataSerializable);
      });
    }

    // verify present in others
    verifyEntryExistsInRegion(vms, regionName, start, end);
  }

  private void verifyValues(String objectType, String regionName, int start, int end,
      MemberVM... vms) {
    if (objectType.equals("strings")) {
      for (MemberVM vm : vms) {
        vm.invoke(() -> {
          for (int i = start; i < end; i++) {
            Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
            Object value = region.get("" + i);
            if (value == null) {
              fail("Region value does not exist for key" + i);
            } else if (!value.equals("VALUE(" + i + ")")) {
              fail(" Region value " + value + " does not match with expected value :" + "VALUE(" + i
                  + ")");
            }
          }
        });
      }
    } else {
      verifyEntryExistsInRegion(vms, regionName, start, end);
    }
  }

}
