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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Config;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.admin.AlertListener;
import org.apache.geode.internal.admin.ApplicationVM;
import org.apache.geode.internal.admin.DLockInfo;
import org.apache.geode.internal.admin.EntryValueNode;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.GfManagerAgent;
import org.apache.geode.internal.admin.GfManagerAgentConfig;
import org.apache.geode.internal.admin.GfManagerAgentFactory;
import org.apache.geode.internal.admin.StatResource;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * This class tests the functionality of the {@linkplain org.apache.geode.internal.admin internal
 * admin} API.
 */

public class ClusterDistributionManagerForAdminDUnitTest extends CacheTestCase
    implements AlertListener {

  private static Logger logger = LogService.getLogger();

  private transient GfManagerAgent agent;

  @Before
  public void setUp() throws Exception {
    IgnoredException.addIgnoredException("Error occurred while reading system log");

    ClusterDistributionManager.setIsDedicatedAdminVM(true);

    populateCache();

    boolean created = !isConnectedToDS();
    InternalDistributedSystem ds = getSystem();
    RemoteTransportConfig transport =
        new RemoteTransportConfig(ds.getConfig(), ClusterDistributionManager.ADMIN_ONLY_DM_TYPE);
    if (created) {
      disconnectFromDS();
    }

    // create a GfManagerAgent in the master vm.
    this.agent = GfManagerAgentFactory.getManagerAgent(
        new GfManagerAgentConfig(null, transport, getLogWriter(), Alert.SEVERE, this, null));

    await().untilAsserted(() -> assertThat(agent.isConnected()).isTrue());
  }

  @After
  public void preTearDownCacheTestCase() throws Exception {
    try {
      if (this.agent != null) {
        this.agent.disconnect();
      }
      disconnectFromDS();
    } finally {
      ClusterDistributionManager.setIsDedicatedAdminVM(false);
    }
  }

  @Test
  public void testGetDistributionVMType() {
    DistributionManager dm = this.agent.getDM();
    assertThat(dm.getId().getVmKind()).isEqualTo(ClusterDistributionManager.ADMIN_ONLY_DM_TYPE);
  }

  @Test
  public void testAgent() {
    assertThat(agent.listPeers()).hasSize(0);
    assertThat(agent.isConnected()).isTrue();
    agent.disconnect();
    assertThat(agent.isConnected()).isFalse();
  }

  @Test
  public void testApplications() throws Exception {
    await()
        .untilAsserted(() -> assertThat(agent.listApplications().length).isGreaterThanOrEqualTo(4));

    ApplicationVM[] applications = agent.listApplications();
    for (int whichApplication = 0; whichApplication < applications.length; whichApplication++) {

      InetAddress host = applications[whichApplication].getHost();
      String appHostName = host.getHostName();

      assertThat(host).isEqualTo(InetAddress.getByName(appHostName));

      StatResource[] stats = applications[whichApplication].getStats(null);
      assertThat(stats.length).isGreaterThan(0);

      Config config = applications[whichApplication].getConfig();
      String[] attributeNames = config.getAttributeNames();
      boolean foundStatisticSamplingEnabled = false;
      for (String attributeName : attributeNames) {
        if (attributeName.equals(STATISTIC_SAMPLING_ENABLED)) {
          foundStatisticSamplingEnabled = true;
          assertThat(config.getAttribute(attributeName)).isEqualTo("true");
          break;
        }
      }
      assertThat(foundStatisticSamplingEnabled).isTrue();

      String[] logs = applications[whichApplication].getSystemLogs();
      assertThat(logs.length).isGreaterThan(0);

      VM vm = findVMForAdminObject(applications[whichApplication]);
      assertThat(vm).isNotNull();

      String lockName = "cdm_testlock" + whichApplication;
      assertThat(acquireDistributedLock(vm, lockName)).isTrue();

      DLockInfo[] locks = applications[whichApplication].getDistributedLockInfo();
      assertThat(locks.length).isGreaterThan(0);

      boolean foundLock = false;
      for (DLockInfo lock : locks) {
        if (lock.getLockName().equals(lockName)) {
          foundLock = true;
          assertThat(lock.isAcquired()).isTrue();
        }
      }
      assertThat(foundLock).isTrue();

      Region[] roots = applications[whichApplication].getRootRegions();
      assertThat(roots.length).isGreaterThan(0);

      Region root = roots[0];
      assertThat(root).isNotNull();
      assertThat(root.getName()).isEqualTo("root");
      assertThat(root.getFullPath()).isEqualTo("/root");

      RegionAttributes attributes = root.getAttributes();
      assertThat(attributes).isNotNull();
      if (attributes.getStatisticsEnabled()) {
        assertThat(root.getStatistics()).isNotNull();
      }

      Set subregions = root.subregions(false);
      assertThat(subregions).hasSize(3);
      assertThat(root.keySet()).hasSize(2);

      Region.Entry entry = root.getEntry("cacheObj1");
      assertThat(entry).isNotNull();
      if (attributes.getStatisticsEnabled()) {
        assertThat(entry.getStatistics()).isNotNull();
      }
      assertThat(entry.getValue()).isEqualTo("null");

      /// test lightweight inspection;
      entry = root.getEntry("cacheObj2");
      assertThat(entry).isNotNull();

      Object val = entry.getValue();
      assertThat(val).isInstanceOf(String.class);
      assertThat(((String) val)).contains("java.lang.StringBuffer");

      /// test physical inspection
      applications[whichApplication].setCacheInspectionMode(GemFireVM.PHYSICAL_CACHE_VALUE);
      entry = root.getEntry("cacheObj2");
      assertThat(entry).isNotNull();

      val = entry.getValue();
      assertThat(val).isInstanceOf(EntryValueNode.class);

      EntryValueNode node = (EntryValueNode) val;
      String type = node.getType();
      assertThat(type).contains("java.lang.StringBuffer");
      assertThat(node.isPrimitiveOrString()).isFalse();

      EntryValueNode[] fields = node.getChildren();
      assertThat(fields).isNotNull();

      getLogWriter().warning(
          "The tests use StringBuffers for values which might be implemented differently in jdk 1.5");

      /// test destruction in the last valid app
      int lastApplication = applications.length - 1;
      if (whichApplication == lastApplication) {
        int expectedSize = subregions.size() - 1;
        Region subRegion = (Region) subregions.iterator().next();
        Region rootRegion = root;
        subRegion.destroyRegion();

        await()
            .untilAsserted(
                () -> assertThat(rootRegion.subregions(false).size()).isEqualTo(expectedSize));
      }
    }
  }

  @Override
  public void alert(Alert alert) {
    getLogWriter().info("DEBUG: alert=" + alert);
  }

  private void populateCache() {
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setScope(Scope.DISTRIBUTED_NO_ACK);

    RegionAttributes regionAttributes = attributesFactory.create();

    for (int i = 0; i < Host.getHostCount(); i++) {
      Host host = Host.getHost(i);

      for (int j = 0; j < host.getVMCount(); j++) {
        VM vm = host.getVM(j);
        vm.invoke(() -> {
          createRegion("cdm-testSubRegion1", regionAttributes);
          createRegion("cdm-testSubRegion2", regionAttributes);
          createRegion("cdm-testSubRegion3", regionAttributes);
          remoteCreateEntry("", "cacheObj1", null);
          StringBuffer val = new StringBuffer("userDefValue1");
          remoteCreateEntry("", "cacheObj2", val);
        });
      }
    }
  }

  /**
   * Puts (or creates) a value in a region named <code>regionName</code> named
   * <code>entryName</code>.
   */
  private void remoteCreateEntry(String regionName, String entryName, Object value)
      throws CacheException {

    Region root = getRootRegion();
    Region region = root.getSubregion(regionName);
    region.create(entryName, value);

    logger.info("Put value " + value + " in entry " + entryName + " in region '"
        + region.getFullPath() + "'");
  }

  private boolean acquireDistributedLock(VM vm, String lockName) {
    return vm.invoke(() -> remoteAcquireDistLock(lockName));
  }

  private boolean remoteAcquireDistLock(String lockName) {
    String serviceName = "cdmtest_service";
    DistributedLockService service = DistributedLockService.getServiceNamed(serviceName);
    if (service == null) {
      service =
          DistributedLockService.create(serviceName, InternalDistributedSystem.getAnyInstance());
    }
    assertThat(service).isNotNull();
    return service.lock(lockName, 1000, 3000);
  }

  private VM findVMForAdminObject(GemFireVM gemFireVM) {
    for (int i = 0; i < Host.getHostCount(); i++) {
      Host host = Host.getHost(i);
      for (int j = 0; j < host.getVMCount(); j++) {
        VM vm = host.getVM(j);
        InternalDistributedMember member = getJavaGroupsIdForVM(vm);
        if (gemFireVM.getId().equals(member)) {
          return vm;
        }
      }
    }
    return null;
  }

  private InternalDistributedMember getJavaGroupsIdForVM(VM vm) {
    return vm.invoke(() -> remoteGetJavaGroupsIdForVM());
  }

  private InternalDistributedMember remoteGetJavaGroupsIdForVM() {
    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    return system.getDistributionManager().getDistributionManagerId();
  }
}
