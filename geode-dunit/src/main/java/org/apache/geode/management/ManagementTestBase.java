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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.management.ObjectName;

import org.junit.Rule;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;

@SuppressWarnings("serial")
public abstract class ManagementTestBase extends CacheTestCase {

  private static final int MAX_WAIT = 70 * 1000;

  protected static ManagementService managementService;

  /**
   * List containing all the Managed Node VM
   */
  protected static List<VM> managedNodeList;

  /**
   * Managing Node VM
   */
  protected static VM managingNode;

  protected static VM managedNode1;
  protected static VM managedNode2;
  protected static VM managedNode3;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Override
  public final void postSetUp() throws Exception {
    JUnit4DistributedTestCase.disconnectAllFromDS();

    Host host = Host.getHost(0);
    managingNode = host.getVM(0);
    managedNode1 = host.getVM(1);
    managedNode2 = host.getVM(2);
    managedNode3 = host.getVM(3);

    managedNodeList = new ArrayList<>();
    managedNodeList.add(managedNode1);
    managedNodeList.add(managedNode2);
    managedNodeList.add(managedNode3);

    postSetUpManagementTestBase();
  }

  protected void postSetUpManagementTestBase() throws Exception {
    // override if needed
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    preTearDownManagementTestBase();

  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    managementService = null;
    postTearDownManagementTestBase();
  }

  protected void preTearDownManagementTestBase() throws Exception {
    // override if needed
  }

  protected void postTearDownManagementTestBase() throws Exception {
    // override if needed
  }

  /**
   * managingNodeFirst variable tests for two different test cases where Managing & Managed Node
   * creation time lines are reversed.
   */
  protected void initManagement(final boolean managingNodeFirst) {
    if (managingNodeFirst) {
      createManagementCache(managingNode);
      startManagingNode(managingNode);
      for (VM vm : managedNodeList) {
        createCache(vm);
      }

    } else {
      for (VM vm : managedNodeList) {
        createCache(vm);
      }
      createManagementCache(managingNode);
      startManagingNode(managingNode);
    }
  }

  protected void createCache(final VM vm) {
    vm.invoke("Create Cache", () -> {
      createCache(false);
    });
  }

  protected void createCache(final VM vm, final Properties props) {
    vm.invoke("Create Cache", () -> {
      createCache(props);
    });
  }

  private Cache createCache(final Properties config) {
    Cache cache = getCache(config);
    managementService = ManagementService.getManagementService(cache);
    return cache;
  }

  protected Cache createCache(final boolean isManager) {
    Properties props = new Properties();

    if (isManager) {
      props.setProperty(JMX_MANAGER, "true");
      props.setProperty(JMX_MANAGER_START, "false");
      props.setProperty(JMX_MANAGER_PORT, "0");
      props.setProperty(JMX_MANAGER_HTTP_PORT, "0");
    }

    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(LOG_FILE, JUnit4DistributedTestCase.getTestMethodName() + "-.log");

    Cache cache = getCache(props);
    managementService = ManagementService.getManagementService(cache);

    return cache;
  }

  protected void createManagementCache(final VM vm) {
    vm.invoke("Create Management Cache", () -> {
      createCache(true);
    });
  }

  protected void closeCache(final VM vm) {
    vm.invoke("Close Cache", () -> {
      InternalCache existingInstance = GemFireCacheImpl.getInstance();
      if (existingInstance != null) {
        existingInstance.close();
      }
      InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
      if (ds != null) {
        ds.disconnect();
      }
    });
  }

  protected String getMemberId(final VM vm) {
    return vm.invoke("getMemberId", () -> {
      InternalCache cache = GemFireCacheImpl.getInstance();
      return cache.getDistributedSystem().getDistributedMember().getId();
    });
  }

  protected static void waitForProxy(final ObjectName objectName, final Class interfaceClass) {
    GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
      @Override
      public String description() {
        return "Waiting for the proxy of " + objectName.getCanonicalName()
            + " to get propagated to Manager";
      }

      @Override
      public boolean done() {
        SystemManagementService service = (SystemManagementService) managementService;
        return service.getMBeanProxy(objectName, interfaceClass) != null;
      }
    });
  }

  /**
   * Marks a VM as Managing
   */
  protected void startManagingNode(final VM vm) {
    vm.invoke("Start Being Managing Node", () -> {
      Cache existingCache = GemFireCacheImpl.getInstance();
      managementService = ManagementService.getManagementService(existingCache);
      SystemManagementService service = (SystemManagementService) managementService;
      service.createManager();
      service.startManager();
    });
  }

  /**
   * Stops a VM as a Managing node
   */
  protected void stopManagingNode(final VM vm) {
    vm.invoke("Stop Being Managing Node", () -> {
      Cache existingCache = GemFireCacheImpl.getInstance();
      if (existingCache != null && !existingCache.isClosed()) {
        if (managementService.isManager()) {
          managementService.stopManager();
        }
      }
    });
  }

  protected static List<VM> getManagedNodeList() {
    return managedNodeList;
  }

  protected static VM getManagingNode() {
    return managingNode;
  }

  protected static ManagementService getManagementService() {
    return managementService;
  }

  /**
   * Creates a Local region
   */
  protected void createLocalRegion(final VM vm, final String localRegionName) {
    vm.invoke("Create Local region", () -> {
      InternalCache cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) getManagementService();
      RegionFactory factory = cache.createRegionFactory(RegionShortcut.LOCAL);
      factory.create(localRegionName);
    });
  }

  /**
   * Creates a partition Region
   */
  protected void createPartitionRegion(final VM vm, final String partitionRegionName) {
    vm.invoke("Create Partitioned region", () -> {
      InternalCache cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) getManagementService();
      RegionFactory factory = cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT);
      factory.create(partitionRegionName);
    });
  }

  protected static void waitForRefresh(final int expectedRefreshCount,
      final ObjectName objectName) {
    ManagementService service = getManagementService();

    GeodeAwaitility.await().untilAsserted(new WaitCriterion() {

      private int actualRefreshCount = 0;
      private long lastRefreshTime = service.getLastUpdateTime(objectName);

      @Override
      public String description() {
        return "Waiting For Proxy Refresh Count = " + expectedRefreshCount;
      }

      @Override
      public boolean done() {
        long newRefreshTime = service.getLastUpdateTime(objectName);
        if (newRefreshTime > lastRefreshTime) {
          lastRefreshTime = newRefreshTime;
          actualRefreshCount++;

        }
        return actualRefreshCount >= expectedRefreshCount;
      }
    });
  }

  protected DistributedMember getMember(final VM vm) {
    SerializableCallable getMember = new SerializableCallable("Get Member") {
      public Object call() {
        InternalCache cache = GemFireCacheImpl.getInstance();
        return cache.getDistributedSystem().getDistributedMember();
      }
    };
    return (DistributedMember) vm.invoke(getMember);
  }

  protected <T> T getMBeanProxy(final ObjectName objectName, final Class<T> interfaceClass) {
    SystemManagementService service =
        (SystemManagementService) ManagementService.getManagementService(getCache());
    return service.getMBeanProxy(objectName, interfaceClass);
  }
}
