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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.management.ObjectName;

import org.junit.Rule;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.FederatingManager;
import org.apache.geode.management.internal.LocalManager;
import org.apache.geode.management.internal.ManagementStrings;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;

@SuppressWarnings("serial")
public abstract class ManagementTestBase extends JUnit4CacheTestCase {

  private static final int MAX_WAIT = 70 * 1000;

  // protected static DistributedSystem ds;
  protected static ManagementService managementService;
  // protected static Cache cache;

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
  protected static VM locatorVM;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();

    Host host = Host.getHost(0);
    managingNode = host.getVM(0);
    managedNode1 = host.getVM(1);
    managedNode2 = host.getVM(2);
    managedNode3 = host.getVM(3);

    managedNodeList = new ArrayList<VM>();

    managedNodeList.add(managedNode1);
    managedNodeList.add(managedNode2);
    managedNodeList.add(managedNode3);
    locatorVM = host.getLocator();
    postSetUpManagementTestBase();
  }

  protected void postSetUpManagementTestBase() throws Exception {}

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    preTearDownManagementTestBase();

  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    managementService = null;
    postTearDownManagementTestBase();
  }

  protected void preTearDownManagementTestBase() throws Exception {}

  protected void postTearDownManagementTestBase() throws Exception {}

  /**
   * managingNodeFirst variable tests for two different test cases where Managing & Managed Node
   * creation time lines are reversed.
   */
  protected void initManagement(final boolean managingNodeFirst) throws Exception {
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

  protected void createCache(final VM vm1) throws Exception {
    vm1.invoke("Create Cache", () -> {
      createCache(false);
    });
  }

  protected void createCache(final VM vm1, final Properties props) throws Exception {
    vm1.invoke("Create Cache", () -> {
      createCache(props);
    });
  }

  private Cache createCache(final Properties props) {
    Cache cache = getCache(props);
    managementService = ManagementService.getManagementService(cache);

    return cache;
  }

  protected Cache createCache(final boolean management) {

    Properties props = new Properties();
    if (management) {
      props.setProperty(JMX_MANAGER, "true");
      props.setProperty(JMX_MANAGER_START, "false");
      props.setProperty(JMX_MANAGER_PORT, "0");
      props.setProperty(JMX_MANAGER_HTTP_PORT, "0");
    }
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(LOG_FILE, getTestMethodName() + "-.log");

    Cache cache = getCache(props);
    managementService = ManagementService.getManagementService(cache);

    return cache;
  }

  protected void createManagementCache(final VM vm1) throws Exception {
    vm1.invoke("Create Management Cache", () -> {
      createCache(true);
    });
  }

  protected void closeCache(final VM vm1) throws Exception {
    vm1.invoke("Close Cache", () -> {
      GemFireCacheImpl existingInstance = GemFireCacheImpl.getInstance();
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
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      return cache.getDistributedSystem().getDistributedMember().getId();
    });
  }

  protected static void waitForProxy(final ObjectName objectName, final Class interfaceClass) {
    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public String description() {
        return "Waiting for the proxy of " + objectName.getCanonicalName()
            + " to get propagated to Manager";
      }

      @Override
      public boolean done() {
        SystemManagementService service = (SystemManagementService) managementService;
        if (service.getMBeanProxy(objectName, interfaceClass) != null) {
          return true;
        } else {
          return false;
        }
      }
    }, MAX_WAIT, 500, true);
  }

  /**
   * Marks a VM as Managing
   */
  protected void startManagingNode(final VM vm1) {
    vm1.invoke("Start Being Managing Node", () -> {
      Cache existingCache = GemFireCacheImpl.getInstance();
      // if (existingCache != null && !existingCache.isClosed()) {
      managementService = ManagementService.getManagementService(existingCache);
      SystemManagementService service = (SystemManagementService) managementService;
      service.createManager();
      service.startManager();
      // }
    });
  }

  /**
   * Stops a VM as a Managing node
   */
  protected void stopManagingNode(final VM vm1) {
    vm1.invoke("Stop Being Managing Node", () -> {
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
   * Creates a Distributed region
   */
  protected void createDistributedRegion(final VM vm, final String regionName)
      throws InterruptedException {
    AsyncInvocation future = createDistributedRegionAsync(vm, regionName);
    future.join(MAX_WAIT);
    if (future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if (future.exceptionOccurred()) {
      throw new RuntimeException(future.getException());
    }
  }

  /**
   * Creates a Local region
   */
  protected void createLocalRegion(final VM vm, final String localRegionName) throws Exception {
    vm.invoke("Create Local region", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) getManagementService();
      RegionFactory rf = cache.createRegionFactory(RegionShortcut.LOCAL);

      LogWriterUtils.getLogWriter().info("Creating Local Region");
      rf.create(localRegionName);
    });
  }

  /**
   * Creates a Sub region
   */
  protected void createSubRegion(final VM vm, final String parentRegionPath,
      final String subregionName) throws Exception {
    vm.invoke("Create Sub region", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) getManagementService();
      Region region = cache.getRegion(parentRegionPath);

      LogWriterUtils.getLogWriter().info("Creating Sub Region");
      region.createSubregion(subregionName, region.getAttributes());
    });
  }

  /**
   * Creates a Distributed Region
   */
  private AsyncInvocation createDistributedRegionAsync(final VM vm, final String regionName) {
    return vm.invokeAsync("Create Distributed region", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) getManagementService();

      RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
      LogWriterUtils.getLogWriter().info("Creating Dist Region");
      rf.create(regionName);
    });
  }

  /**
   * Creates a partition Region
   */
  protected void createPartitionRegion(final VM vm, final String partitionRegionName) {
    vm.invoke("Create Partitioned region", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) getManagementService();
      RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT);
      LogWriterUtils.getLogWriter().info("Creating Par Region");
      rf.create(partitionRegionName);
    });
  }

  protected void waitForAllMembers(final int expectedCount) {
    ManagementService service = getManagementService();
    final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();

    assertNotNull(service.getDistributedSystemMXBean());

    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public String description() {
        return "Waiting All members to intimate DistributedSystemMBean";
      }

      @Override
      public boolean done() {
        if (bean.listMemberObjectNames() != null) {
          LogWriterUtils.getLogWriter()
              .info("Member Length " + bean.listMemberObjectNames().length);
        }
        if (bean.listMemberObjectNames().length >= expectedCount) {
          return true;
        } else {
          return false;
        }
      }
    }, MAX_WAIT, 500, true);

    assertNotNull(bean.getManagerObjectName());
  }

  protected static void waitForRefresh(final int expectedRefreshCount,
      final ObjectName objectName) {
    final ManagementService service = getManagementService();

    Wait.waitForCriterion(new WaitCriterion() {
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
        if (actualRefreshCount >= expectedRefreshCount) {
          return true;
        }
        return false;
      }
    }, MAX_WAIT, 500, true);
  }

  protected DistributedMember getMember(final VM vm) {
    SerializableCallable getMember = new SerializableCallable("Get Member") {
      public Object call() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        return cache.getDistributedSystem().getDistributedMember();

      }
    };
    return (DistributedMember) vm.invoke(getMember);
  }

  protected boolean mbeanExists(final ObjectName objectName) {
    return ManagementFactory.getPlatformMBeanServer().isRegistered(objectName);
  }

  protected <T> T getMBeanProxy(final ObjectName objectName, Class<T> interfaceClass) {
    SystemManagementService service =
        (SystemManagementService) ManagementService.getManagementService(getCache());
    return service.getMBeanProxy(objectName, interfaceClass);
  }
}
