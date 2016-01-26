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
package com.gemstone.gemfire.management;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.statistics.SampleCollector;
import com.gemstone.gemfire.management.internal.FederatingManager;
import com.gemstone.gemfire.management.internal.LocalManager;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementStrings;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class ManagementTestBase extends DistributedTestCase {

  public ManagementTestBase(String name) {
    super(name);

  }

  private static final int MAX_WAIT = 70 * 1000;

  private static final long serialVersionUID = 1L;
  /** log writer instance */
  private static LogWriter logWriter;


  private static Properties props = new Properties();

  /**
   * Distributed System
   */
  protected static DistributedSystem ds;

  /**
   * List containing all the Managed Node VM
   */
  protected static List<VM> managedNodeList;

  /**
   * Managing Node VM
   */
  protected static VM managingNode;

  /**
   * Management Service
   */
  protected static ManagementService managementService;

  protected static VM managedNode1;
  protected static VM managedNode2;
  protected static VM managedNode3;
  protected static VM locatorVM;

  private static SampleCollector sampleCollector;
  
  protected static MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;



  private static int mcastPort;

  private static Cache cache;

  public void setUp() throws Exception {
    disconnectAllFromDS();
    super.setUp();
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

  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    closeAllCache();
    managementService = null;

    mcastPort = 0;
    disconnectAllFromDS();
    props.clear();
  }


  public void closeAllCache() throws Exception{
    closeCache(managingNode);
    closeCache(managedNode1);
    closeCache(managedNode2);
    closeCache(managedNode3);
    cache = null;
  }

  /**
   * Enable system property gemfire.disableManagement false in each VM.
   *
   * @throws Exception
   */
  public void enableManagement() {
    invokeInEveryVM(new SerializableRunnable("Enable Management") {
      public void run() {
        System.setProperty(InternalDistributedSystem.DISABLE_MANAGEMENT_PROPERTY, "false");
      }
    });

  }

  /**
   * Disable system property gemfire.disableManagement true in each VM.
   *
   * @throws Exception
   */
  public void disableManagement() {
    invokeInEveryVM(new SerializableRunnable("Disable Management") {
      public void run() {
        System.setProperty(InternalDistributedSystem.DISABLE_MANAGEMENT_PROPERTY, "true");
      }
    });

  }


  /**
   * managingNodeFirst variable tests for two different test cases where
   * Managing & Managed Node creation time lines are reversed.
   *
   */
  public void initManagement(boolean managingNodeFirst) throws Exception {

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


  @SuppressWarnings("serial")
  public void createCache(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Create Cache") {
      public void run() {
        createCache(false);
      }
    });

  }

  @SuppressWarnings("serial")
  public void createCache(VM vm1, final Properties props) throws Exception {
    vm1.invoke(new SerializableRunnable("Create Cache") {
      public void run() {
        createCache(props);
      }
    });

  }

  public Cache createCache(Properties props) {
    System.setProperty("dunitLogPerTest", "true");
    props.setProperty(DistributionConfig.LOG_FILE_NAME,testName+"-.log");
    ds = (new ManagementTestBase("temp")).getSystem(props);
    cache = CacheFactory.create(ds);
    managementService = ManagementService.getManagementService(cache);
    logWriter = ds.getLogWriter();
    assertNotNull(cache);
    assertNotNull(managementService);
    return cache;
  }


  public Cache getCache() {
    return cache;
  }

  public Cache createCache(boolean management) {
    System.setProperty("dunitLogPerTest", "true");
    if (management) {
      props.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
      props.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "false");
      props.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, "0");
      props.setProperty(DistributionConfig.JMX_MANAGER_HTTP_PORT_NAME, "0");
    }
    props.setProperty(DistributionConfig.ENABLE_TIME_STATISTICS_NAME, "true");
    props.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.LOG_FILE_NAME,testName+"-.log");
    ds = (new ManagementTestBase("temp")).getSystem(props);
    cache = CacheFactory.create(ds);
    managementService = ManagementService.getManagementService(cache);
    logWriter = ds.getLogWriter();
    assertNotNull(cache);
    assertNotNull(managementService);
    return cache;
  }


  @SuppressWarnings("serial")
  public void createManagementCache(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Create Management Cache") {
      public void run() {
        createCache(true);
      }
    });
  }


  @SuppressWarnings("serial")
  public void closeCache(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Close Cache") {
      public void run() {
        GemFireCacheImpl existingInstance = GemFireCacheImpl.getInstance();
        if (existingInstance != null) {
          existingInstance.close();
        }
        InternalDistributedSystem ds = InternalDistributedSystem
            .getConnectedInstance();
        if (ds != null) {
          ds.disconnect();
        }
      }
    });

  }

  public String getMemberId(final VM vm) {
    SerializableCallable getMember = new SerializableCallable("getMemberId") {
      public Object call() throws Exception{
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        return cache.getDistributedSystem().getDistributedMember().getId();
      }
    };
    return (String) vm.invoke(getMember);
  }

  protected void waitForProxy(final ObjectName objectName,
      final Class interfaceClass) {

    waitForCriterion(new WaitCriterion() {
      public String description() {
        return "Waiting for the proxy of " + objectName.getCanonicalName()
            + " to get propagated to Manager";
      }

      public boolean done() {
        SystemManagementService service = (SystemManagementService)managementService;
        if (service.getMBeanProxy(objectName, interfaceClass) != null) {
          return true;
        } else {
          return false;
        }
      }

    }, MAX_WAIT, 500, true);
  }

  protected void runManagementTaskAdhoc(){
    SystemManagementService service = (SystemManagementService)managementService;
    service.getLocalManager().runManagementTaskAdhoc();
  }

  /**
   * Marks a VM as Managing
   *
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void startManagingNode(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Start Being Managing Node") {
      public void run() {
        startBeingManagingNode();
      }
    });

  }

  public void startBeingManagingNode() {
    Cache existingCache = GemFireCacheImpl.getInstance();
    if (existingCache != null && !existingCache.isClosed()) {
      managementService = ManagementService.getManagementService(existingCache);
      SystemManagementService service = (SystemManagementService) managementService;
      service.createManager();
      service.startManager();
    }
  }

  /**
   * Marks a VM as Managing
   *
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void startManagingNodeAsync(VM vm1) throws Exception {
    vm1.invokeAsync(new SerializableRunnable("Start Being Managing Node") {

      public void run() {
        Cache existingCache = GemFireCacheImpl.getInstance();
        if (existingCache != null && !existingCache.isClosed()) {
          managementService = ManagementService
              .getManagementService(existingCache);
          managementService.startManager();
        }

      }
    });

  }

  /**
   * Stops a VM as a Managing node
   *
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void stopManagingNode(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Stop Being Managing Node") {
      public void run() {
        Cache existingCache = GemFireCacheImpl.getInstance();
        if (existingCache != null && !existingCache.isClosed()) {
          if (managementService.isManager()) {
            managementService.stopManager();
          }

        }

      }
    });

  }

  /**
   *
   * Check various resources clean up Once a VM stops being managable it should
   * remove all the artifacts of management namely a) Notification region b)
   * Monitoring Region c) Management task should stop
   */
  @SuppressWarnings("serial")
  public void checkManagedNodeCleanup(VM vm) throws Exception {
    vm.invoke(new SerializableRunnable("Managing Node Clean up") {

      public void run() {
        Cache existingCache = GemFireCacheImpl.getInstance();
        if (existingCache != null) {
          // Cache is closed
          assertEquals(true, existingCache.isClosed());
          // ManagementService should throw exception
          LocalManager localManager = ((SystemManagementService) managementService)
              .getLocalManager();
          // Check Monitoring region destroyed
          Region monitoringRegion = localManager.getManagementResourceRepo()
              .getLocalMonitoringRegion();
          assertEquals(null, monitoringRegion);
          // check Notification region is destroyed
          Region notifRegion = localManager.getManagementResourceRepo()
              .getLocalNotificationRegion();
          assertEquals(null, notifRegion);
          // check ManagementTask is stopped
          assertEquals(true, localManager.getFederationSheduler().isShutdown());

        }

      }
    });

  }

  /**
   *
   * Check various resources clean up Once a VM stops being Managing.It should
   * remove all the artifacts of management namely a) proxies b) Monitoring
   * Region c) Management task should stop
   */

  public void checkProxyCleanup(VM vm) throws Exception {

    vm.invoke(new SerializableRunnable("Managing Node Clean up") {

      public void run() {

        try {
          GemFireCacheImpl existingCache = GemFireCacheImpl.getInstance();
          if (existingCache == null) {
            return;
          }

          assertEquals(false, existingCache.isClosed());
          // ManagementService should not be closed

          Set<DistributedMember> otherMemberSet = existingCache
              .getDistributionManager().getOtherDistributionManagerIds();

          Iterator<DistributedMember> it = otherMemberSet.iterator();
          FederatingManager federatingManager = ((SystemManagementService) managementService)
              .getFederatingManager();

          // check Proxy factory. There should not be any proxies left
          DistributedMember member;
          while (it.hasNext()) {
            member = it.next();

            assertNull(federatingManager.getProxyFactory().findAllProxies(
                member));
          }


        } catch (ManagementException e) {
          fail("failed with ManagementException", e);
        }
      }
    });

  }

  /**
   * All the expected exceptions are checked here
   *
   * @param e
   * @return is failed
   */
  public boolean checkManagementExceptions(ManagementException e) {

    if (e.getMessage()
        .equals(ManagementStrings.Management_Service_CLOSED_CACHE)
        || e.getMessage().equals(
            ManagementStrings.Management_Service_MANAGEMENT_SERVICE_IS_CLOSED
                .toLocalizedString())
        || e
            .getMessage()
            .equals(
                ManagementStrings.Management_Service_MANAGEMENT_SERVICE_NOT_STARTED_YET
                    .toLocalizedString())
        || e.getMessage().equals(
            ManagementStrings.Management_Service_NOT_A_GEMFIRE_DOMAIN_MBEAN
                .toLocalizedString())
        || e.getMessage().equals(
            ManagementStrings.Management_Service_NOT_A_MANAGING_NODE_YET
                .toLocalizedString())
        || e
            .getMessage()
            .equals(
                ManagementStrings.Management_Service_OPERATION_NOT_ALLOWED_FOR_CLIENT_CACHE
                    .toLocalizedString())
        || e.getMessage().equals(
            ManagementStrings.Management_Service_PROXY_NOT_AVAILABLE
                .toLocalizedString())) {

      return false;
    }
    return true;
  }


  public static List<VM> getManagedNodeList() {
    return managedNodeList;
  }

  public static VM getManagingNode() {
    return managingNode;
  }

  public static ManagementService getManagementService() {
    return managementService;
  }

  /**
   * Creates a Distributed region
   *
   * @param vm
   *          reference to VM
   * @param regionName
   *          name of the distributed region
   * @throws Throwable
   */
  protected void createDistributedRegion(VM vm, final String regionName)
      throws Exception {
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
   *
   * @param vm
   *          reference to VM
   * @param localRegionName
   *          name of the local region
   * @throws Throwable
   */
  protected void createLocalRegion(VM vm, final String localRegionName)
      throws Exception {
    SerializableRunnable createLocalRegion = new SerializableRunnable(
        "Create Local region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        SystemManagementService service = (SystemManagementService) getManagementService();
        RegionFactory rf = cache
            .createRegionFactory(RegionShortcut.LOCAL);

        getLogWriter().info("Creating Local Region");
        rf.create(localRegionName);

      }
    };
    vm.invoke(createLocalRegion);
  }

  /**
   * Creates a Sub region
   *
   * @param vm
   *          reference to VM
   * @param localRegionName
   *          name of the local region
   * @throws Throwable
   */
  protected void createSubRegion(VM vm, final String parentRegionPath, final String subregionName)
      throws Exception {
    SerializableRunnable createSubRegion = new SerializableRunnable(
        "Create Sub region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        SystemManagementService service = (SystemManagementService) getManagementService();
        Region region = cache.getRegion(parentRegionPath);

        getLogWriter().info("Creating Sub Region");
        region.createSubregion(subregionName, region.getAttributes());

      }
    };
    vm.invoke(createSubRegion);
  }

  /**
   * Puts in distributed region
   *
   * @param vm
   */
  protected void putInDistributedRegion(final VM vm, final String key,
      final String value, final String regionPath) {
    SerializableRunnable put = new SerializableRunnable(
        "Put In Distributed Region") {
      public void run() {

        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Region region = cache.getRegion(regionPath);
        region.put(key, value);

      }
    };
    vm.invoke(put);
  }

  /**
   * Creates a Distributed Region
   *
   * @param vm
   */
  protected AsyncInvocation createDistributedRegionAsync(final VM vm,
      final String regionName) {
    SerializableRunnable createRegion = new SerializableRunnable(
        "Create Distributed region") {
      public void run() {

        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        SystemManagementService service = (SystemManagementService) getManagementService();

        RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
        getLogWriter().info("Creating Dist Region");
        rf.create(regionName);

      }
    };
    return vm.invokeAsync(createRegion);
  }

  /**
   * Creates a partition Region
   *
   * @param vm
   */
  protected void createPartitionRegion(final VM vm,
      final String partitionRegionName) {
    SerializableRunnable createParRegion = new SerializableRunnable(
        "Create Partitioned region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        SystemManagementService service = (SystemManagementService) getManagementService();
        RegionFactory rf = cache
            .createRegionFactory(RegionShortcut.PARTITION_REDUNDANT);
        getLogWriter().info("Creating Par Region");
        rf.create(partitionRegionName);

      }
    };
    vm.invoke(createParRegion);
  }

  /**
   * closes a Distributed Region
   *
   * @param vm
   */
  protected void closeRegion(final VM vm, final String regionPath) {
    SerializableRunnable closeRegion = new SerializableRunnable(
        "Close Distributed region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

        getLogWriter().info("Closing Dist Region");
        Region region = cache.getRegion(regionPath);
        region.close();

      }
    };
    vm.invoke(closeRegion);
  }

  public void waitForAllMembers(final int expectedCount) {
    ManagementService service = getManagementService();
    final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();

    assertNotNull(service.getDistributedSystemMXBean());


    waitForCriterion(new WaitCriterion() {
      public String description() {
        return "Waiting All members to intimate DistributedSystemMBean";
      }

      public boolean done() {
        if (bean.listMemberObjectNames() != null) {

          getLogWriter().info(
              "Member Length " + bean.listMemberObjectNames().length);

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

  public void waitForRefresh(final int expectedRefreshCount,
      final ObjectName objectName) {
    final ManagementService service = getManagementService();

    final long currentTime = System.currentTimeMillis();

    waitForCriterion(new WaitCriterion() {
      int actualRefreshCount = 0;
      long lastRefreshTime = service.getLastUpdateTime(objectName);

      public String description() {
        return "Waiting For Proxy Refresh Count = " + expectedRefreshCount;
      }

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

  public DistributedMember getMember(final VM vm) {
    SerializableCallable getMember = new SerializableCallable("Get Member") {
      public Object call() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        return cache.getDistributedSystem().getDistributedMember();

      }
    };
    return (DistributedMember) vm.invoke(getMember);
  }
}
