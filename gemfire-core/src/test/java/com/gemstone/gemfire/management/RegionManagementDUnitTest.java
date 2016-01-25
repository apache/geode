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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.TestObjectSizerImpl;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;
import com.gemstone.gemfire.internal.cache.partitioned.fixed.SingleHopQuarterPartitionResolver;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This class checks and verifies various data and operations exposed through
 * RegionMXBean interface.
 * 
 * Goal of the Test : RegionMBean gets created once region is created. Data like
 * Region Attributes data and stats are of proper value
 * 
 * @author rishim
 * 
 */
public class RegionManagementDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  private final String VERIFY_CONFIG_METHOD = "verifyConfigData";

  private final String VERIFY_REMOTE_CONFIG_METHOD = "verifyConfigDataRemote";

  static final String REGION_NAME = "MANAGEMENT_TEST_REGION";

  static final String PARTITIONED_REGION_NAME = "MANAGEMENT_PAR_REGION";

  static final String FIXED_PR_NAME = "MANAGEMENT_FIXED_PR";
  
  static final String REGION_PATH = "/MANAGEMENT_TEST_REGION";

  static final String PARTITIONED_REGION_PATH = "/MANAGEMENT_PAR_REGION";

  static final String FIXED_PR_PATH = "/MANAGEMENT_FIXED_PR";
  
  static final String LOCAL_REGION_NAME = "TEST_LOCAL_REGION";
  static final String LOCAL_SUB_REGION_NAME = "TEST_LOCAL_SUB_REGION";
  static final String LOCAL_REGION_PATH = "/TEST_LOCAL_REGION";
  static final String LOCAL_SUB_REGION_PATH = "/TEST_LOCAL_REGION/TEST_LOCAL_SUB_REGION";
  
  private static final int MAX_WAIT = 70 * 1000;

  protected static final Region DiskRegion = null;

  static List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();


  private static Region fixedPrRegion;


  public RegionManagementDUnitTest(String name) {
    super(name);
    
  }

  public void setUp() throws Exception {
    super.setUp();

  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    
  }

  /**
   * Tests all Region MBean related Management APIs
   * 
   * a) Notification propagated to member MBean while a region is created
   * 
   * b) Creates and check a Distributed Region
   * 
   * 
   * @throws Exception
   */

  public void testDistributedRegion() throws Exception {

    initManagement(false);


    VM[] managedNodes = new VM[getManagedNodeList()
        .size()];

    getManagedNodeList().toArray(managedNodes);
    // Adding notif listener for remote cache members
    addMemberListener(managingNode);

    for (int j = 0; j < managedNodes.length; j++) {

      VM vm = managedNodes[j];

      createDistributedRegion(vm, REGION_NAME);
      validateReplicateRegionAfterCreate(vm);

    }

    verifyRemoteDistributedRegion(managingNode, 3);


    for (VM vm : getManagedNodeList()) {
      closeRegion(vm, REGION_PATH);
      validateReplicatedRegionAfterClose(vm);
    }
    
    ensureProxyCleanup(managingNode);
  }
  
  /**
   * Tests all Region MBean related Management APIs
   * 
   * a) Notification propagated to member MBean while a region is created
   * 
   * b) Created and check a Partitioned Region
   * 
   * @throws Exception
   */
  public void testPartitionedRegion() throws Exception {
    initManagement(false);

    VM managingNode = getManagingNode();

    VM[] managedNodes = new VM[getManagedNodeList()
        .size()];

    getManagedNodeList().toArray(managedNodes);
    // Adding notif listener for remote cache members

    addMemberListener(managingNode);

    for (int j = 0; j < managedNodes.length; j++) {

      VM vm = managedNodes[j];
      createPartitionRegion(vm, PARTITIONED_REGION_NAME);
      validatePartitionRegionAfterCreate(vm);
    }
    

    validateRemotePartitionRegion(managingNode);

    for (VM vm : getManagedNodeList()) {

      closeRegion(vm, PARTITIONED_REGION_PATH);
      validatePartitionRegionAfterClose(vm);
    }
  }
  
  /**
   * Tests all Region MBean related Management APIs
   * 
   * a) Notification propagated to member MBean while a region is created
   * 
   * b) Creates and check a Fixed Partitioned Region
   * 
   * @throws Exception
   */
  public void testFixedPRRegionMBean() throws Exception {

    initManagement(false);

    VM managingNode = getManagingNode();

    VM[] managedNodes = new VM[getManagedNodeList()
        .size()];

    getManagedNodeList().toArray(managedNodes);
    // Adding notif listener for remote cache members
    addMemberListener(managingNode);

    for (int j = 0; j < managedNodes.length; j++) {

      VM vm = managedNodes[j];

      createFixedPartitionList(j + 1);
      Object[] args = new Object[1];
      args[0] = fpaList;
      vm.invoke(RegionManagementDUnitTest.class, "createFixedPartitionRegion",
          args);

    }
    // Workaround for bug 46683. Renable validation when bug is fixed.
    validateRemoteFixedPartitionRegion(managingNode);

    for (VM vm : getManagedNodeList()) {
      closeFixedPartitionRegion(vm);
    }
  }

  /**
   * Tests a Distributed Region at Managing Node side
   * while region is created in a member node asynchronously.
   * @throws Exception
   */
  public void testRegionAggregate() throws Exception{
    initManagement(true);

    VM managingNode = getManagingNode();

    VM[] managedNodes = new VM[getManagedNodeList()
        .size()];

    getManagedNodeList().toArray(managedNodes);
    // Adding notif listener for remote cache members
    addDistrListener(managingNode);


    for (int j = 0; j < managedNodes.length; j++) {

      VM vm = managedNodes[j];

      createDistributedRegion(vm, REGION_NAME);

    }

    
    validateDistributedMBean(managingNode, 3);
    
    createDistributedRegion(managingNode, REGION_NAME);
    validateDistributedMBean(managingNode, 4);
    


    for (int j = 0; j < managedNodes.length; j++) {

      VM vm = managedNodes[j];

      closeRegion(vm, REGION_PATH);

    }
    ensureProxyCleanup(managingNode);
    
    validateDistributedMBean(managingNode, 1);
    
    closeRegion(managingNode, REGION_PATH);
    validateDistributedMBean(managingNode, 0);
    

  }
  
  public void testNavigationAPIS() throws Exception {
    initManagement(true);
    for(VM vm : managedNodeList){
      createDistributedRegion(vm, REGION_NAME);
      createPartitionRegion(vm, PARTITIONED_REGION_NAME);
    }
    createDistributedRegion(managingNode, REGION_NAME);
    createPartitionRegion(managingNode, PARTITIONED_REGION_NAME);
    List<String> memberIds = new ArrayList<String>();
    
    for(VM vm : managedNodeList){
      memberIds.add(getMemberId(vm));
    }
    checkNavigationAPIS(managingNode, memberIds);
    


    for(VM vm : managedNodeList){
      closeRegion(vm, REGION_PATH);
    }
 
    closeRegion(managingNode, REGION_PATH);

  }

 
  
  public void testSubRegions() throws Exception{
    initManagement(false);
    for (VM vm : managedNodeList) {
      createLocalRegion(vm, LOCAL_REGION_NAME);
      createSubRegion(vm, LOCAL_REGION_NAME, LOCAL_SUB_REGION_NAME);
    }
    
    for (VM vm : managedNodeList) {
      checkSubRegions(vm, LOCAL_SUB_REGION_PATH);
    }
    
    for (VM vm : managedNodeList) {
      closeRegion(vm, LOCAL_REGION_NAME);
      checkNullRegions(vm, LOCAL_SUB_REGION_NAME);
    }
    
  }
  
  
  
  
  public void testSpecialRegions() throws Exception{
    initManagement(false);
    createSpecialRegion(managedNodeList.get(0));
    DistributedMember member = getMember(managedNodeList.get(0));
    checkSpecialRegion(managingNode,member);
  }
  
  
  public void createSpecialRegion(VM vm1) throws Exception{
    {
      vm1.invoke(new SerializableRunnable("Check Sub Regions") {

        public void run() {
          Cache cache = getCache();
          AttributesFactory attributesFactory = new AttributesFactory();
          attributesFactory.setValueConstraint(Portfolio.class);
          RegionAttributes regionAttributes = attributesFactory.create();
          
          cache.createRegion("p:os",regionAttributes);
          cache.createRegion("p@os",regionAttributes);
          cache.createRegion("p-os",regionAttributes);
          cache.createRegion("p#os",regionAttributes);
          cache.createRegion("p+os",regionAttributes);
          cache.createRegion("p?os",regionAttributes);
        }
      });

    }
  }
  
  public void checkSpecialRegion(VM vm1, final DistributedMember member)
      throws Exception {
    {
      vm1.invoke(new SerializableRunnable("Check Sub Regions") {

        public void run() {

          ManagementService service = getManagementService();
          
          try {
            MBeanUtil.getDistributedRegionMbean("/p:os", 1);
            MBeanUtil.getDistributedRegionMbean("/p@os", 1);
            MBeanUtil.getDistributedRegionMbean("/p-os", 1);
            MBeanUtil.getDistributedRegionMbean("/p#os", 1);
            MBeanUtil.getDistributedRegionMbean("/p+os", 1);
            MBeanUtil.getDistributedRegionMbean("/p?os", 1);

          } catch (Exception e) {
            InternalDistributedSystem.getLoggerI18n().fine(
                "Undesired Result , DistributedRegionMXBean Should not be null"
                    + e);
          }

        }
      });

    }

  }
  
  public void testLruStats() throws Exception{
    initManagement(false);
    for (VM vm : managedNodeList) {
      createDiskRegion(vm);

    }
    checkEntrySize(managingNode,3);
  }
  
  public void createDiskRegion(VM vm1) throws Exception{
    {
      vm1.invoke(new SerializableRunnable("Check Sub Regions") {

        public void run() {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setEvictionAttributes(EvictionAttributes
                .createLRUMemoryAttributes(20, new TestObjectSizerImpl(),
                    EvictionAction.LOCAL_DESTROY));
          /*File d = new File("DiskRegions" + OSProcess.getId());
          d.mkdirs();

          DiskStoreFactory dsf = getCache().createDiskStoreFactory();
          dsf.setDiskDirs(new File[]{d});
          factory.setDiskSynchronous(true);
          DiskStore ds = dsf.create(REGION_NAME);
          factory.setDiskStoreName(ds.getName());
*/
          Region region = getCache().createRegion(REGION_NAME, factory.create());

          LRUStatistics lruStats = getLRUStats(region);

          assertNotNull(lruStats);
          
          RegionMXBean bean = managementService.getLocalRegionMBean(REGION_PATH);
          
          assertNotNull(bean);
          
          int total;
          for (total = 0; total < 10000; total++) {
            int[] array = new int[250];
            array[0] = total;
            region.put(new Integer(total), array);
          }
          assertTrue(bean.getEntrySize() > 0);
          getLogWriter().info("DEBUG: EntrySize =" + bean.getEntrySize());
          


        }
      });

    }
    
  }

  public void checkEntrySize(VM vm1, final int expectedMembers)
      throws Exception {
    {
      vm1.invoke(new SerializableRunnable("Check Sub Regions") {

        public void run() {

          DistributedRegionMXBean bean = null;
          try {
            bean = MBeanUtil.getDistributedRegionMbean(REGION_PATH,
                expectedMembers);
          } catch (Exception e) {
            InternalDistributedSystem.getLoggerI18n().fine(
                "Undesired Result , DistributedRegionMXBean Should not be null"
                    + e);
          }

          assertNotNull(bean);

          assertTrue(bean.getEntrySize() > 0);
          getLogWriter().info("DEBUG: EntrySize =" + bean.getEntrySize());
        }
      });

    }

  }
  
  protected LRUStatistics getLRUStats(Region region) {
    final LocalRegion l = (LocalRegion) region;
    return l.getEvictionController().getLRUHelper().getStats();
  }
  
  @SuppressWarnings("serial")
  public void checkSubRegions(VM vm1, final String subRegionPath) throws Exception {
    {
      vm1.invoke(new SerializableRunnable("Check Sub Regions") {

        public void run() {

          RegionMXBean bean = managementService
              .getLocalRegionMBean(subRegionPath);
          assertNotNull(bean);

        }
      });

    }
  }
  
  @SuppressWarnings("serial")
  public void checkNullRegions(VM vm1, final String subRegionPath) throws Exception {
    {
      vm1.invoke(new SerializableRunnable("Check Sub Regions") {

        public void run() {

          RegionMXBean bean = managementService
              .getLocalRegionMBean(subRegionPath);
          assertNull(bean);

        }
      });

    }
  }

  
  
  
  protected void checkNavigationAPIS(final VM vm,
      final List<String> managedNodeMemberIds) {
    SerializableRunnable checkNavigationAPIS = new SerializableRunnable(
        "checkNavigationAPIS") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        ManagementService service = getManagementService();
        final DistributedSystemMXBean bean = service
            .getDistributedSystemMXBean();

        assertNotNull(service.getDistributedSystemMXBean());

        waitForAllMembers(4);
        assertTrue(bean.listDistributedRegionObjectNames().length == 2);
        try {
          assertNotNull(bean
              .fetchDistributedRegionObjectName(PARTITIONED_REGION_PATH));
          assertNotNull(bean.fetchDistributedRegionObjectName(REGION_PATH));
          ObjectName actualName = bean
              .fetchDistributedRegionObjectName(PARTITIONED_REGION_PATH);
          ObjectName expectedName = MBeanJMXAdapter
              .getDistributedRegionMbeanName(PARTITIONED_REGION_PATH);
          assertEquals(expectedName, actualName);

          actualName = bean.fetchDistributedRegionObjectName(REGION_PATH);
          expectedName = MBeanJMXAdapter
              .getDistributedRegionMbeanName(REGION_PATH);
          assertEquals(expectedName, actualName);

        } catch (Exception e) {
          fail("fetchDistributedRegionObjectName () Unsuccessful " + e);
        }

        for (String memberId : managedNodeMemberIds) {
          ObjectName memberMBeanName = MBeanJMXAdapter
              .getMemberMBeanName(memberId);
          ObjectName expectedName;
          try {
            waitForProxy(memberMBeanName, MemberMXBean.class);
            
            ObjectName[] regionMBeanNames = bean
                .fetchRegionObjectNames(memberMBeanName);
            assertNotNull(regionMBeanNames);
            assertTrue(regionMBeanNames.length == 2);
            List<ObjectName> listOfNames = Arrays.asList(regionMBeanNames);

            expectedName = MBeanJMXAdapter.getRegionMBeanName(memberId,
                PARTITIONED_REGION_PATH);
            listOfNames.contains(expectedName);
            expectedName = MBeanJMXAdapter.getRegionMBeanName(memberId,
                REGION_PATH);
            listOfNames.contains(expectedName);
          } catch (Exception e) {
            fail("fetchRegionObjectNames () Unsuccessful " + e);
          }
        }

        for (String memberId : managedNodeMemberIds) {
          ObjectName expectedName;
          ObjectName actualName;
          ObjectName memberMBeanName = MBeanJMXAdapter
          .getMemberMBeanName(memberId);
          try {
            waitForProxy(memberMBeanName, MemberMXBean.class);
            expectedName = MBeanJMXAdapter.getRegionMBeanName(memberId,
                PARTITIONED_REGION_PATH);
            waitForProxy(expectedName, RegionMXBean.class);
            actualName = bean.fetchRegionObjectName(memberId,
                PARTITIONED_REGION_PATH);

            assertEquals(expectedName, actualName);
            expectedName = MBeanJMXAdapter.getRegionMBeanName(memberId,
                REGION_PATH);
            waitForProxy(expectedName, RegionMXBean.class);
            actualName = bean.fetchRegionObjectName(memberId, REGION_PATH);

            assertEquals(expectedName, actualName);
          } catch (Exception e) {
            fail("fetchRegionObjectName () Unsuccessful ");
          }
        }

      }
    };
    vm.invoke(checkNavigationAPIS);
  }
  
  
  protected void putBulkData(final VM vm, final int numKeys) {
    SerializableRunnable putBulkData = new SerializableRunnable("putBulkData") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Region region = cache.getRegion(REGION_PATH);
        for (int i = 0; i < numKeys; i++) {
          region.put(i, i * i);
        }

      }
    };
    vm.invoke(putBulkData);
  }
 
  

  /**
   * creates a Fixed Partition List to be used for Fixed Partition Region
   * 
   * @param primaryIndex
   *          index for each fixed partition
   */
  private static void createFixedPartitionList(int primaryIndex) {
    fpaList.clear();
    if (primaryIndex == 1) {
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 2) {
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 3) {
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));
    }

  }
  


  /**
   * Creates a Fixed Partitioned Region
   * @param fpaList partition list
   */
  protected static void createFixedPartitionRegion(
      List<FixedPartitionAttributes> fpaList) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    SystemManagementService service = (SystemManagementService)getManagementService();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    paf.setRedundantCopies(2).setTotalNumBuckets(12);
    for (FixedPartitionAttributes fpa : fpaList) {
      paf.addFixedPartitionAttributes(fpa);
    }
    paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());

    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    fixedPrRegion = cache.createRegion(FIXED_PR_NAME, attr.create());
    assertNotNull(fixedPrRegion);
    getLogWriter().info(
        "Partitioned Region " + FIXED_PR_NAME + " created Successfully :"
            + fixedPrRegion.toString());

    RegionMXBean bean = service.getLocalRegionMBean(FIXED_PR_PATH);
    RegionAttributes regAttrs = fixedPrRegion.getAttributes();

    getLogWriter().info(
        "FixedPartitionAttribute From GemFire :"
            + regAttrs.getPartitionAttributes().getFixedPartitionAttributes());

    RegionAttributesData data = bean.listRegionAttributes();

    PartitionAttributesData parData = bean.listPartitionAttributes();

    assertPartitionData(regAttrs, parData);

    FixedPartitionAttributesData[] fixedPrData = bean
        .listFixedPartitionAttributes();

    assertNotNull(fixedPrData);

    assertEquals(3, fixedPrData.length);
    for (int i = 0; i < fixedPrData.length; i++) {
      getLogWriter().info(
          "<ExpectedString> Fixed PR Data is " + fixedPrData[i]
              + "</ExpectedString> ");
    }
  }

  /**
   * Verifies the Fixed Partition Region for partition related attributes
   * 
   * @param vm
   */
  protected void validateRemoteFixedPartitionRegion(final VM vm) throws Exception {
    SerializableRunnable verifyFixedRegion = new SerializableRunnable(
        "Verify Partition region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();

        for (DistributedMember member : otherMemberSet) {
          RegionMXBean bean = null;
          try {
            bean = MBeanUtil.getRegionMbeanProxy(member, FIXED_PR_PATH);
          } catch (Exception e) {
            InternalDistributedSystem.getLoggerI18n().fine(
                "Undesired Result , RegionMBean Should not be null");
          }
          PartitionAttributesData data = bean.listPartitionAttributes();
          assertNotNull(data);
          FixedPartitionAttributesData[] fixedPrData = bean
              .listFixedPartitionAttributes();
          assertNotNull(fixedPrData);
          assertEquals(3, fixedPrData.length);
          for (int i = 0; i < fixedPrData.length; i++) {
            getLogWriter().info(
                "<ExpectedString> Remote PR Data is " + fixedPrData[i]
                    + "</ExpectedString> ");
          }
        }

      }

    };
    vm.invoke(verifyFixedRegion);
  }

  /**
   * Add a Notification listener to MemberMBean 
   * @param vm
   */
  protected void addMemberListener(final VM vm) {
    SerializableRunnable addMemberListener = new SerializableRunnable(
        "addMemberListener") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        
        SystemManagementService service = (SystemManagementService) getManagementService();

        Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();
         
        for (DistributedMember member : otherMemberSet) {
          
          MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

          RegionNotif regionCreate = new RegionNotif();

          ObjectName memberMBeanName;
          try {
            memberMBeanName = service.getMemberMBeanName(member);
            Set<ObjectName> names = service.queryMBeanNames(member);
            if(names != null){
              for(ObjectName name : names){
                getLogWriter().info(
                    "<ExpectedString> ObjectNames arr" + name
                        + "</ExpectedString> ");
              }
            }
            waitForProxy(memberMBeanName, MemberMXBean.class);
            mbeanServer.addNotificationListener(memberMBeanName, regionCreate,
                null, null);
          } catch (NullPointerException e) {
            fail("FAILED WITH EXCEPION", e);
          } catch (InstanceNotFoundException e) {
            fail("FAILED WITH EXCEPION", e);
          } catch (Exception e) {
            fail("FAILED WITH EXCEPION", e);
          }

        }

      }
    };
    vm.invoke(addMemberListener);

  }

  /**
   * Add a Notification listener to DistributedSystemMBean which should gather
   * all the notifications which are propagated through all individual
   * MemberMBeans Hence Region created/destroyed should be visible to this
   * listener
   * 
   * @param vm
   */
  protected void addDistrListener(final VM vm) {
    SerializableRunnable addDistrListener = new SerializableRunnable(
        "addDistrListener") {
      public void run() {
        MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

        DistrNotif regionCreate = new DistrNotif();

        ObjectName systemMBeanName;
        try {
          systemMBeanName = MBeanJMXAdapter.getDistributedSystemName();
          mbeanServer.addNotificationListener(systemMBeanName, regionCreate,
              null, null);

        } catch (NullPointerException e) {
          fail("FAILED WITH EXCEPION", e);
        } catch (InstanceNotFoundException e) {
          fail("FAILED WITH EXCEPION", e);

        }

      }
    };
    vm.invoke(addDistrListener);

  }
  
  public void ensureProxyCleanup(final VM vm) {

    SerializableRunnable ensureProxyCleanup = new SerializableRunnable(
        "Ensure Proxy cleanup") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();

        final SystemManagementService service = (SystemManagementService) getManagementService();

        for (final DistributedMember member : otherMemberSet) {
          RegionMXBean bean = null;
          try {

            waitForCriterion(new WaitCriterion() {

              RegionMXBean bean = null;

              public String description() {
                return "Waiting for the proxy to get deleted at managing node";
              }

              public boolean done() {
                ObjectName objectName = service.getRegionMBeanName(member, REGION_PATH);
                bean = service.getMBeanProxy(objectName, RegionMXBean.class);
                boolean done = (bean == null);
                return done;
              }

            }, MAX_WAIT, 500, true);

          } catch (Exception e) {
            fail("could not remove proxies in required time");

          }
          assertNull(bean);

        }

      }
    };
    vm.invoke(ensureProxyCleanup);
  }

  /**
   * Verifies a Remote Distributed Region
   * 
   * @param vm
   */
  protected void verifyRemoteDistributedRegion(final VM vm, final int expectedMembers) throws Exception {
    SerializableRunnable verifyRegion = new SerializableRunnable(
        "Verify Distributed region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();

        for (DistributedMember member : otherMemberSet) {
          RegionMXBean bean = null;
          try {
            bean = MBeanUtil.getRegionMbeanProxy(member, REGION_PATH);
          } catch (Exception e) {
            InternalDistributedSystem.getLoggerI18n().fine(
                "Undesired Result , RegionMBean Should not be null" + e);

          }
          assertNotNull(bean);

          RegionAttributesData data = bean.listRegionAttributes();
          assertNotNull(data);
          MembershipAttributesData membershipData = bean
              .listMembershipAttributes();
          EvictionAttributesData evictionData = bean.listEvictionAttributes();
          assertNotNull(membershipData);
          assertNotNull(evictionData);
          getLogWriter().info(
              "<ExpectedString> Membership Data is "
                  + membershipData.toString() + "</ExpectedString> ");
          getLogWriter().info(
              "<ExpectedString> Eviction Data is " + membershipData.toString()
                  + "</ExpectedString> ");
 
        }
        DistributedRegionMXBean bean = null;
        try {
          bean = MBeanUtil.getDistributedRegionMbean(REGION_PATH, expectedMembers);
        } catch (Exception e) {
          InternalDistributedSystem.getLoggerI18n().fine(
              "Undesired Result , DistributedRegionMXBean Should not be null"
                  + e);
        }

        assertNotNull(bean);
        assertEquals(REGION_PATH, bean.getFullPath());
        

      }
    };
    vm.invoke(verifyRegion);
  }

  
  protected void validateDistributedMBean(final VM vm, final int expectedMembers) {
    SerializableRunnable verifyRegion = new SerializableRunnable(
        "Verify Distributed region") {
      public void run() {
        DistributedRegionMXBean bean = null;
        DistributedSystemMXBean sysMBean = null;
        final ManagementService service = getManagementService();

        if (expectedMembers == 0) {
          try {
            waitForCriterion(new WaitCriterion() {

              RegionMXBean bean = null;

              public String description() {
                return "Waiting for the proxy to get deleted at managing node";
              }

              public boolean done() {
                DistributedRegionMXBean bean = service
                    .getDistributedRegionMXBean(REGION_PATH);
                boolean done = (bean == null);
                return done;
              }

            }, MAX_WAIT, 500, true);

          } catch (Exception e) {
            fail("could not remove Aggregate Bean in required time");

          }
          return;
        }

        try {
          bean = MBeanUtil.getDistributedRegionMbean(REGION_PATH,
              expectedMembers);
          sysMBean = service.getDistributedSystemMXBean();
        } catch (Exception e) {
          InternalDistributedSystem.getLoggerI18n().fine(
              "Undesired Result , DistributedRegionMXBean Should not be null"
                  + e);
        }

        assertNotNull(bean);
        assertEquals(REGION_PATH, bean.getFullPath());
        assertEquals(expectedMembers, bean.getMemberCount());
        assertEquals(expectedMembers, bean.getMembers().length);

        // Check Stats related Data
        // Add Mock testing
        getLogWriter()
            .info(
                "<ExpectedString> CacheListenerCallsAvgLatency is "
                    + bean.getCacheListenerCallsAvgLatency()
                    + "</ExpectedString> ");
        getLogWriter().info(
            "<ExpectedString> CacheWriterCallsAvgLatency is "
                + bean.getCacheWriterCallsAvgLatency() + "</ExpectedString> ");
        getLogWriter().info(
            "<ExpectedString> CreatesRate is " + bean.getCreatesRate()
                + "</ExpectedString> ");

      }
    };
    // Test DistributedRegionMXBean

    vm.invoke(verifyRegion);
  }
  /**
   * Verifies a Remote Partition Region
   * 
   * @param vm
   */
  protected void validateRemotePartitionRegion(final VM vm) throws Exception {
    SerializableRunnable verifyRegion = new SerializableRunnable(
        "Verify Partition region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();

        for (DistributedMember member : otherMemberSet) {
          RegionMXBean bean = null;
          try {
            bean = MBeanUtil.getRegionMbeanProxy(member,
                PARTITIONED_REGION_PATH);
          } catch (Exception e) {
            InternalDistributedSystem.getLoggerI18n().fine(
                "Undesired Result , RegionMBean Should not be null");
          }
          PartitionAttributesData data = bean.listPartitionAttributes();
          assertNotNull(data);
        }
        
        ManagementService service = getManagementService();
        DistributedRegionMXBean bean = service.getDistributedRegionMXBean(PARTITIONED_REGION_PATH);
        assertEquals(3,bean.getMembers().length);

      }

    };
    vm.invoke(verifyRegion);
  }


  
  

  /**
   * Creates a Distributed Region
   * 
   * @param vm
   */
  protected void validateReplicateRegionAfterCreate(final VM vm) {
    SerializableRunnable checkDistributedRegion = new SerializableRunnable(
        "Check Distributed region") {
      public void run() {

        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        SystemManagementService service = (SystemManagementService)getManagementService();

        MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
        RegionNotif test = new RegionNotif();

        String memberId = MBeanJMXAdapter.getMemberNameOrId(cache
            .getDistributedSystem().getDistributedMember());

        ObjectName memberMBeanName;
        try {
          memberMBeanName = ObjectName
              .getInstance("GemFire:type=Member,member=" + memberId);
          mbeanServer
              .addNotificationListener(memberMBeanName, test, null, null);
        } catch (MalformedObjectNameException e) {

          fail("FAILED WITH EXCEPION", e);
        } catch (NullPointerException e) {
          fail("FAILED WITH EXCEPION", e);

        } catch (InstanceNotFoundException e) {
          fail("FAILED WITH EXCEPION", e);

        }

        assertNotNull(service.getLocalRegionMBean(REGION_PATH));

        RegionMXBean bean = service.getLocalRegionMBean(REGION_PATH);
        Region region = cache.getRegion(REGION_PATH);

        RegionAttributes regAttrs = region.getAttributes();

        RegionAttributesData data = bean.listRegionAttributes();

        assertRegionAttributes(regAttrs, data);
        MembershipAttributesData membershipData = bean
            .listMembershipAttributes();
        EvictionAttributesData evictionData = bean.listEvictionAttributes();
        assertNotNull(membershipData);
        assertNotNull(evictionData);
        getLogWriter().info(
            "<ExpectedString> Membership Data is " + membershipData.toString()
                + "</ExpectedString> ");
        getLogWriter().info(
            "<ExpectedString> Eviction Data is " + membershipData.toString()
                + "</ExpectedString> ");
      }
    };
    vm.invoke(checkDistributedRegion);
  }

  /**
   * Creates a partition Region
   * 
   * @param vm
   */
  protected void validatePartitionRegionAfterCreate(final VM vm) {
    SerializableRunnable createParRegion = new SerializableRunnable(
        "Create Partitioned region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        SystemManagementService service = (SystemManagementService)getManagementService();
        assertNotNull(service.getLocalRegionMBean(PARTITIONED_REGION_PATH));
        RegionMXBean bean = service
            .getLocalRegionMBean(PARTITIONED_REGION_PATH);
        Region partitionedRegion = cache.getRegion(PARTITIONED_REGION_PATH);
        RegionAttributes regAttrs = partitionedRegion.getAttributes();
        RegionAttributesData data = bean.listRegionAttributes();
        PartitionAttributesData parData = bean.listPartitionAttributes();
        assertPartitionData(regAttrs, parData);
        
      }
    };
    vm.invoke(createParRegion);
  }

  /**
   * closes a Distributed Region
   * 
   * @param vm
   */
  protected void validateReplicatedRegionAfterClose(final VM vm) {
    SerializableRunnable closeRegion = new SerializableRunnable(
        "Close Distributed region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        SystemManagementService service = (SystemManagementService)getManagementService();
        RegionMXBean bean = null;
        try {
          bean = service.getLocalRegionMBean(REGION_PATH);
        } catch (ManagementException mgtEx) {
          getLogWriter().info(
              "<ExpectedString> Expected Exception  "
                  + mgtEx.getLocalizedMessage() + "</ExpectedString> ");
        }
        assertNull(bean);
        ObjectName regionObjectName = service.getRegionMBeanName(cache
            .getDistributedSystem().getDistributedMember(), REGION_PATH);
        assertNull(service.getLocalManager().getManagementResourceRepo()
            .getEntryFromLocalMonitoringRegion(regionObjectName));
      }
    };
    vm.invoke(closeRegion);
  }

  /**
   * close a partition Region
   * 
   * @param vm
   */
  protected void validatePartitionRegionAfterClose(final VM vm) {
    SerializableRunnable closeParRegion = new SerializableRunnable(
        "Close Partition region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        ManagementService service = getManagementService();
        getLogWriter().info("Closing Par Region");
        RegionMXBean bean = null;
        try {
          bean = service.getLocalRegionMBean(PARTITIONED_REGION_PATH);
        } catch (ManagementException mgtEx) {
          getLogWriter().info(
              "<ExpectedString> Expected Exception  "
                  + mgtEx.getLocalizedMessage() + "</ExpectedString> ");
        }
        assertNull(bean);
      }
    };
    vm.invoke(closeParRegion);
  }

  /**
   * Closes Fixed Partition region
   * 
   * @param vm
   */
  protected void closeFixedPartitionRegion(final VM vm) {
    SerializableRunnable closeParRegion = new SerializableRunnable(
        "Close Fixed Partition region") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        ManagementService service = getManagementService();
        getLogWriter().info("Closing Fixed Par Region");
        Region region = cache.getRegion(FIXED_PR_PATH);
        region.close();
        RegionMXBean bean = null;
        try {
          bean = service.getLocalRegionMBean(FIXED_PR_PATH);
        } catch (ManagementException mgtEx) {
          getLogWriter().info(
              "<ExpectedString> Expected Exception  "
                  + mgtEx.getLocalizedMessage() + "</ExpectedString> ");
        }
        assertNull(bean);
      }
    };
    vm.invoke(closeParRegion);
  }

  /**
   * Asserts and verifies all the partition related data
   * 
   * @param regAttrs
   * @param partitionAttributesData
   */

  protected static void assertPartitionData(RegionAttributes regAttrs,
      PartitionAttributesData partitionAttributesData) {
    PartitionAttributesData data = partitionAttributesData;

    PartitionAttributes partAttrs = regAttrs.getPartitionAttributes();

    int redundantCopies = partAttrs.getRedundantCopies();
    assertEquals(redundantCopies, data.getRedundantCopies());
    long totalMaxMemory = partAttrs.getTotalMaxMemory();
    assertEquals(totalMaxMemory, data.getTotalMaxMemory());
    // Total number of buckets for whole region
    int totalNumBuckets = partAttrs.getTotalNumBuckets();
    assertEquals(totalNumBuckets, data.getTotalNumBuckets());

    int localMaxMemory = partAttrs.getLocalMaxMemory();
    assertEquals(localMaxMemory, data.getLocalMaxMemory());

    String colocatedWith = partAttrs.getColocatedWith();
    assertEquals(colocatedWith, data.getColocatedWith());

    String partitionResolver = null;
    if (partAttrs.getPartitionResolver() != null) {
      partitionResolver = partAttrs.getPartitionResolver().getName();
    }

    assertEquals(partitionResolver, data.getPartitionResolver());

    long recoveryDelay = partAttrs.getRecoveryDelay();
    assertEquals(recoveryDelay, data.getRecoveryDelay());

    long startupRecoveryDelay = partAttrs.getStartupRecoveryDelay();
    assertEquals(startupRecoveryDelay, data.getStartupRecoveryDelay());

    if (partAttrs.getPartitionListeners() != null) {
      for (int i = 0; i < partAttrs.getPartitionListeners().length; i++) {
        assertEquals((partAttrs.getPartitionListeners())[i].getClass()
            .getCanonicalName(), data.getPartitionListeners()[i]);
      }

    }

  }

  /**
   * Checks all Region Attributes
   * 
   * @param regAttrs
   * @param data
   */
  protected static void assertRegionAttributes(RegionAttributes regAttrs,
      RegionAttributesData data) {

    String compressorClassName = null;
    if (regAttrs.getCompressor() != null) {
      compressorClassName = regAttrs.getCompressor().getClass()
          .getCanonicalName();
    }
    assertEquals(compressorClassName, data.getCompressorClassName());
    String cacheLoaderClassName = null;
    if (regAttrs.getCacheLoader() != null) {
      cacheLoaderClassName = regAttrs.getCacheLoader().getClass()
          .getCanonicalName();
    }
    assertEquals(cacheLoaderClassName, data.getCacheLoaderClassName());
    String cacheWriteClassName = null;
    if (regAttrs.getCacheWriter() != null) {
      cacheWriteClassName = regAttrs.getCacheWriter().getClass()
          .getCanonicalName();
    }
    assertEquals(cacheWriteClassName, data.getCacheWriterClassName());
    String keyConstraintClassName = null;
    if (regAttrs.getKeyConstraint() != null) {
      keyConstraintClassName = regAttrs.getKeyConstraint().getName();
    }
    assertEquals(keyConstraintClassName, data.getKeyConstraintClassName());
    String valueContstaintClassName = null;
    if (regAttrs.getValueConstraint() != null) {
      valueContstaintClassName = regAttrs.getValueConstraint().getName();
    }
    assertEquals(valueContstaintClassName, data.getValueConstraintClassName());
    CacheListener[] listeners = regAttrs.getCacheListeners();
    

    if (listeners != null) {
      String[] value = data.getCacheListeners();
      for (int i = 0; i < listeners.length; i++) {
        assertEquals(value[i], listeners[i].getClass().getName());
      }
      
    }
    
 
    

    int regionTimeToLive = regAttrs.getRegionTimeToLive().getTimeout();

    assertEquals(regionTimeToLive, data.getRegionTimeToLive());

    int regionIdleTimeout = regAttrs.getRegionIdleTimeout().getTimeout();

    assertEquals(regionIdleTimeout, data.getRegionIdleTimeout());

    int entryTimeToLive = regAttrs.getEntryTimeToLive().getTimeout();

    assertEquals(entryTimeToLive, data.getEntryTimeToLive());

    int entryIdleTimeout = regAttrs.getEntryIdleTimeout().getTimeout();

    assertEquals(entryIdleTimeout, data.getEntryIdleTimeout());
    String customEntryTimeToLive = null;
    Object o1 = regAttrs.getCustomEntryTimeToLive();
    if (o1 != null) {
      customEntryTimeToLive = o1.toString();
    }
    assertEquals(customEntryTimeToLive, data.getCustomEntryTimeToLive());

    String customEntryIdleTimeout = null;
    Object o2 = regAttrs.getCustomEntryIdleTimeout();
    if (o2 != null) {
      customEntryIdleTimeout = o2.toString();
    }
    assertEquals(customEntryIdleTimeout, data.getCustomEntryIdleTimeout());

    boolean ignoreJTA = regAttrs.getIgnoreJTA();
    assertEquals(ignoreJTA, data.isIgnoreJTA());

    String dataPolicy = regAttrs.getDataPolicy().toString();
    assertEquals(dataPolicy, data.getDataPolicy());

    String scope = regAttrs.getScope().toString();
    assertEquals(scope, data.getScope());

    int initialCapacity = regAttrs.getInitialCapacity();
    assertEquals(initialCapacity, data.getInitialCapacity());
    float loadFactor = regAttrs.getLoadFactor();
    assertEquals(loadFactor, data.getLoadFactor());

    boolean lockGrantor = regAttrs.isLockGrantor();
    assertEquals(lockGrantor, data.isLockGrantor());

    boolean multicastEnabled = regAttrs.getMulticastEnabled();
    assertEquals(multicastEnabled, data.isMulticastEnabled());

    int concurrencyLevel = regAttrs.getConcurrencyLevel();
    assertEquals(concurrencyLevel, data.getConcurrencyLevel());

    boolean indexMaintenanceSynchronous = regAttrs
        .getIndexMaintenanceSynchronous();
    assertEquals(indexMaintenanceSynchronous, data
        .isIndexMaintenanceSynchronous());

    boolean statisticsEnabled = regAttrs.getStatisticsEnabled();

    assertEquals(statisticsEnabled, data.isStatisticsEnabled());

    boolean subsciptionConflationEnabled = regAttrs
        .getEnableSubscriptionConflation();
    assertEquals(subsciptionConflationEnabled, data
        .isSubscriptionConflationEnabled());

    boolean asyncConflationEnabled = regAttrs.getEnableAsyncConflation();
    assertEquals(asyncConflationEnabled, data.isAsyncConflationEnabled());

    String poolName = regAttrs.getPoolName();
    assertEquals(poolName, data.getPoolName());

    boolean isCloningEnabled = regAttrs.getCloningEnabled();
    assertEquals(isCloningEnabled, data.isCloningEnabled());

    String diskStoreName = regAttrs.getDiskStoreName();
    assertEquals(diskStoreName, data.getDiskStoreName());

    String interestPolicy = null;
    if (regAttrs.getSubscriptionAttributes() != null) {
      interestPolicy = regAttrs.getSubscriptionAttributes().getInterestPolicy()
          .toString();
    }
    assertEquals(interestPolicy, data.getInterestPolicy());
    boolean diskSynchronus = regAttrs.isDiskSynchronous();
    assertEquals(diskSynchronus, data.isDiskSynchronous());
  }

  /**
   * Verifies Region related Statistics
   */
  public void verifyStatistics() {

  }


  /**
   * User defined notification handler for Region creation handling
   * 
   * @author rishim
   * 
   */
  private static class RegionNotif implements NotificationListener {

    @Override
    public void handleNotification(Notification notification, Object handback) {
      assertNotNull(notification);
      Notification rn =  notification;
      assertTrue(rn.getType().equals(JMXNotificationType.REGION_CREATED)
          || rn.getType().equals(JMXNotificationType.REGION_CLOSED));
      getLogWriter().info(
          "<ExpectedString> Member Level Notifications" + rn.toString()
              + "</ExpectedString> ");
    }

  }

  /**
   * User defined notification handler for Region creation handling
   * 
   * @author rishim
   * 
   */
  private static class DistrNotif implements NotificationListener {

    @Override
    public void handleNotification(Notification notification, Object handback) {
      assertNotNull(notification);
      Notification rn = notification;
      getLogWriter().info(
          "<ExpectedString> Distributed System Notifications" + rn.toString()
              + "</ExpectedString> ");
    }

  }

}
