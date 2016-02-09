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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.NotificationHub.NotificationHubListener;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

import static com.jayway.awaitility.Awaitility.*;
import static org.hamcrest.Matchers.*;

/**
 * This class checks and verifies various data and operations exposed through
 * MemberMXBean interface.
 * 
 * Goal of the Test : MemberMBean gets created once cache is created. Data like
 * config data and stats are of proper value To check proper federation of
 * MemberMBean including remote ops and remote data access
 * 
 * @author rishim
 * 
 */
public class CacheManagementDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  private final String VERIFY_CONFIG_METHOD = "verifyConfigData";

  private final String VERIFY_REMOTE_CONFIG_METHOD = "verifyConfigDataRemote";

  static final String REGION_NAME = "MANAGEMENT_TEST_REGION";

  static final String PARTITIONED_REGION_NAME = "MANAGEMENT_PAR_REGION";

  static final String FIXED_PR_NAME = "MANAGEMENT_FIXED_PR";

  static List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();


  static final List<Notification> notifList = new ArrayList<Notification>();
  
  // This must be bigger than the dunit ack-wait-threshold for the revoke
  // tests. The command line is setting the ack-wait-threshold to be
  // 60 seconds.
  private static final int MAX_WAIT = 70 * 1000;



  public CacheManagementDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();  

  }

  public void testGemFireConfigData() throws Exception {
     initManagement(false);
   
    Map<DistributedMember, DistributionConfig> configMap = new HashMap<DistributedMember, DistributionConfig>();
    for (VM vm : getManagedNodeList()) {
      Map<DistributedMember, DistributionConfig> configMapMember = (Map<DistributedMember, DistributionConfig>) vm
          .invoke(CacheManagementDUnitTest.class, VERIFY_CONFIG_METHOD);
      configMap.putAll(configMapMember);
    }

    Object[] args = new Object[1];
    args[0] = configMap;
    getManagingNode().invoke(
        CacheManagementDUnitTest.class, VERIFY_REMOTE_CONFIG_METHOD, args);
 
  }

  /**
   * Tests each and every operations that is defined on the MemberMXBean
   * 
   * @throws Exception
   */
  public void testMemberMBeanOperations() throws Exception {
    
    initManagement(false);
    
    for (VM vm : managedNodeList) {
            
      //Do some operations to fill the logs
      
      createLocalRegion(vm,"testRegion");



      String log = (String) vm.invoke(CacheManagementDUnitTest.class,
          "fetchLog");
      assertNotNull(log);
      LogWriterUtils.getLogWriter().info(
          "<ExpectedString> Log Of Member is " + log.toString()
              + "</ExpectedString> ");

     

      vm.invoke(CacheManagementDUnitTest.class, "fetchJVMMetrics");

      vm.invoke(CacheManagementDUnitTest.class, "fetchOSMetrics");

      vm.invoke(CacheManagementDUnitTest.class, "shutDownMember");

    }
    
    VM managingNode = getManagingNode();
    Object[] args = new Object[1];
    args[0] = 1;// Only locator member wont be shutdown
    managingNode.invoke(CacheManagementDUnitTest.class,
        "assertExpectedMembers", args);
    
  }

  /**
   * Invoke remote operations on MemberMBean
   * @throws Exception
   */
  public void testMemberMBeanOpsRemote() throws Exception {
    initManagement(false);
    getManagingNode().invoke(
        CacheManagementDUnitTest.class, "invokeRemoteOps");
   }

  /**
   * Creates and starts a manager.
   * Multiple Managers
   * 
   * @throws Exception
   */
  public void testManager() throws Exception {
    List<VM> managedNodeList = getManagedNodeList();
    VM node1 = managedNodeList.get(0);
    VM node2 = managedNodeList.get(1);
    VM node3 = managedNodeList.get(2);
    VM managingNode = getManagingNode();

    createCache(node1);
    createCache(node2);
    createManagementCache(node3);

    // Only creates a cache in Managing Node
    // Does not start the manager
    createManagementCache(managingNode);
    
    node3.invoke(CacheManagementDUnitTest.class, "startManager");

    // Now start Managing node manager. System will have two Managers now which
    // should be OK
    DistributedMember member = getMember(node3);
    startManagingNode(managingNode);
    checkManagerView(managingNode, member);
    stopManagingNode(managingNode);

  }
  
  /**
   * Creates and starts a manager.
   * Multiple Managers
   * 
   * @throws Exception
   */
  public void testManagerShutdown() throws Exception {
    List<VM> managedNodeList = getManagedNodeList();
    VM node1 = managedNodeList.get(0);
    VM node2 = managedNodeList.get(1);
    VM node3 = managedNodeList.get(2);
    VM managingNode = getManagingNode();

    createCache(node1);
    createCache(node2);
    createCache(node3);

    // Only creates a cache in Managing Node
    // Does not start the manager
    createManagementCache(managingNode);
    
    startManagingNode(managingNode);
    DistributedMember member = getMember(managingNode);
    checkManagerView(managingNode, member);
    stopManagingNode(managingNode);
    checkNonManagerView(managingNode);
  }
  
  public void testServiceCloseManagedNode() throws Exception{
    List<VM> managedNodeList = getManagedNodeList();
    VM node1 = managedNodeList.get(0);
    VM node2 = managedNodeList.get(1);
    VM node3 = managedNodeList.get(2);
    VM managingNode = getManagingNode();

    createCache(node1);
    createCache(node2);
    createManagementCache(node3);

    // Only creates a cache in Managing Node
    // Does not start the manager
    createManagementCache(managingNode);
    
    node3.invoke(CacheManagementDUnitTest.class, "startManager");
    
    closeCache(node3);
    validateServiceResource(node3);

  }
  
  public void testGetMBean() throws Exception{
    List<VM> managedNodeList = getManagedNodeList();
    VM node1 = managedNodeList.get(0);
    VM node2 = managedNodeList.get(1);
    VM node3 = managedNodeList.get(2);
    VM managingNode = getManagingNode();

    createCache(node1);
    createCache(node2);
    createCache(node3);

    createManagementCache(managingNode);
    
    startManagingNode(managingNode);

    checkGetMBean(managingNode);

  }
  
  public void testQueryMBeans() throws Exception{
    List<VM> managedNodeList = getManagedNodeList();
    VM node1 = managedNodeList.get(0);
    VM node2 = managedNodeList.get(1);
    VM node3 = managedNodeList.get(2);
    VM managingNode = getManagingNode();

    createCache(node1);
    createCache(node2);
    createCache(node3);

    createManagementCache(managingNode);
    
    startManagingNode(managingNode);

    checkQueryMBeans(managingNode);

  }

  protected void checkQueryMBeans(final VM vm) {
    SerializableRunnable validateServiceResource = new SerializableRunnable(
        "Check Query MBeans") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

        Set<DistributedMember> otherMembers = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();
        
        Set<ObjectName> superSet = new HashSet<ObjectName>();

        for (DistributedMember member : otherMembers) {

          ObjectName memberMBeanName = managementService
              .getMemberMBeanName(member);

          waitForProxy(memberMBeanName, MemberMXBean.class);
          Set<ObjectName> names = managementService.queryMBeanNames(member);
          superSet.addAll(names);
          assertTrue(names.contains(memberMBeanName));

        }
        
        
        Set<ObjectName> names = managementService.queryMBeanNames(cache
            .getDistributedSystem().getDistributedMember());
        assertTrue(!superSet.contains(names));
         

      }
    };
    vm.invoke(validateServiceResource);

  }

  protected void checkGetMBean(final VM vm) {
    SerializableRunnable validateServiceResource = new SerializableRunnable(
        "Check Get MBean") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Set<DistributedMember> otherMembers = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();

        for (DistributedMember member : otherMembers) {
          
          ObjectName memberMBeanName = managementService
              .getMemberMBeanName(member);
          
          waitForProxy(memberMBeanName, MemberMXBean.class);
          
          MemberMXBean bean = managementService.getMBeanInstance(memberMBeanName,
              MemberMXBean.class);
          assertNotNull(bean);
        }

        DistributedMember thisMember = cache.getDistributedSystem()
            .getDistributedMember();
        ObjectName memberMBeanName = managementService
            .getMemberMBeanName(thisMember);
        MemberMXBean bean = managementService.getMBeanInstance(memberMBeanName,
            MemberMXBean.class);
        assertNotNull(bean);

      }
    };
    vm.invoke(validateServiceResource);

  }

  protected void validateServiceResource(final VM vm) {
    SerializableRunnable validateServiceResource = new SerializableRunnable(
        "Valideate Management Service Resource") {
      public void run() {

        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        assertNull(cache);
        assertFalse(managementService.isManager());
        
        SystemManagementService service = (SystemManagementService)managementService;
        assertNull(service.getLocalManager());
        
        

      }
    };
    vm.invoke(validateServiceResource);

  }
  
  /**
   * Creates a Distributed Region
   * 
   * @param vm
   */
  protected AsyncInvocation checkManagerView(final VM vm,
      final DistributedMember oneManager) {
    SerializableRunnable createRegion = new SerializableRunnable(
        "Check Manager View") {
      public void run() {

        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        SystemManagementService service = (SystemManagementService) getManagementService();
        ObjectName memberMBeanName = service.getMemberMBeanName(oneManager);
        MemberMXBean bean = service.getMBeanProxy(memberMBeanName, MemberMXBean.class);
        assertNotNull(bean);
        //Ensure Data getting federated from Managing node
        long t1 = bean.getMemberUpTime();
        try {
          this.wait(ManagementConstants.REFRESH_TIME *3);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
        long t2 = bean.getMemberUpTime();
        
        assertTrue(t2>t1);
        
      }
    };
    return vm.invokeAsync(createRegion);
  }
  
  /**
   * Add any Manager clean up asserts here
   * 
   * @param vm
   */

  protected void checkNonManagerView(final VM vm) {
    SerializableRunnable checkNonManagerView = new SerializableRunnable(
        "Check Non Manager View") {
      public void run() {

        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        assertNotNull(cache);
        assertFalse(managementService.isManager());

        SystemManagementService service = (SystemManagementService) managementService;
        assertTrue(service.getLocalManager().isRunning());
        assertFalse(service.getLocalManager().getFederationSheduler()
            .isShutdown());

        // Check for Proxies
        Set<DistributedMember> otherMembers = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();
        assertTrue(otherMembers.size() > 0);
        for (DistributedMember member : otherMembers) {
          Set<ObjectName> proxyNames = service.getFederatingManager()
              .getProxyFactory().findAllProxies(member);
          assertTrue(proxyNames.isEmpty());
          ObjectName proxyMBeanName = service.getMemberMBeanName(member);
          assertFalse(MBeanJMXAdapter.mbeanServer.isRegistered(proxyMBeanName));
        }

      }
    };
    vm.invoke(checkNonManagerView);

  }
  

  public static Map<DistributedMember, DistributionConfig> verifyConfigData() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    ManagementService service = getManagementService();
    DistributionConfig config = ((InternalDistributedSystem) cache
        .getDistributedSystem()).getConfig();
    MemberMXBean bean = service.getMemberMXBean();
    GemFireProperties data = bean.listGemFireProperties();
    assertEquals(config, data);
    Map<DistributedMember, DistributionConfig> configMap = new HashMap<DistributedMember, DistributionConfig>();
    configMap.put(cache.getMyId(), config);
    return configMap;
  }

  /**
   * This is to check whether the config data has been propagated to the
   * Managing node properly or not.
   */
  public static void verifyConfigDataRemote(
      Map<DistributedMember, DistributionConfig> configMap) throws Exception {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
        .getOtherNormalDistributionManagerIds();

    for (DistributedMember member : otherMemberSet) {
      MemberMXBean bean = MBeanUtil.getMemberMbeanProxy(member);
      GemFireProperties data = bean.listGemFireProperties();
      DistributionConfig config = configMap.get(member);
      assertEquals(config, data);

    }

  }

  /**
   * Asserts that distribution config and gemfireProperty composite types hold
   * the same values
   */
  public static void assertEquals(DistributionConfig config,
      GemFireProperties data) {

    assertEquals(data.getMemberName(), config.getName());
    // **TODO **/
    String memberGroups = null;

    assertEquals(data.getMcastPort(), config.getMcastPort());
    assertEquals(data.getMcastAddress(), config.getMcastAddress()
        .getHostAddress());
    assertEquals(data.getBindAddress(), config.getBindAddress());
    assertEquals(data.getTcpPort(), config.getTcpPort());
    assertEquals(removeVMDir(data.getCacheXMLFile()), removeVMDir(config.getCacheXmlFile()
        .getAbsolutePath()));
    // **TODO **/
    String configFile = null;
    String configFile1 = null;
    assertEquals(data.getMcastTTL(), config.getMcastTtl());
    assertEquals(data.getServerBindAddress(), config.getServerBindAddress());
    assertEquals(data.getLocators(), config.getLocators());
    //The start locator may contain a directory
    assertEquals(removeVMDir(data.getStartLocator()), removeVMDir(config.getStartLocator()));
    assertEquals(removeVMDir(data.getLogFile()), removeVMDir(config.getLogFile().getAbsolutePath()));
    assertEquals(data.getLogLevel(), config.getLogLevel());
    assertEquals(data.isStatisticSamplingEnabled(), config
        .getStatisticSamplingEnabled());
    assertEquals(removeVMDir(data.getStatisticArchiveFile()), removeVMDir(config
        .getStatisticArchiveFile().getAbsolutePath()));
    // ** TODO **//
    String includeFile = null;
    assertEquals(data.getAckWaitThreshold(), config.getAckWaitThreshold());
    assertEquals(data.getAckSevereAlertThreshold(), config
        .getAckSevereAlertThreshold());
    assertEquals(data.getArchiveFileSizeLimit(), config
        .getArchiveFileSizeLimit());
    assertEquals(data.getArchiveDiskSpaceLimit(), config
        .getArchiveDiskSpaceLimit());
    assertEquals(data.getLogFileSizeLimit(), config.getLogFileSizeLimit());
    assertEquals(data.getLogDiskSpaceLimit(), config.getLogDiskSpaceLimit());
    assertEquals(data.isSslEnabled(), config.getSSLEnabled());
    assertEquals(data.getSslCiphers(), config.getSSLCiphers());
    assertEquals(data.getSslProtocols(), config.getSSLProtocols());
    assertEquals(data.isSslRequireAuthentication(), config
        .getSSLRequireAuthentication());
    assertEquals(data.getSocketLeaseTime(), config.getSocketLeaseTime());
    assertEquals(data.getSocketBufferSize(), config.getSocketBufferSize());
    assertEquals(data.getMcastSendBufferSize(), config.getMcastSendBufferSize());
    assertEquals(data.getMcastRecvBufferSize(), config.getMcastRecvBufferSize());
    assertEquals(data.getMcastByteAllowance(), config.getMcastFlowControl()
        .getByteAllowance());
    assertEquals(data.getMcastRechargeThreshold(), config.getMcastFlowControl()
        .getRechargeThreshold());
    assertEquals(data.getMcastRechargeBlockMs(), config.getMcastFlowControl()
        .getRechargeBlockMs());
    assertEquals(data.getUdpFragmentSize(), config.getUdpFragmentSize());
    assertEquals(data.getUdpSendBufferSize(), config.getUdpSendBufferSize());
    assertEquals(data.getUdpRecvBufferSize(), config.getUdpRecvBufferSize());
    assertEquals(data.isDisableTcp(), config.getDisableTcp());
    assertEquals(data.isEnableTimeStatistics(), config
        .getEnableTimeStatistics());
    assertEquals(data.isEnableNetworkPartitionDetection(), config
        .getEnableNetworkPartitionDetection());
    assertEquals(data.getMemberTimeout(), config.getMemberTimeout());

    int[] configPortRange = config.getMembershipPortRange();
    int[] dataPortRange = data.getMembershipPortRange();

    assertEquals(dataPortRange.length, configPortRange.length);
    for (int i = 0; i < dataPortRange.length; i++) {
      assertEquals(dataPortRange[i], configPortRange[i]);
    }
    assertEquals(data.isConserveSockets(), config.getConserveSockets());
    assertEquals(data.getRoles(), config.getRoles());
    assertEquals(data.getMaxWaitTimeForReconnect(), config
        .getMaxWaitTimeForReconnect());
    assertEquals(data.getMaxNumReconnectTries(), config
        .getMaxNumReconnectTries());
    assertEquals(data.getAsyncDistributionTimeout(), config
        .getAsyncDistributionTimeout());
    assertEquals(data.getAsyncQueueTimeout(), config.getAsyncQueueTimeout());
    assertEquals(data.getAsyncMaxQueueSize(), config.getAsyncMaxQueueSize());
    assertEquals(data.getClientConflation(), config.getClientConflation());
    assertEquals(data.getDurableClientId(), config.getDurableClientId());
    assertEquals(data.getDurableClientTimeout(), config
        .getDurableClientTimeout());
    assertEquals(data.getSecurityClientAuthInit(), config
        .getSecurityClientAuthInit());
    assertEquals(data.getSecurityClientAuthenticator(), config
        .getSecurityClientAuthenticator());
    assertEquals(data.getSecurityClientDHAlgo(), config
        .getSecurityClientDHAlgo());
    assertEquals(data.getSecurityPeerAuthInit(), config
        .getSecurityPeerAuthInit());
    assertEquals(data.getSecurityClientAuthenticator(), config
        .getSecurityPeerAuthenticator());
    assertEquals(data.getSecurityClientAccessor(), config
        .getSecurityClientAccessor());
    assertEquals(data.getSecurityClientAccessorPP(), config
        .getSecurityClientAccessorPP());
    assertEquals(data.getSecurityLogLevel(), config.getSecurityLogLevel());
    assertEquals(removeVMDir(data.getSecurityLogFile()), removeVMDir(config.getSecurityLogFile()
        .getAbsolutePath()));
    assertEquals(data.getSecurityPeerMembershipTimeout(), config
        .getSecurityPeerMembershipTimeout());
    assertEquals(data.isRemoveUnresponsiveClient(), config
        .getRemoveUnresponsiveClient());
    assertEquals(data.isDeltaPropagation(), config.getDeltaPropagation());
    assertEquals(data.getRedundancyZone(), config.getRedundancyZone());
    assertEquals(data.isEnforceUniqueHost(), config.getEnforceUniqueHost());
    assertEquals(data.getStatisticSampleRate(), config.getStatisticSampleRate());
  }

  
  private static String removeVMDir(String string) {
    return string.replaceAll("vm.", "");
  }

  public static void startManager() {
    MemberMXBean bean = getManagementService()
    .getMemberMXBean();
    // When the cache is created if jmx-manager is true then we create the manager.
    // So it may already exist when we get here.
    if (!bean.isManagerCreated()) {
      if (!bean.createManager()) {
        fail("Could not create Manager");
      } else if (!bean.isManagerCreated()) {
        fail("Should have been a manager after createManager returned true.");
      }
    }
    ManagerMXBean mngrBean = getManagementService().getManagerMXBean();
    try {
      mngrBean.start();
    } catch (JMException e) {
      fail("Could not start Manager " + e);
    }
    assertTrue(mngrBean.isRunning());
    assertTrue(getManagementService().isManager());
    assertTrue(bean.isManager());
  }
  
  public static void isManager(){
    MemberMXBean bean = getManagementService()
    .getMemberMXBean();
    if(bean.createManager()){
      ManagerMXBean mngrBean = getManagementService().getManagerMXBean();
      try {
        mngrBean.start();
      } catch (JMException e) {
        fail("Could not start Manager " + e);
      }
    }else{
      fail(" Could not create Manager");
    }
  }

  public static String fetchLog() {
    MemberMXBean bean = getManagementService()
        .getMemberMXBean();
    String log = bean.showLog(30);
    return log;
  }

  public static void fetchJVMMetrics() {
    MemberMXBean bean = getManagementService()
        .getMemberMXBean();
    JVMMetrics metrics = bean.showJVMMetrics();

    LogWriterUtils.getLogWriter().info(
        "<ExpectedString> JVMMetrics is " + metrics.toString()
            + "</ExpectedString> ");

  }

  public static void fetchOSMetrics() {
    MemberMXBean bean = getManagementService()
        .getMemberMXBean();
    OSMetrics metrics = bean.showOSMetrics();

    LogWriterUtils.getLogWriter().info(
        "<ExpectedString> OSMetrics is " + metrics.toString()
            + "</ExpectedString> ");

  }

  public static void shutDownMember() {
    MemberMXBean bean = getManagementService()
        .getMemberMXBean();
    bean.shutDownMember();

  }

  public static void assertExpectedMembers(int expectedMemberCount) {
    Wait.waitForCriterion(new WaitCriterion() {
      public String description() {
        return "Waiting all nodes to shutDown";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Set<DistributedMember> setOfOtherMembers = cache.getDistributedSystem()
            .getAllOtherMembers();
        boolean done = (setOfOtherMembers != null && setOfOtherMembers.size() == 1);
        return done;
      }

    }, MAX_WAIT, 500, true);

  }


  public static void invokeRemoteOps() throws Exception {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
        .getOtherNormalDistributionManagerIds();

    for (DistributedMember member : otherMemberSet) {
      MemberMXBean bean = MBeanUtil.getMemberMbeanProxy(member);
      JVMMetrics metrics = bean.showJVMMetrics();

      LogWriterUtils.getLogWriter().info(
          "<ExpectedString> JVMMetrics is " + metrics.toString()
              + "</ExpectedString> ");
      LogWriterUtils.getLogWriter().info(
          "<ExpectedString> OSMetrics is " + metrics.toString()
              + "</ExpectedString> ");
      
      LogWriterUtils.getLogWriter().info(
          "<ExpectedString> Boolean Data Check " +bean.isManager()
              + "</ExpectedString> ");
      
    }
  }

  public void verifyStatistics() {

  }

  public void invokeOperations() {

  }
  
  
  public void testNotification() throws Exception {

    List<VM> managedNodeList = getManagedNodeList();
    VM node1 = managedNodeList.get(0);
    VM node2 = managedNodeList.get(1);
    VM node3 = managedNodeList.get(2);
    VM managingNode = getManagingNode();

    // Step : 1 : Create Managed Node Caches
    createCache(node1);
    createCache(node2);
    createCache(node3);

    // Step : 2 : Create Managing Node Cache, start manager, add a notification
    // handler to DistributedSystemMXBean
    createManagementCache(managingNode);
    startManagingNode(managingNode);
    attchListenerToDSMBean(managingNode);

    // Step : 3 : Verify Notification count, notification region sizes
    countNotificationsAndCheckRegionSize(node1, node2, node3, managingNode);

  }

  public void testNotificationManagingNodeFirst() throws Exception {

    List<VM> managedNodeList = getManagedNodeList();
    VM node1 = managedNodeList.get(0);
    VM node2 = managedNodeList.get(1);
    VM node3 = managedNodeList.get(2);
    VM managingNode = getManagingNode();

    // Step : 1 : Create Managing Node Cache, start manager, add a notification
    // handler to DistributedSystemMXBean
    createManagementCache(managingNode);
    startManagingNode(managingNode);
    attchListenerToDSMBean(managingNode);

    // Step : 2 : Create Managed Node Caches
    createCache(node1);
    createCache(node2);
    createCache(node3);

    // Step : 3 : Verify Notification count, notification region sizes
    countNotificationsAndCheckRegionSize(node1, node2, node3, managingNode);

  }
  
  public void testRedundancyZone() throws Exception {

    List<VM> managedNodeList = getManagedNodeList();
    VM node1 = managedNodeList.get(0);
    VM node2 = managedNodeList.get(1);
    VM node3 = managedNodeList.get(2);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.REDUNDANCY_ZONE_NAME, "ARMY_ZONE");
    
    createCache(node1,props);
    
    node1.invoke(new SerializableRunnable("Assert Redundancy Zone") {

      public void run() {

        ManagementService service = ManagementService.getExistingManagementService(getCache());
        MemberMXBean bean = service.getMemberMXBean();
        assertEquals("ARMY_ZONE", bean.getRedundancyZone());

        

      }
    });


  }

  protected void attchListenerToDSMBean(final VM vm) {
    SerializableRunnable attchListenerToDSMBean = new SerializableRunnable("Attach Listener to DS MBean") {
      public void run() {
        assertTrue(managementService.isManager());
        DistributedSystemMXBean dsMBean = managementService.getDistributedSystemMXBean();

        // First clear the notification list
        notifList.clear();

        NotificationListener nt = new NotificationListener() {
          @Override
          public void handleNotification(Notification notification, Object handback) {
            if (notification.getType().equals(JMXNotificationType.REGION_CREATED)) {
              notifList.add(notification);
            }
          }
        };

        try {
          mbeanServer.addNotificationListener(MBeanJMXAdapter.getDistributedSystemName(), nt, null, null);
        } catch (InstanceNotFoundException e) {
          fail("Failed With Exception "  + e);
        }

      }
    };
    vm.invoke(attchListenerToDSMBean);
  }
  
  public void waitForManagerToRegisterListener() {
    SystemManagementService service = (SystemManagementService) getManagementService();
    final Map<ObjectName, NotificationHubListener> hubMap = service.getNotificationHub().getListenerObjectMap();

    Wait.waitForCriterion(new WaitCriterion() {
      public String description() {
        return "Waiting for manager to register the listener";
      }

      public boolean done() {
        boolean done = (1 == hubMap.size());
        return done;
      }

    }, MAX_WAIT, 500, true);
  }

  public void countNotificationsAndCheckRegionSize(VM node1, VM node2, VM node3, VM managingNode) {

    DistributedMember member1 = getMember(node1);
    DistributedMember member2 = getMember(node2);
    DistributedMember member3 = getMember(node3);

    final String appender1 = MBeanJMXAdapter.getUniqueIDForMember(member1);
    final String appender2 = MBeanJMXAdapter.getUniqueIDForMember(member2);
    final String appender3 = MBeanJMXAdapter.getUniqueIDForMember(member3);

    node1.invoke(new SerializableRunnable("Create Regions") {

      public void run() {

        Cache cache = getCache();

        waitForManagerToRegisterListener();
        RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
        for (int i = 1; i <= 15; i++) {
          rf.create("NotifTestRegion_"+ i);
        }
        Region member1NotifRegion = cache.getRegion(ManagementConstants.NOTIFICATION_REGION + "_" + appender1);

        assertEquals(0, member1NotifRegion.size());

      }
    });

    node2.invoke(new SerializableRunnable("Create Regions") {

      public void run() {

        Cache cache = getCache();
        waitForManagerToRegisterListener();
        RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
        for (int i = 1; i <= 15; i++) {
          rf.create("NotifTestRegion_" +   i);
        }

        Region member2NotifRegion = cache.getRegion(ManagementConstants.NOTIFICATION_REGION + "_" + appender2);

        assertEquals(0, member2NotifRegion.size());
      }
    });

    node3.invoke(new SerializableRunnable("Create Regions") {

      public void run() {

        Cache cache = getCache();
        waitForManagerToRegisterListener();
        RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
        for (int i = 1; i <= 15; i++) {
          rf.create("NotifTestRegion_" + i);
        }

        Region member3NotifRegion = cache.getRegion(ManagementConstants.NOTIFICATION_REGION + "_" + appender3);

        assertEquals(0, member3NotifRegion.size());
      }
    });
    managingNode.invoke(new SerializableRunnable("Validate Notification Count") {

      public void run() {

        Wait.waitForCriterion(new WaitCriterion() {
          public String description() {
            return "Waiting for all the RegionCreated notification to reach the manager " + notifList.size();
          }

          public boolean done() {
            boolean done = (45 == notifList.size());
            return done;
          }

        }, MAX_WAIT, 500, true);

        assertEquals(45, notifList.size());
        Cache cache = getCache();
        SystemManagementService service = (SystemManagementService) getManagementService();

        Region member1NotifRegion = cache.getRegion(ManagementConstants.NOTIFICATION_REGION  + "_" +  appender1);
        Region member2NotifRegion = cache.getRegion(ManagementConstants.NOTIFICATION_REGION  + "_" +   appender2);
        Region member3NotifRegion = cache.getRegion(ManagementConstants.NOTIFICATION_REGION  + "_" +   appender3);

        // Even though we got 15 notification only 10 should be there due to
        // eviction attributes set in notification region

        waitAtMost(5, TimeUnit.SECONDS).untilCall(to(member1NotifRegion).size(), equalTo(10));
        waitAtMost(5, TimeUnit.SECONDS).untilCall(to(member2NotifRegion).size(), equalTo(10));
        waitAtMost(5, TimeUnit.SECONDS).untilCall(to(member3NotifRegion).size(), equalTo(10));
      }
    });

  }

}
