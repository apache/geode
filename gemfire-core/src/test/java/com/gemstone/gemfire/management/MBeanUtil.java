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

import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberManager;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

/**
 * Utility test class to get various proxies
 * 
 * @author rishim
 * 
 */
public class MBeanUtil {
  

  private static final int MAX_WAIT = 8 * ManagementConstants.REFRESH_TIME;
  
  public static MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

  /**
   * Utility Method to obtain MemberMXBean proxy reference for a particular
   * Member
   * 
   * @param member
   * @return a reference to MemberMXBean
   * @throws Exception
   */
  public static MemberMXBean getMemberMbeanProxy(DistributedMember member)
      throws Exception {
    MemberMXBean bean = null;
    final SystemManagementService service = (SystemManagementService) ManagementTestBase
        .getManagementService();
    final ObjectName memberMBeanName = service.getMemberMBeanName(member);
    DistributedTestCase.waitForCriterion(new WaitCriterion() {
      MemberMXBean bean = null;

      public String description() {
        return "Waiting for the proxy to get reflected at managing node";
      }

      public boolean done() {
          bean = service.getMBeanProxy(memberMBeanName, MemberMXBean.class);
        boolean done = bean != null;
        return done;
      }

    }, MAX_WAIT, 500, true);

    try {
      bean = service.getMBeanProxy(memberMBeanName, MemberMXBean.class);
    } catch (ManagementException mgz) {
      if (bean == null) {
        InternalDistributedSystem.getLoggerI18n().fine(
            "Undesired Result :MemberMBean Proxy Should Not be Empty for : "
                + memberMBeanName.getCanonicalName());
      }
    }

    return bean;
  }

  /**
   * Utility Method to obtain CacheServerMXBean proxy reference for a particular
   * Member
   * 
   * @param member
   * @return a reference to CacheServerMXBean
   * @throws Exception
   */
  public static CacheServerMXBean getCacheServerMbeanProxy(
      DistributedMember member, int port) throws Exception {
    CacheServerMXBean bean = null;
    final SystemManagementService service = (SystemManagementService) ManagementTestBase
        .getManagementService();
    final ObjectName cacheServerMBeanName = service
        .getCacheServerMBeanName(port,member);

    DistributedTestCase.waitForCriterion(new WaitCriterion() {
      CacheServerMXBean bean = null;

      public String description() {
        return "Waiting for the proxy to get reflected at managing node";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        GemFireCacheImpl cacheImpl = (GemFireCacheImpl) cache;
        bean = service.getMBeanProxy(cacheServerMBeanName,
            CacheServerMXBean.class);
        boolean done = bean != null;
        return done;
      }

    }, MAX_WAIT, 500, true);

    try {
      bean = service.getMBeanProxy(cacheServerMBeanName,
          CacheServerMXBean.class);
    } catch (ManagementException mgz) {
      if (bean == null) {
        InternalDistributedSystem.getLoggerI18n().fine(
            "Undesired Result :CacheServer Proxy Should Not be Empty for : "
                + cacheServerMBeanName.getCanonicalName());
      }
    }

    return bean;

  }
 
  /**
   * Utility Method to obtain LockServiceMXBean proxy reference for a particular
   * lock service on a Member
   * 
   * @param member
   * @return a reference to LockServiceMXBean
   * @throws Exception
   */
  public static LockServiceMXBean getLockServiceMbeanProxy(
      DistributedMember member, String lockServiceName) throws Exception {
    LockServiceMXBean bean = null;
    final SystemManagementService service = (SystemManagementService) ManagementTestBase
        .getManagementService();
    final ObjectName lockServiceMBeanName = service.getLockServiceMBeanName(
        member, lockServiceName);

    DistributedTestCase.waitForCriterion(new WaitCriterion() {
      LockServiceMXBean bean = null;

      public String description() {
        return "Waiting for the proxy to get reflected at managing node";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        GemFireCacheImpl cacheImpl = (GemFireCacheImpl) cache;
        bean = service.getMBeanProxy(lockServiceMBeanName,
            LockServiceMXBean.class);
        boolean done = bean != null;
        return done;
      }

    }, MAX_WAIT, 500, true);

    try {
      bean = service.getMBeanProxy(lockServiceMBeanName,
          LockServiceMXBean.class);
    } catch (ManagementException mgz) {
      if (bean == null) {
        InternalDistributedSystem.getLoggerI18n().fine(
            "Undesired Result :LockService Proxy Should Not be Empty for : "
                + lockServiceMBeanName.getCanonicalName());
      }
    }

    return bean;
  }

  /**
   * Utility Method to obtain RegionMXBean proxy reference for a particular
   * region on a member
   * 
   * @param member
   * @return a reference to RegionMXBean
   * @throws Exception
   */
  public static RegionMXBean getRegionMbeanProxy(DistributedMember member,
      String regionPath) throws Exception {

    final SystemManagementService service = (SystemManagementService) ManagementTestBase
        .getManagementService();

    final ObjectName regionMBeanName = service.getRegionMBeanName(
        member, regionPath);
    
    DistributedTestCase.waitForCriterion(new WaitCriterion(){ 
      
      RegionMXBean bean = null;
      public String description() {
        return "Waiting for the proxy to get reflected at managing node";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        bean = service.getMBeanProxy(regionMBeanName, RegionMXBean.class);
        boolean done = (bean !=null);
        return done;
      }

    }, MAX_WAIT, 500, true);
    
    RegionMXBean bean = null;
    try {
      bean = service.getMBeanProxy(regionMBeanName, RegionMXBean.class);
    } catch (ManagementException mgz) {
      if (bean == null) {
        InternalDistributedSystem.getLoggerI18n().fine(
            "Undesired Result :RegionMBean Proxy Should Not be Empty for : "
                + regionMBeanName.getCanonicalName());
      }
    }

    return bean;
  }
  

  /**
   * Utility Method to obtain GatewaySenderMXBean proxy reference for a
   * particular sender id on a member
   * 
   * @param member
   *          distributed member
   * @param gatwaySenderId
   *          sender id
   * @return a reference to GatewaySenderMXBean
   * @throws Exception
   */
  public static GatewaySenderMXBean getGatewaySenderMbeanProxy(
      DistributedMember member, String gatwaySenderId) throws Exception {

    final SystemManagementService service = (SystemManagementService) ManagementTestBase
        .getManagementService();

    final ObjectName senderMBeanName = service.getGatewaySenderMBeanName(member, gatwaySenderId);

    DistributedTestCase.waitForCriterion(new WaitCriterion() {

      GatewaySenderMXBean bean = null;

      public String description() {
        return "Waiting for the proxy to get reflected at managing node";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        bean = service
            .getMBeanProxy(senderMBeanName, GatewaySenderMXBean.class);
        boolean done = (bean != null);
        return done;
      }

    }, MAX_WAIT, 500, true);

    GatewaySenderMXBean bean = null;
    try {
      bean = service.getMBeanProxy(senderMBeanName, GatewaySenderMXBean.class);
    } catch (ManagementException mgz) {
      if (bean == null) {
        InternalDistributedSystem.getLoggerI18n().fine(
            "Undesired Result :GatewaySender MBean Proxy Should Not be Empty for : "
                + senderMBeanName.getCanonicalName());
      }
    }

    return bean;
  }
  
  /**
   * Utility Method to obtain AsyncEventQueueMXBean proxy reference for a
   * particular queue id on a member
   * 
   * @param member
   *          distributed member
   * @param queueId
   *          Queue id
   * @return a reference to AsyncEventQueueMXBean
   * @throws Exception
   */
  public static AsyncEventQueueMXBean getAsyncEventQueueMBeanProxy(
      DistributedMember member, String queueId) throws Exception {
    
    final SystemManagementService service = (SystemManagementService) ManagementTestBase
        .getManagementService();

    final ObjectName queueMBeanName = service.getAsyncEventQueueMBeanName(
        member, queueId);

    DistributedTestCase.waitForCriterion(new WaitCriterion() {

      AsyncEventQueueMXBean bean = null;

      public String description() {
        return "Waiting for the proxy to get reflected at managing node";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        bean = service.getMBeanProxy(queueMBeanName,
            AsyncEventQueueMXBean.class);
        boolean done = (bean != null);
        return done;
      }

    }, MAX_WAIT, 500, true);

    AsyncEventQueueMXBean bean = null;
    try {
      bean = service.getMBeanProxy(queueMBeanName, AsyncEventQueueMXBean.class);
    } catch (ManagementException mgz) {
      if (bean == null) {
        InternalDistributedSystem.getLoggerI18n().fine(
            "Undesired Result :Async Event Queue MBean Proxy Should Not be Empty for : "
                + queueMBeanName.getCanonicalName());
      }
    }

    return bean;
  }

  /**
   * Utility Method to obtain GatewayReceiverMXBean proxy reference for a member
   * 
   * @param member
   *          distributed member
   * @return a reference to GatewayReceiverMXBean
   * @throws Exception
   */
  public static GatewayReceiverMXBean getGatewayReceiverMbeanProxy(
      DistributedMember member) throws Exception {

    final SystemManagementService service = (SystemManagementService) ManagementTestBase
        .getManagementService();

    final ObjectName receiverMBeanName = service.getGatewayReceiverMBeanName(member);

    DistributedTestCase.waitForCriterion(new WaitCriterion() {

      GatewayReceiverMXBean bean = null;

      public String description() {
        return "Waiting for the proxy to get reflected at managing node";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        bean = service
            .getMBeanProxy(receiverMBeanName, GatewayReceiverMXBean.class);
        boolean done = (bean != null);
        return done;
      }

    }, MAX_WAIT, 500, true);

    GatewayReceiverMXBean bean = null;
    try {
      bean = service.getMBeanProxy(receiverMBeanName, GatewayReceiverMXBean.class);
    } catch (ManagementException mgz) {
      if (bean == null) {
        InternalDistributedSystem.getLoggerI18n().fine(
            "Undesired Result :GatewaySender MBean Proxy Should Not be Empty for : "
                + receiverMBeanName.getCanonicalName());
      }
    }

    return bean;
  }

  /**
   * Utility Method to obtain DistributedRegionMXBean proxy reference for a
   * particular region
   * 
   * @param regionName
   *          name of the region
   * @return a reference to DistributedRegionMXBean
   * @throws Exception
   */
  public static DistributedRegionMXBean getDistributedRegionMbean(
      final String regionName, final int expectedMembers) throws Exception {
    DistributedRegionMXBean bean = null;

    final ManagementService service = ManagementTestBase
        .getManagementService();

    DistributedTestCase.waitForCriterion(new WaitCriterion() {

      DistributedRegionMXBean bean = null;

      public String description() {
        return "Waiting for " + regionName + " With " +  expectedMembers
            + " proxies to get reflected at managing node";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        bean = service.getDistributedRegionMXBean(regionName);

        boolean done = (bean != null && bean.getMemberCount() == expectedMembers);
        return done;
      }

    }, MAX_WAIT, 1000, true);

    bean = service.getDistributedRegionMXBean(regionName);
    return bean;
  }

  /**
   * Utility Method to obtain DistributedRegionMXBean proxy reference for a
   * particular region
   * 
   * @param lockServiceName
   *          name of the lock service
   * @return a reference to DistributedLockServiceMXBean
   * @throws Exception
   */
  public static DistributedLockServiceMXBean getDistributedLockMbean(
      final String lockServiceName, final int expectedMembers) throws Exception {
    DistributedLockServiceMXBean bean = null;

    final ManagementService service = ManagementTestBase
        .getManagementService();
    DistributedTestCase.waitForCriterion(new WaitCriterion() {

      DistributedLockServiceMXBean bean = null;

      public String description() {
        return "Waiting for " + expectedMembers
            + " proxies to get reflected at managing node";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        bean = service.getDistributedLockServiceMXBean(lockServiceName);
        boolean done = (bean != null && bean.getMemberCount() == expectedMembers);
        return done;
      }

    }, MAX_WAIT, 500, true);

    bean = service.getDistributedLockServiceMXBean(lockServiceName);
    return bean;
  }
  

  /**
   * Utility Method to obtain GatewayReceiverMXBean proxy reference for a member
   * 
   * @param member
   *          distributed member
   * @return a reference to GatewayReceiverMXBean
   * @throws Exception
   */
  public static LocatorMXBean getLocatorMbeanProxy(DistributedMember member)
      throws Exception {

    final SystemManagementService service = (SystemManagementService) ManagementTestBase
        .getManagementService();

    final ObjectName locatorMBeanName = service.getLocatorMBeanName(member);

    DistributedTestCase.waitForCriterion(new WaitCriterion() {

      LocatorMXBean bean = null;

      public String description() {
        return "Waiting for the proxy to get reflected at managing node";
      }

      public boolean done() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        bean = service.getMBeanProxy(locatorMBeanName, LocatorMXBean.class);
        boolean done = (bean != null);
        return done;
      }

    }, MAX_WAIT, 500, true);

    LocatorMXBean bean = null;
    try {
      bean = service.getMBeanProxy(locatorMBeanName, LocatorMXBean.class);
    } catch (ManagementException mgz) {
      if (bean == null) {
        InternalDistributedSystem.getLoggerI18n().fine(
            "Undesired Result :Locator MBean Proxy Should Not be Empty for : "
                + locatorMBeanName.getCanonicalName());
      }
    }

    return bean;
  }
  
  public static void printBeanDetails(ObjectName objName) throws Exception {

    MBeanAttributeInfo[] attributeInfos;
    MBeanInfo info = null;
    try {
      info = mbeanServer.getMBeanInfo(objName);
    } catch (IntrospectionException e1) {
      DistributedTestCase.fail("Could not obtain Sender Proxy Details");
    } catch (InstanceNotFoundException e1) {
      DistributedTestCase.fail("Could not obtain Sender Proxy Details");
    } catch (ReflectionException e1) {
      DistributedTestCase.fail("Could not obtain Sender Proxy Details");
    }
    attributeInfos = info.getAttributes();
    for (MBeanAttributeInfo attributeInfo : attributeInfos) {

      Object propertyValue = null;
      String propertyName = null;

      try {
        propertyName = attributeInfo.getName();
        propertyValue = mbeanServer.getAttribute(objName, propertyName);
        DistributedTestCase.getLogWriter().info(
            "<ExpectedString> " + propertyName + " = " + propertyValue
                + "</ExpectedString> ");
      } catch (Exception e) {

      }
    }

  }
}
