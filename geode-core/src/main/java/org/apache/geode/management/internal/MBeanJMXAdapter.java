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
package com.gemstone.gemfire.management.internal;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.management.AsyncEventQueueMXBean;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.management.DiskStoreMXBean;
import com.gemstone.gemfire.management.DistributedLockServiceMXBean;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.GatewayReceiverMXBean;
import com.gemstone.gemfire.management.GatewaySenderMXBean;
import com.gemstone.gemfire.management.LocatorMXBean;
import com.gemstone.gemfire.management.LockServiceMXBean;
import com.gemstone.gemfire.management.ManagementException;
import com.gemstone.gemfire.management.ManagerMXBean;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.RegionMXBean;

/**
 * Utility class to interact with the JMX server
 * 
 * 
 */
public class MBeanJMXAdapter implements ManagementConstants {

  /** The <code>MBeanServer</code> for this application */
  public static MBeanServer mbeanServer = ManagementFactory
      .getPlatformMBeanServer();

  /**
	 * 
	 */
  private Map<ObjectName, Object> localGemFireMBean;
  
  private DistributedMember distMember;

  /**
   * log writer, or null if there is no distributed system available
   */
//  private LogWriterI18n logger = InternalDistributedSystem.getLoggerI18n();

  /**
   * public constructor
   */
  
  public MBeanJMXAdapter() {
    this.localGemFireMBean = new ConcurrentHashMap<ObjectName, Object>();
    this.distMember = InternalDistributedSystem.getConnectedInstance()
        .getDistributedMember();  
  }

  /**
   * This method will register an MBean in GemFire domain. Even if the client
   * provides a domain name it will be ignored and GemFire domain name will be
   * used.
   * 
   * This method checks the local Filter for registering the MBean. If filtered
   * the MBean wont be registered. Although the filter will remember the
   * filtered MBean and register it once the filter is removed.
   * 
   * @param object
   * @param objectName
   * @return modifed ObjectName
   */
  public ObjectName registerMBean(Object object, ObjectName objectName, boolean isGemFireMBean) {
    ObjectName newObjectName = objectName;
    try {
      if (!isGemFireMBean) {
        String member = getMemberNameOrId(distMember);
        String objectKeyProperty = objectName.getKeyPropertyListString();
        
        newObjectName = ObjectName.getInstance(OBJECTNAME__PREFIX + objectKeyProperty + 
            KEYVAL_SEPARATOR + "member=" + member);
      }
      mbeanServer.registerMBean(object, newObjectName);
      this.localGemFireMBean.put(newObjectName, object);

    } catch (InstanceAlreadyExistsException e) {
      throw new ManagementException(e);
    } catch (MBeanRegistrationException e) {
      throw new ManagementException(e);
    } catch (NotCompliantMBeanException e) {
      throw new ManagementException(e);
    } catch (MalformedObjectNameException e) {
      throw new ManagementException(e);
    } catch (NullPointerException e) {
      throw new ManagementException(e);
    }
    return newObjectName;
  }

  /**
   * Checks whether an MBean implements notification support classes or not
   * 
   * @param objectName
   * @return if this MBean can be a notification broadcaster
   */
  public boolean hasNotificationSupport(ObjectName objectName) {
    try {
      if(!isRegistered(objectName)){
        return false;
      }
      ObjectInstance instance = mbeanServer.getObjectInstance(objectName);
      String className = instance.getClassName();
      Class cls = ClassLoadUtil.classFromName(className);
      Type[] intfTyps = cls.getGenericInterfaces();
      for (int i = 0; i < intfTyps.length; i++) {
        Class intfTyp = (Class)intfTyps[i];
        if (intfTyp.equals(NotificationEmitter.class)) {
          return true;
        }
      }
      Class supreClassTyp = (Class)cls.getGenericSuperclass();
      if (supreClassTyp != null
          && supreClassTyp.equals(NotificationBroadcasterSupport.class)) {
        return true;
      }
    } catch (InstanceNotFoundException e) {
      throw new ManagementException(e);
    } catch (ClassNotFoundException e) {
      throw new ManagementException(e);
    }
    return false;
  }

  /**
   * This method will register an MBean in GemFire domain. Even if the client
   * provides a domain name it will be ignored and GemFire domain name will be
   * used.
   * 
   * This method checks the local Filter for registering the MBean. If filtered
   * the MBean wont be registered. Although the filter will remember the
   * filtered MBean and register it once the filter is removed.
   * 
   * @param object
   * @param objectName
   */
  public void registerMBeanProxy(Object object, ObjectName objectName) {

    try {

      mbeanServer.registerMBean(object, objectName);

    } catch (InstanceAlreadyExistsException e) {
      throw new ManagementException(e);
    } catch (MBeanRegistrationException e) {
      throw new ManagementException(e);
    } catch (NotCompliantMBeanException e) {
      throw new ManagementException(e);
    } catch (NullPointerException e) {
      throw new ManagementException(e);
    }

  }

  /**
   * 
   * This method will unregister the MBean from GemFire Domain
   * 
   * @param objectName
   */
  public void unregisterMBean(ObjectName objectName) {

    try {

      if(!isRegistered(objectName)){
        return;
      }
      mbeanServer.unregisterMBean(objectName);
      // For Local GemFire MBeans
      
      if (localGemFireMBean.get(objectName) != null) {
        localGemFireMBean.remove(objectName);
      }
    } catch (NullPointerException e) {
      throw new ManagementException(e);
    } catch (InstanceNotFoundException e) {
      throw new ManagementException(e);
    } catch (MBeanRegistrationException e) {
      throw new ManagementException(e);
    }

  }
  
  public Object getMBeanObject(ObjectName objectName){
    return localGemFireMBean.get(objectName);
  }
  
  /**
   * Finds the MBean instance by {@link javax.management.ObjectName}
   * 
   * @param objectName
   * @param interfaceClass
   * @return instance of MBean
   */
  public <T> T findMBeanByName(ObjectName objectName, Class<T> interfaceClass) {

    Object mbeanInstance = localGemFireMBean.get(objectName);
    if (mbeanInstance != null) {
      return interfaceClass.cast(mbeanInstance);
    } else {
      return null;
    }
  }

  public boolean isLocalMBean(ObjectName objectName){
    return localGemFireMBean.containsKey(objectName);
  }
  /**
   * Method to unregister all the local GemFire MBeans
   * 
   */

  public void unregisterAll() {
    try {
      ObjectName name = new ObjectName(OBJECTNAME__PREFIX + "*");
      Set<ObjectName> gemFireObjects = mbeanServer.queryNames(name, null);

      for (ObjectName objectName : gemFireObjects) {
        unregisterMBean(objectName);
      }
    } catch (MalformedObjectNameException e) {
      throw new ManagementException(e);
    } catch (NullPointerException e) {
      throw new ManagementException(e);
    }

  }

  public void cleanJMXResource() {
    localGemFireMBean.clear();
    unregisterAll();
}
  
  public boolean isRegistered(ObjectName objectName){
    
    return mbeanServer.isRegistered(objectName);
  }

  /**
   * This method returns the name that will be used for a DistributedMember when it is registered
   * as a JMX bean.
   * 
   * @param member Member to find the name for
   * @return The name used to register this member as a JMX bean.
   */
  public static String getMemberNameOrId(DistributedMember member) {
    if (member.getName() != null && !member.getName().equals("")) {
      return makeCompliantName(member.getName());
    }
    return makeCompliantName(member.getId());
  }
  
  /**
   * Return a String that been modified to be compliant as a property of an
   * ObjectName.
   * <p>
   * The property name of an ObjectName may not contain any of the following
   * characters: <b><i>: , = * ?</i></b>
   * <p>
   * This method will replace the above non-compliant characters with a dash:
   * <b><i>-</i></b>
   * <p>
   * If value is empty, this method will return the string "nothing".
   * <p>
   * Note: this is <code>public</code> because certain tests call this from
   * outside of the package. 
   * 
   * @param value
   *          the potentially non-compliant ObjectName property
   * @return the value modified to be compliant as an ObjectName property
   */
  public static String makeCompliantName(String value) {
    value = value.replace(':', '-');
    value = value.replace(',', '-');
    value = value.replace('=', '-');
    value = value.replace('*', '-');
    value = value.replace('?', '-');
    if (value.length() < 1) {
      value = "nothing";
    }
    return value;
  }
  
  public static String makeCompliantRegionPath(String value) {
    if (isQuoted(value)) {
      value = ObjectName.unquote(value);
    } else {
      if (containsSpecialChar(value)) {
        value = ObjectName.quote(value);
      }
    }
    return value;
  }

  private static boolean containsSpecialChar(String value) {
    if (value.contains(":")) {
      return true;
    }
    if (value.contains("@")) {
      return true;
    }
    if (value.contains("-")) {
      return true;
    }
    if (value.contains("#")) {
      return true;
    }
    if (value.contains("+")) {
      return true;
    }
    if (value.contains("?")) {
      return true;
    }
    return false;
  }
  
  private static boolean isQuoted(String value){
    final int len = value.length();
    if (len < 2 || value.charAt(0) != '"' || value.charAt(len - 1) != '"'){
      return false;
    }else{
      return true;
    }
  }

  /**
	 * 
	 */

  public static String makeCompliantRegionNameAppender(String value) {

    return value.replace("/", "-").replace(":", "");

  }

  /**
   * returns Member MBean if any
   * 
   */
  public MemberMXBean getMemberMXBean() {
    ObjectName objName = getMemberMBeanName(distMember);
    return (MemberMXBean) localGemFireMBean.get(objName);

  }

  public RegionMXBean getLocalRegionMXBean(String regionPath) {
    ObjectName objName = getRegionMBeanName(distMember, regionPath);
    return (RegionMXBean) localGemFireMBean.get(objName);

  }

  public LockServiceMXBean getLocalLockServiceMXBean(String lockServiceName) {
    ObjectName objName = getLockServiceMBeanName(distMember, lockServiceName);
    return (LockServiceMXBean) localGemFireMBean.get(objName);
  }

  public DiskStoreMXBean getLocalDiskStoreMXBean(String disStoreName) {
    ObjectName objName = getDiskStoreMBeanName(distMember, disStoreName);
    return (DiskStoreMXBean) localGemFireMBean.get(objName);

  }

  public CacheServerMXBean getClientServiceMXBean(int serverPort) {
    ObjectName objName = getClientServiceMBeanName(serverPort,distMember);
    return (CacheServerMXBean) localGemFireMBean.get(objName);

  }

  public DistributedLockServiceMXBean getDistributedLockServiceMXBean(
      String lockServiceName) {
    ObjectName objName = getDistributedLockServiceName(lockServiceName);
    return (DistributedLockServiceMXBean) localGemFireMBean.get(objName);
  }

  public DistributedRegionMXBean getDistributedRegionMXBean(String regionPath) {
    ObjectName objName = getDistributedRegionMbeanName(regionPath);
    return (DistributedRegionMXBean) localGemFireMBean.get(objName);
  }

  public ManagerMXBean getManagerMXBean() {
    ObjectName objName = getManagerName();
    return (ManagerMXBean) localGemFireMBean.get(objName);
  }
  
  public DistributedSystemMXBean getDistributedSystemMXBean() {
    ObjectName objName = getDistributedSystemName();
    return (DistributedSystemMXBean) localGemFireMBean.get(objName);
  }
  
  public GatewayReceiverMXBean getGatewayReceiverMXBean() {
    ObjectName objName = getGatewayReceiverMBeanName(distMember);
    return (GatewayReceiverMXBean) localGemFireMBean.get(objName);
  }

  public GatewaySenderMXBean getGatewaySenderMXBean(String senderId) {
    ObjectName objName = getGatewaySenderMBeanName(distMember, senderId);
    return (GatewaySenderMXBean) localGemFireMBean.get(objName);
  }
  
  public AsyncEventQueueMXBean getAsyncEventQueueMXBean(String queueId) {
    ObjectName objName = getAsycnEventQueueMBeanName(distMember, queueId);
    return (AsyncEventQueueMXBean) localGemFireMBean.get(objName);
  }
  

  public LocatorMXBean getLocatorMXBean() {
    ObjectName objName = getLocatorMBeanName(distMember);
    return (LocatorMXBean) localGemFireMBean.get(objName);
  }

  public static ObjectName getObjectName(String name) {
    try {
      return ObjectName.getInstance(name);
    } catch (MalformedObjectNameException e) {
      throw new ManagementException(e);
    } catch (NullPointerException e) {
      throw new ManagementException(e);
    }
  }

  public static ObjectName getMemberMBeanName(DistributedMember member) {
    return getObjectName((MessageFormat.format(OBJECTNAME__MEMBER_MXBEAN, new Object[] { getMemberNameOrId(member) })));
  }

  public static ObjectName getMemberMBeanName(String member) {
    return getObjectName((MessageFormat.format(OBJECTNAME__MEMBER_MXBEAN, new Object[] { makeCompliantName(member) })));
  }

  public static ObjectName getRegionMBeanName(DistributedMember member,
      String regionPath) {

    return getObjectName((MessageFormat.format(OBJECTNAME__REGION_MXBEAN,
        new Object[] { makeCompliantRegionPath(regionPath),
            getMemberNameOrId(member) })));
  }

  public static ObjectName getRegionMBeanName(String member, String regionPath) {
    return getObjectName((MessageFormat.format(OBJECTNAME__REGION_MXBEAN,
        new Object[] { makeCompliantRegionPath(regionPath),
            makeCompliantName(member) })));
  }

  public static ObjectName getRegionMBeanName(ObjectName memberMBeanName, String regionPath) {
    return getObjectName((MessageFormat.format(OBJECTNAME__REGION_MXBEAN, new Object[] { makeCompliantRegionPath(regionPath),
        memberMBeanName.getKeyProperty(ManagementConstants.OBJECTNAME_MEMBER_APPENDER) })));
  }

  public static ObjectName getDiskStoreMBeanName(DistributedMember member, String diskName) {
    return getObjectName((MessageFormat.format(OBJECTNAME__DISKSTORE_MXBEAN, new Object[] { diskName, getMemberNameOrId(member) })));
  }

  public static ObjectName getDiskStoreMBeanName(String member, String diskName) {
    return getObjectName((MessageFormat.format(OBJECTNAME__DISKSTORE_MXBEAN, new Object[] { diskName, makeCompliantName(member) })));
  }

  public static ObjectName getClientServiceMBeanName(int serverPort, DistributedMember member) {
    return getObjectName((MessageFormat.format(OBJECTNAME__CLIENTSERVICE_MXBEAN, new Object[] { String.valueOf(serverPort),
        getMemberNameOrId(member) })));
  }

  public static ObjectName getClientServiceMBeanName(int serverPort, String member) {
    return getObjectName((MessageFormat.format(OBJECTNAME__CLIENTSERVICE_MXBEAN, new Object[] { String.valueOf(serverPort),
        makeCompliantName(member) })));
  }

  public static ObjectName getLockServiceMBeanName(DistributedMember member, String lockServiceName) {
    return getObjectName((MessageFormat.format(OBJECTNAME__LOCKSERVICE_MXBEAN, new Object[] { lockServiceName,
        getMemberNameOrId(member) })));
  }

  public static ObjectName getLockServiceMBeanName(String member, String lockServiceName) {
    return getObjectName((MessageFormat.format(OBJECTNAME__LOCKSERVICE_MXBEAN, new Object[] { lockServiceName,
        makeCompliantName(member) })));
  }

  public static ObjectName getGatewayReceiverMBeanName(DistributedMember member) {
    return getObjectName((MessageFormat.format(OBJECTNAME__GATEWAYRECEIVER_MXBEAN, new Object[] { getMemberNameOrId(member) })));
  }

  public static ObjectName getGatewayReceiverMBeanName(String member) {
    return getObjectName((MessageFormat.format(OBJECTNAME__GATEWAYRECEIVER_MXBEAN, new Object[] { makeCompliantName(member) })));
  }

  public static ObjectName getGatewaySenderMBeanName(DistributedMember member, String id) {
    return getObjectName((MessageFormat.format(OBJECTNAME__GATEWAYSENDER_MXBEAN, new Object[] { id, getMemberNameOrId(member) })));
  }

  public static ObjectName getGatewaySenderMBeanName(String member, String id) {
    return getObjectName((MessageFormat.format(OBJECTNAME__GATEWAYSENDER_MXBEAN, new Object[] { id, makeCompliantName(member) })));
  }

  public static ObjectName getAsycnEventQueueMBeanName(DistributedMember member, String queueId) {
    return getObjectName((MessageFormat.format(OBJECTNAME__ASYNCEVENTQUEUE_MXBEAN, new Object[] { queueId,
        getMemberNameOrId(member) })));
  }

  public static ObjectName getAsycnEventQueueMBeanName(String member, String queueId) {
    return getObjectName((MessageFormat.format(OBJECTNAME__ASYNCEVENTQUEUE_MXBEAN, new Object[] { queueId,
        makeCompliantName(member) })));
  }

  public static ObjectName getDistributedRegionMbeanName(String regionPath) {
    return getObjectName((MessageFormat.format(OBJECTNAME__DISTRIBUTEDREGION_MXBEAN, new Object[] { makeCompliantRegionPath(regionPath) })));
  }
  
  /**
   * Without special character transformation
   * @param regionPath region path
   * @return ObjectName MBean name
   */
  public static ObjectName getDistributedRegionMbeanNameInternal(String regionPath) {
    return getObjectName((MessageFormat.format(OBJECTNAME__DISTRIBUTEDREGION_MXBEAN, new Object[] { regionPath })));
  }

  public static ObjectName getDistributedLockServiceName(String lockService) {
    return getObjectName((MessageFormat.format(OBJECTNAME__DISTRIBUTEDLOCKSERVICE_MXBEAN, new Object[] { lockService })));
  }

  public static ObjectName getDistributedSystemName() {
    return getObjectName(OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN);
  }

  public static ObjectName getManagerName() {
    String member = getMemberNameOrId(InternalDistributedSystem.getConnectedInstance().getDistributedMember());
    return getObjectName((MessageFormat.format(OBJECTNAME__MANAGER_MXBEAN, new Object[] { member })));
  }

  public static ObjectName getLocatorMBeanName(DistributedMember member) {
    return getObjectName((MessageFormat.format(OBJECTNAME__LOCATOR_MXBEAN, new Object[] { getMemberNameOrId(member) })));
  }

  public static ObjectName getLocatorMBeanName(String member) {
    return getObjectName((MessageFormat.format(OBJECTNAME__LOCATOR_MXBEAN, new Object[] { makeCompliantName(member) })));
  }

  public static ObjectName getCacheServiceMBeanName(DistributedMember member, String cacheServiceId) {
    return getObjectName((MessageFormat.format(OBJECTNAME__CACHESERVICE_MXBEAN, new Object[] { cacheServiceId,
        getMemberNameOrId(member) })));
  }

  public Map<ObjectName, Object> getLocalGemFireMBean() {
    return this.localGemFireMBean;
  }

  public static String getUniqueIDForMember(DistributedMember member) {

    InternalDistributedMember iMember = (InternalDistributedMember) member;
    final StringBuilder sb = new StringBuilder();
    sb.append(iMember.getInetAddress().getHostAddress());
    sb.append("<v" + iMember.getVmViewId() + ">"); // View ID will be 0 for
    // Loner, but in that case no
    // federation as well
    sb.append(iMember.getPort());
    return makeCompliantName(sb.toString().toLowerCase());// Lower case to
    // handle IPv6
  }
  
  public static boolean isAttributeAvailable(String attributeName,
      String objectName) {

      try {
        ObjectName objName = new ObjectName(objectName);
        mbeanServer.getAttribute(objName, attributeName);
      } catch (MalformedObjectNameException e) {
        return false;
      } catch (NullPointerException e) {
        return false;
      } catch (AttributeNotFoundException e) {
        return false;
      } catch (InstanceNotFoundException e) {
        return false;
      } catch (MBeanException e) {
        return false;
      } catch (ReflectionException e) {
        return false;
      }

    return true;

  }
  
  public static int VALUE_NOT_AVAILABLE = -1;

}
