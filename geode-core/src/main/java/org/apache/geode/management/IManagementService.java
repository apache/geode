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

import java.util.Set;

import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.membership.MembershipListener;

public interface IManagementService {

  /**
   * Returns whether this member is running the management service.
   *
   * @return True if this member is running the management service, false otherwise.
   */
  boolean isManager();

  /**
   * Starts the management service on this member.
   */
  void startManager();

  /**
   * Stops the management service running on this member.
   */
  void stopManager();

  /**
   * Registers an MBean in the GemFire domain. Any other domain specified as part of the object will
   * be ignored.
   *
   * @param object MBean to register.
   * @param objectName Object name of the MBean to register.
   * @return Object name, which may have been modified to use the GemFire domain.
   */
  ObjectName registerMBean(Object object, ObjectName objectName);

  /**
   * Unregisters an MBean.
   *
   * @param objectName Object name of the MBean to unregister.
   */
  void unregisterMBean(ObjectName objectName);

  /**
   * Adds a bean to the list of those being federated, meaning that it's state will be periodically
   * pushed to managing members. It's possible to simply register MBeans in the GemFire domain
   * without calling federate if the developer doesn't need their state to be shared amongst
   * members. Note that Dynamic MBeans are not supported by this service.
   *
   * <pre>
   * Example Usage:
   *
   * <code>CustomMXBean bean = new CustomMBean();
   * ObjectName beanName = ObjectName.getInstance("DefualtDomain:type=CustomType");
   * ObjectName gemfireBeanName = service.registerMBean(customMBean, customMBeanName);
   * service.federate(gemfireBeanName, CustomMXBean.class, true);</code>
   * </pre>
   *
   * @param objectName Object name of the MBean.
   * @param interfaceClass Interface which this MBean exposes.
   * @param notificationEmitter True if the MBean is a notification emitter.
   */

  <T> void federate(ObjectName objectName, Class<T> interfaceClass,
      boolean notificationEmitter);

  /**
   * Returns the MemberMXBean for managing and monitoring the local member.
   */
  MemberMXBean getMemberMXBean();

  /**
   * Returns a RegionMXBbean for managing and monitoring a Region.
   *
   * @param regionPath Path of the region.
   * @return A RegionMXBean if the region exists, null otherwise.
   */
  RegionMXBean getLocalRegionMBean(String regionPath);

  /**
   * Returns a LockServiceMXBean for managing and monitoring a lock service.
   *
   * @param lockServiceName Name of the lock service.
   * @return A LockServiceMXBean if the lock service exists, null otherwise.
   */
  LockServiceMXBean getLocalLockServiceMBean(String lockServiceName);

  /**
   * Returns a DiskStoreMXBean for managing and monitoring a disk store.
   *
   * @param diskStoreName Name of the disk store.
   * @return A DiskStoreMXBean if the disk store exists, null otherwise.
   */
  DiskStoreMXBean getLocalDiskStoreMBean(String diskStoreName);

  /**
   * Returns a CacheServerMXBean for managing and monitoring a cache server.
   *
   * @param serverPort Port on which the cache server is listening.
   * @return A CacheServerMXBean if the cache server is found, null otherwise.
   */
  CacheServerMXBean getLocalCacheServerMXBean(int serverPort);

  /**
   * Returns the DistributedSystemMXBean for managing and monitoring the distributed system as a
   * whole.
   *
   * @return A DistributedSystemMXBean if one is found, null otherwise.
   */
  DistributedSystemMXBean getDistributedSystemMXBean();

  /**
   * Returns the ManagerMXBean for the management service.
   *
   * @return A ManagerMXBean if one is found, null otherwise.
   */
  ManagerMXBean getManagerMXBean();

  /**
   * Returns a DistributedRegionMXBean for managing and monitoring a region from a system wide
   * perspective.
   *
   * @param regionPath Path of the Region.
   * @return A DistributedRegionMXBean if the region exists, null otherwise.
   */
  DistributedRegionMXBean getDistributedRegionMXBean(String regionPath);

  /**
   * Returns a LockServiceMXBean for managing and monitoring a lock service from a system wide
   * perspective.
   *
   * @param lockServiceName Name of the LockService.
   * @return A DistributedLockServiceMXBean if the lock service exists, null otherwise.
   */
  DistributedLockServiceMXBean getDistributedLockServiceMXBean(
      String lockServiceName);

  /**
   * Returns the GatewayReceiverMXBean for managing and monitoring the gateway receiver.
   *
   * @return A GatewayReceiverMXBean if one is found, null otherwise.
   */
  GatewayReceiverMXBean getLocalGatewayReceiverMXBean();

  /**
   * Returns a GatewaySenderMXBean for managing and monitoring a gateway sender.
   *
   * @param senderId ID of the gateway sender.
   * @return A GatewaySenderMXBean if the gateway sender is found, null otherwise.
   */
  GatewaySenderMXBean getLocalGatewaySenderMXBean(String senderId);

  /**
   * Returns a AsyncEventQueueMXBean for managing and monitoring an asynchronous queue.
   *
   * @param queueId ID of the asynchronous queue.
   * @return An AsyncEventQueueMXBean if the asynchronous queue is found, null otherwise.
   */
  AsyncEventQueueMXBean getLocalAsyncEventQueueMXBean(String queueId);

  /**
   * Returns the LocatorMXBean for managing and monitoring the locator.
   *
   * @return A LocatorMXBean if the locator is found, null otherwise.
   */
  LocatorMXBean getLocalLocatorMXBean();

  /**
   * Returns the object names for all MBeans associated with a member.
   *
   * @param member Member for which to find MBeans.
   */
  Set<ObjectName> queryMBeanNames(DistributedMember member);

  /**
   * Returns the ids of the async event queues on this member
   */
  Set<ObjectName> getAsyncEventQueueMBeanNames(DistributedMember member);

  /**
   * Returns an instance of an MBean. This is a reference to the MBean instance and not a
   * {@link ObjectInstance}.
   *
   * @param objectName Object name of the MBean.
   * @param interfaceClass Interface which this MBean exposes.
   * @throws ClassCastException if the MBean does not implement the given interface.
   */
  <T> T getMBeanInstance(ObjectName objectName, Class<T> interfaceClass);

  /**
   * Returns the last updated time of the remote MBean as reported by Sysem.currentTimeMillis().
   *
   * @param objectName Object name of the MBean.
   * @return Last updated time or 0 if the MBean is local or the management service is not running
   *         on this member.
   */
  long getLastUpdateTime(ObjectName objectName);

  /**
   * Returns the object name of the MemberMBean representing a distributed member. This is a utility
   * method for generating an object name and it does not register an MBean.
   *
   * @param member Distributed member used to generate the object name.
   */
  ObjectName getMemberMBeanName(DistributedMember member);

  /**
   * Returns the object name of the RegionMBean representing a region. This is a utility method for
   * generating an object name and it does not register an MBean.
   *
   * @param member Distributed member used to generate the object name.
   * @param regionPath Path of the region.
   */
  ObjectName getRegionMBeanName(DistributedMember member, String regionPath);

  /**
   * Returns the object name of the DiskStoreMBean representing a disk store. This is a utility
   * method for generating an object name and it does not register an MBean.
   *
   * @param member Distributed member used to generate the object name.
   * @param diskName Name of the disk store.
   */
  ObjectName getDiskStoreMBeanName(DistributedMember member, String diskName);

  /**
   * Returns the object name of the CacheServerMBean representing a cache server. This is a utility
   * method for generating an object name and it does not register an MBean.
   *
   * @param member Distributed member used to generate the object name.
   * @param serverPort Port on which the cache server is listening.
   */
  ObjectName getCacheServerMBeanName(int serverPort, DistributedMember member);

  /**
   * Returns the object name of the LockServiceMBean representing a lock service. This is a utility
   * method for generating an object name and it does not register an MBean.
   *
   * @param member Distributed member used to generate the object name.
   * @param lockServiceName Name of the lock service.
   */
  ObjectName getLockServiceMBeanName(DistributedMember member,
      String lockServiceName);

  /**
   * Returns the object name of the GatewayReciverMBean representing a gateway receiver. This is a
   * utility method for generating an object name and it does not register an MBean.
   *
   * @param member Distributed member used to generate the object name.
   */
  ObjectName getGatewayReceiverMBeanName(DistributedMember member);

  /**
   * Returns the object name of the GatewaySenderMBean representing a gateway sender. This is a
   * utility method for generating an object name and it does not register an MBean.
   *
   * @param member Distributed member used to generate the object name.
   * @param gatwaySenderId ID of the gateway sender.
   */
  ObjectName getGatewaySenderMBeanName(DistributedMember member,
      String gatwaySenderId);

  /**
   * Returns the object name of the AsyncEventQueueMBean representing a asynchronous queue. This is
   * a utility method for generating an object name and it does not register an MBean.
   *
   * @param member Distributed member used to generate the object name.
   * @param queueId ID of the asynchronous queue.
   */
  ObjectName getAsyncEventQueueMBeanName(DistributedMember member, String queueId);

  /**
   * Returns the object name of the DistributedRegionMBean representing a region. This is a utility
   * method for generating an object name and it does not register an MBean.
   *
   * @param regionName Name of the region.
   */
  ObjectName getDistributedRegionMBeanName(String regionName);

  /**
   * Returns the object name of the DistributedLockServiceMBean representing a lock service. This is
   * a utility method for generating an object name and it does not register an MBean.
   *
   * @param lockService Name of the lock service.
   */
  ObjectName getDistributedLockServiceMBeanName(String lockService);

  /**
   * Returns the object name of the getDistributedSystemMBeanName representing a distributed system.
   * This is a utility method for generating an object name and it does not register an MBean.
   */
  ObjectName getDistributedSystemMBeanName();

  /**
   * Returns the object name of the ManagerMBean representing a manager. This is a utility method
   * for generating an object name and it does not register an MBean.
   */
  ObjectName getManagerMBeanName();

  /**
   * Returns the object name of the LocatorMBean representing a locator. This is a utility method
   * for generating an object name and it does not register an MBean.
   *
   * @param member Distributed member used to generate the object name.
   */
  ObjectName getLocatorMBeanName(DistributedMember member);


  /**
   * Registers a listener that receives call backs when a member joins or leaves the distributed
   * system.
   */
  void addMembershipListener(MembershipListener listener);

  /**
   * Unregisters a membership listener
   *
   * @see #addMembershipListener
   */
  void removeMembershipListener(MembershipListener listener);
}
