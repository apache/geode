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
package org.apache.geode.management.internal;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassLoadUtils;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementException;

/**
 * Instance of this class is responsible for proxy creation/deletion etc.
 *
 * <p>
 * If a member is added/removed proxy factory is responsible for creating removing the corresponding
 * proxies for that member.
 *
 * <p>
 * It also maintains a proxy repository {@link MBeanProxyInfoRepository} for quick access to the
 * proxy instances
 */
public class MBeanProxyFactory {
  private static final Logger logger = LogService.getLogger();

  /**
   * Proxy repository contains several indexes to search proxies in an efficient manner.
   */
  private final MBeanProxyInfoRepository proxyRepo;
  private final MBeanJMXAdapter jmxAdapter;
  private final SystemManagementService service;

  public MBeanProxyFactory(MBeanJMXAdapter jmxAdapter, SystemManagementService service) {
    this.jmxAdapter = jmxAdapter;
    this.service = service;
    proxyRepo = new MBeanProxyInfoRepository();
  }

  /**
   * Creates a single proxy and adds a {@link ProxyInfo} to proxy repository
   * {@link MBeanProxyInfoRepository}
   */
  void createProxy(DistributedMember member, ObjectName objectName,
      Region<String, Object> monitoringRegion, Object newValue) {
    try {
      FederationComponent federation = (FederationComponent) newValue;
      String interfaceClassName = federation.getMBeanInterfaceClass();

      Class interfaceClass = ClassLoadUtils.classFromName(interfaceClassName);

      Object proxy = MBeanProxyInvocationHandler.newProxyInstance(member, monitoringRegion,
          objectName, federation, interfaceClass);

      jmxAdapter.registerMBeanProxy(proxy, objectName);

      if (logger.isDebugEnabled()) {
        logger.debug("Registered ObjectName : {}", objectName);
      }

      ProxyInfo proxyInfo = new ProxyInfo(interfaceClass, proxy, objectName);
      proxyRepo.addProxyToRepository(member, proxyInfo);

      service.afterCreateProxy(objectName, interfaceClass, proxy, (FederationComponent) newValue);

      if (logger.isDebugEnabled()) {
        logger.debug("Proxy Created for : {}", objectName);
      }

    } catch (ClassNotFoundException e) {
      throw new ManagementException(e);
    }
  }

  /**
   * This method will create all the proxies for a given DistributedMember. It does not throw any
   * exception to its caller. It handles the error and logs error messages
   *
   * <p>
   * It will be called from GII or when a member joins the system
   */
  void createAllProxies(DistributedMember member, Region<String, Object> monitoringRegion) {
    if (logger.isDebugEnabled()) {
      logger.debug("Creating proxy for: {}", member.getId());
    }

    Set<Map.Entry<String, Object>> mbeans = monitoringRegion.entrySet();

    for (Map.Entry<String, Object> mbean : mbeans) {
      try {
        ObjectName objectName = ObjectName.getInstance(mbean.getKey());

        if (logger.isDebugEnabled()) {
          logger.debug("Creating proxy for ObjectName {}", objectName);
        }

        createProxy(member, objectName, monitoringRegion, mbean.getValue());
      } catch (Exception e) {
        logger.warn("Create Proxy failed for {} with exception {}", mbean.getKey(), e.getMessage(),
            e);
      }
    }
  }

  /**
   * Removes all proxies for a given member
   */
  void removeAllProxies(DistributedMember member, Region<String, Object> monitoringRegion) {
    Set<Entry<String, Object>> entries = monitoringRegion.entrySet();

    if (logger.isDebugEnabled()) {
      logger.debug("Removing {} proxies for member {}", entries.size(), member.getId());
    }

    for (Entry<String, Object> entry : entries) {
      String key = null;
      try {
        // MBean Name in String format.
        key = entry.getKey();
        // Federation Component
        Object federation = entry.getValue();
        ObjectName mbeanName = ObjectName.getInstance(key);
        removeProxy(member, mbeanName, federation);
      } catch (EntryNotFoundException entryNotFoundException) {
        // Entry has already been removed by another thread, so no need to remove it
        logProxyAlreadyRemoved(member, entry);
      } catch (Exception e) {
        if (!(e.getCause() instanceof InstanceNotFoundException)) {
          logger.warn("Remove Proxy failed for {} due to {}", key, e.getMessage(), e);
        }
      }
    }
  }

  /**
   * Removes the proxy
   */
  void removeProxy(DistributedMember member, ObjectName objectName, Object oldValue) {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Removing proxy for ObjectName {}", objectName);

      }

      ProxyInfo proxyInfo = proxyRepo.findProxyInfo(objectName);
      proxyRepo.removeProxy(member, objectName);
      if (proxyInfo != null) {
        service.afterRemoveProxy(objectName, proxyInfo.getProxyInterface(),
            proxyInfo.getProxyInstance(), (FederationComponent) oldValue);
      }
      jmxAdapter.unregisterMBean(objectName);

      if (logger.isDebugEnabled()) {
        logger.debug("Removed proxy for ObjectName {}", objectName);
      }

    } catch (Exception e) {
      if (!(e.getCause() instanceof InstanceNotFoundException)) {
        logger.warn("Could not remove proxy for Member {} due to {}", member, e.getMessage(), e);
      }
    }
  }

  void updateProxy(ObjectName objectName, ProxyInfo proxyInfo, Object newObject, Object oldObject) {
    try {
      if (proxyInfo != null) {
        Class interfaceClass = proxyInfo.getProxyInterface();
        service.afterUpdateProxy(objectName, interfaceClass, proxyInfo.getProxyInstance(),
            (FederationComponent) newObject, (FederationComponent) oldObject);
      }
    } catch (Exception e) {
      throw new ManagementException(e);
    }
  }

  /**
   * Find a particular proxy instance for a {@link ObjectName}, {@link DistributedMember} and
   * interface class If the proxy interface does not implement the given interface class a
   * {@link ClassCastException} will be thrown.
   *
   * @param objectName {@link ObjectName} of the MBean
   * @param interfaceClass interface class implemented by proxy
   *
   * @return an instance of proxy exposing the given interface
   */
  <T> T findProxy(ObjectName objectName, Class<T> interfaceClass) {
    return proxyRepo.findProxyByName(objectName, interfaceClass);
  }

  ProxyInfo findProxyInfo(ObjectName objectName) {
    return proxyRepo.findProxyInfo(objectName);
  }

  /**
   * Find a set of proxies given a {@link DistributedMember}.
   *
   * @param member {@link DistributedMember}
   *
   * @return a set of {@link ObjectName}
   */
  public Set<ObjectName> findAllProxies(DistributedMember member) {
    return proxyRepo.findProxySet(member);
  }

  /**
   * This will return the last updated time of the proxyMBean
   *
   * @param objectName {@link ObjectName} of the MBean
   * @return last updated time of the proxy
   */
  long getLastUpdateTime(ObjectName objectName) {
    ProxyInterface proxyInterface = findProxy(objectName, ProxyInterface.class);
    return proxyInterface.getLastRefreshedTime();
  }

  @VisibleForTesting
  void logProxyAlreadyRemoved(DistributedMember member, Entry<String, Object> entry) {
    logger.warn("Proxy for entry {} and member {} has already been removed", entry,
        member.getId());
  }
}
