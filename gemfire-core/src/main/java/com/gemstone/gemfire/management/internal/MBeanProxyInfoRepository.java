/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.LogService;


/**
 * This class is a repository of all proxy related information multiple indices
 * are provided for searching This searching capability will ease while various
 * proxy ops. It will also be used while filter addition/ removal if dynamic
 * filters are going to be supported.
 * 
 * @author rishim
 * 
 */

public class MBeanProxyInfoRepository {

  private static final Logger logger = LogService.getLogger();

  /**
   * This index will keep a map between old object name and proxy info
   */
  private Map<ObjectName, ProxyInfo> objectNameIndex;

  /**
   * This index will keep a map between old object name and proxy info
   */
  private Map<DistributedMember, Set<ObjectName>> memberIndex;

  protected MBeanProxyInfoRepository() {

    objectNameIndex = new ConcurrentHashMap<ObjectName, ProxyInfo>();
    memberIndex = new ConcurrentHashMap<DistributedMember, Set<ObjectName>>();
  }

  /**
   * Add the {@link ProxyInfo} into repository for future quick access
   * 
   * @param member
   *          Distributed Member
   * @param proxyInfo
   *          Proxy Info instance
   */
  protected void addProxyToRepository(DistributedMember member,
      ProxyInfo proxyInfo) {
    ObjectName objectName = proxyInfo.getObjectName();
    if (logger.isTraceEnabled()) {
      logger.trace("ADDED TO PROXY REPO : {}", proxyInfo.getObjectName());
    }

    objectNameIndex.put(objectName, proxyInfo);
    if (memberIndex.get(member) != null) {
      memberIndex.get(member).add(proxyInfo.getObjectName());
    } else {
      Set<ObjectName> proxyInfoSet = new CopyOnWriteArraySet<ObjectName>();
      proxyInfoSet.add(proxyInfo.getObjectName());
      memberIndex.put(member, proxyInfoSet);
    }

  }

  /**
   * Finds the proxy instance by {@link javax.management.ObjectName}
   * 
   * @param objectName
   * @param interfaceClass
   * @return instance of proxy
   */
  protected <T> T findProxyByName(ObjectName objectName, Class<T> interfaceClass) {
    if (logger.isDebugEnabled()) {
      logger.debug("findProxyByName : {}", objectName);
      logger.debug("findProxyByName Existing ObjectNames  : {}", objectNameIndex.keySet());
    }

    ProxyInfo proxyInfo = objectNameIndex.get(objectName);
    if (proxyInfo != null) {
      return interfaceClass.cast(proxyInfo.getProxyInstance());
    }else{
      return null;
    }
    

  }
  
  /**
   * Finds the proxy instance by {@link javax.management.ObjectName}
   * 
   * @param objectName
   * @return instance of proxy
   */
  protected ProxyInfo findProxyInfo(ObjectName objectName) {
    if (logger.isTraceEnabled()) {
      logger.trace("SEARCHING FOR PROXY INFO N REPO FOR MBEAN : {}", objectName);
    }
    ProxyInfo proxyInfo = objectNameIndex.get(objectName);
   
    return proxyInfo;
  }

  /**
   * Finds the set of proxy instances by {@link com.gemstone.gemfire.distributed.DistributedMember} 
   * 
   * @param member
   *          DistributedMember
   * @return A set of proxy instance on which user can invoke operations as
   *         defined by the proxy interface
   */
  protected Set<ObjectName> findProxySet(DistributedMember member) {
    if (logger.isTraceEnabled()) {
      logger.trace("SEARCHING PROXIES FOR MEMBER : {}", member.getId());
    }

    Set<ObjectName> proxyInfoSet = memberIndex.get(member);
    if (proxyInfoSet != null) {
      return Collections.unmodifiableSet(proxyInfoSet);
    }else{
      return Collections.emptySet();
    }
  }

  /**
   * Removes a proxy of a given
   * {@link com.gemstone.gemfire.distributed.DistributedMember} and given
   * {@link javax.management.ObjectName}
   * 
   * @param member
   *          DistributedMember
   * @param objectName
   *          MBean name
   */
  protected void removeProxy(DistributedMember member, ObjectName objectName) {
    ProxyInfo info = objectNameIndex.remove(objectName);
    Set<ObjectName> proxyInfoSet = memberIndex.get(member);
    if (proxyInfoSet == null || proxyInfoSet.size() == 0) {
      return;
    }
    if (proxyInfoSet.contains(objectName)) {
      proxyInfoSet.remove(objectName);
    }

  }

}