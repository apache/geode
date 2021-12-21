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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;


/**
 * This class is a repository of all proxy related information multiple indices are provided for
 * searching This searching capability will ease while various proxy ops. It will also be used while
 * filter addition/ removal if dynamic filters are going to be supported.
 *
 *
 */

public class MBeanProxyInfoRepository {

  private static final Logger logger = LogService.getLogger();

  /**
   * This index will keep a map between old object name and proxy info
   */
  private final Map<ObjectName, ProxyInfo> objectNameIndex;

  /**
   * This index will keep a map between old object name and proxy info
   */
  private final Map<DistributedMember, Set<ObjectName>> memberIndex;

  protected MBeanProxyInfoRepository() {

    objectNameIndex = new ConcurrentHashMap<ObjectName, ProxyInfo>();
    memberIndex = new ConcurrentHashMap<DistributedMember, Set<ObjectName>>();
  }

  /**
   * Add the {@link ProxyInfo} into repository for future quick access
   *
   * @param member Distributed Member
   * @param proxyInfo Proxy Info instance
   */
  protected void addProxyToRepository(DistributedMember member, ProxyInfo proxyInfo) {
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
    } else {
      return null;
    }


  }

  /**
   * Finds the proxy instance by {@link javax.management.ObjectName}
   *
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
   * Finds the set of proxy instances by {@link org.apache.geode.distributed.DistributedMember}
   *
   * @param member DistributedMember
   * @return A set of proxy instance on which user can invoke operations as defined by the proxy
   *         interface
   */
  protected Set<ObjectName> findProxySet(DistributedMember member) {
    if (logger.isTraceEnabled()) {
      logger.trace("SEARCHING PROXIES FOR MEMBER : {}", member.getId());
    }

    Set<ObjectName> proxyInfoSet = memberIndex.get(member);
    if (proxyInfoSet != null) {
      return Collections.unmodifiableSet(proxyInfoSet);
    } else {
      return Collections.emptySet();
    }
  }

  /**
   * Removes a proxy of a given {@link org.apache.geode.distributed.DistributedMember} and given
   * {@link javax.management.ObjectName}
   *
   * @param member DistributedMember
   * @param objectName MBean name
   */
  protected void removeProxy(DistributedMember member, ObjectName objectName) {
    ProxyInfo info = objectNameIndex.remove(objectName);
    Set<ObjectName> proxyInfoSet = memberIndex.get(member);
    if (proxyInfoSet == null || proxyInfoSet.size() == 0) {
      return;
    }
    proxyInfoSet.remove(objectName);

  }

}
