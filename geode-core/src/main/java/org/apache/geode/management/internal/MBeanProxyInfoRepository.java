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
 */
public class MBeanProxyInfoRepository {

  private static final Logger logger = LogService.getLogger();

  /**
   * map between old object name and proxy info
   */
  private final Map<ObjectName, ProxyInfo> objectNameToProxyInfo;

  /**
   * map between old object name and proxy info
   */
  private final Map<DistributedMember, Set<ObjectName>> memberToObjectNames;

  protected MBeanProxyInfoRepository() {
    objectNameToProxyInfo = new ConcurrentHashMap<>();
    memberToObjectNames = new ConcurrentHashMap<>();
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

    objectNameToProxyInfo.put(objectName, proxyInfo);
    if (memberToObjectNames.get(member) != null) {
      memberToObjectNames.get(member).add(proxyInfo.getObjectName());
    } else {
      Set<ObjectName> proxyInfoSet = new CopyOnWriteArraySet<>();
      proxyInfoSet.add(proxyInfo.getObjectName());
      memberToObjectNames.put(member, proxyInfoSet);
    }
  }

  /**
   * Finds the proxy instance by {@link ObjectName}
   *
   * @return instance of proxy
   */
  protected <T> T findProxyByName(ObjectName objectName, Class<T> interfaceClass) {
    if (logger.isDebugEnabled()) {
      logger.debug("findProxyByName : {}", objectName);
      logger.debug("findProxyByName Existing ObjectNames  : {}", objectNameToProxyInfo.keySet());
    }

    ProxyInfo proxyInfo = objectNameToProxyInfo.get(objectName);
    if (proxyInfo != null) {
      return interfaceClass.cast(proxyInfo.getProxyInstance());
    }
    return null;
  }

  /**
   * Finds the proxy instance by {@link ObjectName}
   *
   * @return instance of proxy
   */
  protected ProxyInfo findProxyInfo(ObjectName objectName) {
    if (logger.isTraceEnabled()) {
      logger.trace("SEARCHING FOR PROXY INFO N REPO FOR MBEAN : {}", objectName);
    }

    return objectNameToProxyInfo.get(objectName);
  }

  /**
   * Finds the set of proxy instances by {@link DistributedMember}
   *
   * @param member DistributedMember
   * @return A set of proxy instance on which user can invoke operations as defined by the proxy
   *         interface
   */
  protected Set<ObjectName> findProxySet(DistributedMember member) {
    if (logger.isTraceEnabled()) {
      logger.trace("SEARCHING PROXIES FOR MEMBER : {}", member.getId());
    }

    Set<ObjectName> proxyInfoSet = memberToObjectNames.get(member);
    if (proxyInfoSet != null) {
      return Collections.unmodifiableSet(proxyInfoSet);
    }
    return Collections.emptySet();
  }

  /**
   * Removes a proxy of a given {@link DistributedMember} and given
   * {@link ObjectName}
   *
   * @param member DistributedMember
   * @param objectName MBean name
   */
  protected void removeProxy(DistributedMember member, ObjectName objectName) {
    objectNameToProxyInfo.remove(objectName);
    Set<ObjectName> proxyInfoSet = memberToObjectNames.get(member);
    if (proxyInfoSet == null || proxyInfoSet.isEmpty()) {
      return;
    }
    proxyInfoSet.remove(objectName);
  }

  @Override
  public String toString() {
    return "MBeanProxyInfoRepository{" +
        "objectNameToProxyInfo=" + objectNameToProxyInfo +
        ", memberToObjectNames=" + memberToObjectNames +
        '}';
  }
}
