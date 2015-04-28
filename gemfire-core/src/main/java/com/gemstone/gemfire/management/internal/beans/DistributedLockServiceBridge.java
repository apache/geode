/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.ObjectName;

import com.gemstone.gemfire.management.LockServiceMXBean;
import com.gemstone.gemfire.management.internal.FederationComponent;
import com.gemstone.gemfire.management.internal.ManagementConstants;

/**
 * Bridge for the Distributed lock service. It provides an aggregated view of a
 * lock service which might be present in multiple members.
 * 
 * Proxies are added as and when proxies are received by Federation framework.
 * 
 * Each method which access all the proxies to gather data from them creates an
 * Iterator. Iterates over them and gather data. Creating multiple iterator on
 * each method call is a concern and a better way needs to be introduced.
 * 
 * @author rishim
 * 
 */
public class DistributedLockServiceBridge {

  /**
   * Map of LockServiceMXBean proxies
   */
  private Map<ObjectName, LockServiceMXBean> mapOfProxy;

  /**
   * List of locks. keeping it member level to avoid object creation cost during
   * each call.
   */
  private List<String> listHeldLock;

  /**
   * Map of threads holding lock
   */
  private Map<String, String> threadsHoldingLock;

  /**
   * set size of this proxy set
   */
  private volatile int setSize;

  /**
   * Public constructor
   * 
   * @param objectName
   *          name of the MBean
   * @param proxy
   *          reference to the proxy
   */
  public DistributedLockServiceBridge(ObjectName objectName,
      LockServiceMXBean proxy, FederationComponent newState) {
    this.mapOfProxy = new ConcurrentHashMap<ObjectName, LockServiceMXBean>();
    this.listHeldLock = new ArrayList<String>();
    this.threadsHoldingLock = new HashMap<String, String>();
    addProxyToMap(objectName, proxy);

  }

  /**
   * Add a proxy to the proxy map
   * 
   * @param objectName
   *          name of the MBean
   * @param proxy
   *          reference to the proxy
   */
  public void addProxyToMap(ObjectName objectName, LockServiceMXBean proxy) {
    if (mapOfProxy != null) {
      mapOfProxy.put(objectName, proxy);
      setSize = mapOfProxy.values().size();
    }
  }

  /**
   * 
   * @param objectName
   *          name of the MBean
   * @param proxy
   *          reference to the proxy
   * @return true if no proxies left for this aggregator to work on
   */
  public boolean removeProxyFromMap(ObjectName objectName,
      LockServiceMXBean proxy) {
    if (mapOfProxy != null) {
      mapOfProxy.remove(objectName);
      setSize = mapOfProxy.values().size();
      if (mapOfProxy.values().size() == 0) {
        setSize = 0;
        return true;

      }
    }
    return false;
  }

  

  /**
   * 
   * @return member name of the grantor
   */
  public String fetchGrantorMember() {
    Iterator<LockServiceMXBean> it = mapOfProxy.values().iterator();
    if (it != null) {
      while (it.hasNext()) {
        String grantorMember = it.next().fetchGrantorMember();
        return grantorMember;
      }
    }
    return null;
  }

  /**
   * 
   * @return number of members using this lock service
   */
  public int getMemberCount() {

    return setSize;
  }

  /**
   * 
   * @return list of members using this lock service
   */
  public String[] getMemberNames() {
    Iterator<LockServiceMXBean> it = mapOfProxy.values().iterator();
    if (it != null) {
      while (it.hasNext()) {
        String[] memberNames = it.next().getMemberNames();
        return memberNames;
      }

    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * 
   * @return name of the lock service
   */
  public String getName() {
    Iterator<LockServiceMXBean> it = mapOfProxy.values().iterator();
    if (it != null) {
      while (it.hasNext()) {
        String name = it.next().getName();
        return name;
      }

    }
    return null;
  }

  /**
   * 
   * @return lists the name of locks held by this member's threads
   */
  public String[] listHeldLocks() {
    Iterator<LockServiceMXBean> it = mapOfProxy.values().iterator();
    listHeldLock.clear();
    if (it != null) {
      while (it.hasNext()) {
        String[] locks = it.next().listHeldLocks();
        if (locks != null && locks.length > 0) {
          for (String lock : locks) {
            listHeldLock.add(lock);
          }
        }

      }

    }
    String[] tmpStr = new String[listHeldLock.size()];
    return listHeldLock.toArray(tmpStr);

  }

  /**
   * 
   * @return a map of object name and thread name if this member holds lock or
   *         null/none
   */
  public Map<String, String> listThreadsHoldingLock() {
    Iterator<LockServiceMXBean> it = mapOfProxy.values().iterator();
    threadsHoldingLock.clear();
    if (it != null) {
      while (it.hasNext()) {
        Map<String, String> threadLockMap = it.next()
            .listThreadsHoldingLock();
        if (threadLockMap != null) {
          threadsHoldingLock.putAll(threadLockMap);
        }


      }

    }
    return threadsHoldingLock;
  }

}
