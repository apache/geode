/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * This listener is attached to the Monitoring Region to receive any addition or
 * deletion of MBEans
 * 
 * It updates the last refreshed time of proxy once it gets the update request
 * from the Managed Node
 * 
 * @author rishim
 * 
 */
public class ManagementCacheListener extends
    CacheListenerAdapter<String, Object> {

  private static final Logger logger = LogService.getLogger();
  
  private MBeanProxyFactory proxyHelper;
  
  private volatile boolean  readyForEvents;

  public ManagementCacheListener(MBeanProxyFactory proxyHelper) {
    this.proxyHelper = proxyHelper;
    this.readyForEvents = false;
  }

  @Override
  public void afterCreate(EntryEvent<String, Object> event) {
    if (!readyForEvents) {
      return;
    }
    ObjectName objectName = null;

    try {
      objectName = ObjectName.getInstance(event.getKey());
      Object newObject = event.getNewValue();
      proxyHelper.createProxy(event.getDistributedMember(), objectName, event
          .getRegion(), newObject);
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Proxy Create failed for {} with exception {}", objectName, e.getMessage(), e);
      }
    }

  }

  @Override
  public void afterDestroy(EntryEvent<String, Object> event) {
    ObjectName objectName = null;

    try {
      objectName = ObjectName.getInstance(event.getKey());
      Object oldObject = event.getOldValue();
      proxyHelper.removeProxy(event.getDistributedMember(), objectName,
          oldObject);
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Proxy Destroy failed for {} with exception {}", objectName, e.getMessage(), e);
      }
    }

  }

  @Override
  public void afterUpdate(EntryEvent<String, Object> event) {
    ObjectName objectName = null;
    try {
      if (!readyForEvents) {
        return;
      }
      objectName = ObjectName.getInstance(event.getKey());

      ProxyInfo proxyInfo = proxyHelper.findProxyInfo(objectName);
      if (proxyInfo != null) {
        ProxyInterface proxyObj = (ProxyInterface) proxyInfo.getProxyInstance();
        // Will return null if proxy is filtered out
        if (proxyObj != null) {
          proxyObj.setLastRefreshedTime(System.currentTimeMillis());
        }
        Object oldObject = event.getOldValue();
        Object newObject = event.getNewValue();
        proxyHelper.updateProxy(objectName, proxyInfo, newObject, oldObject);
      }

    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Proxy Update failed for {} with exception {}", objectName, e.getMessage(), e);
      }

    }

  }
  
  void markReady(){
    readyForEvents = true;
  }

}