/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Cache listener on local monitoring region to get Manager's local MBean
 * updates. This acts as an aggregator interface for Manager. Its used only on
 * Manager's side.
 * 
 * @author rishim
 * 
 */
public class MonitoringRegionCacheListener extends
    CacheListenerAdapter<String, Object> {

  private static final Logger logger = LogService.getLogger();

  private SystemManagementService service;

  private Map<String, Class> classRef = new HashMap<String, Class>();

  private static final String THIS_COMPONENT = MonitoringRegionCacheListener.class.getName();

  public MonitoringRegionCacheListener(SystemManagementService service) {
    this.service = service;
  }

  @Override
  public void afterUpdate(EntryEvent<String, Object> event) {
    ObjectName objectName = null;
    try {

      if (!service.isStartedAndOpen() || !service.isManager()) {
        // NO OP return; No work for Non Manager Nodes
        return;
      }
      objectName = ObjectName.getInstance(event.getKey());

      FederationComponent oldObject = (FederationComponent) event.getOldValue();
      FederationComponent newObject = (FederationComponent) event.getNewValue();
      String className = newObject.getMBeanInterfaceClass();
      Class interfaceClass;
      if (classRef.get(className) != null) {
        interfaceClass = classRef.get(className);
      } else {
        interfaceClass = ClassLoadUtil.classFromName(className);
        classRef.put(className, interfaceClass);
      }

      service.afterUpdateProxy(objectName, interfaceClass, null, newObject, oldObject);

    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Aggregation Failed failed for {} with exception {}", e);
      }
    }

  }
  
  @Override 
      public void afterCreate(EntryEvent<String, Object> event) { 
        ObjectName objectName = null; 
        try { 
     
          if (!service.isStartedAndOpen() || !service.isManager()) { 
            // NO OP return; No work for Non Manager Nodes 
            return; 
          } 
          objectName = ObjectName.getInstance(event.getKey()); 
     
          FederationComponent newObject = (FederationComponent) event.getNewValue(); 
          String className = newObject.getMBeanInterfaceClass(); 
          Class interfaceClass; 
          if (classRef.get(className) != null) { 
            interfaceClass = classRef.get(className); 
          } else { 
            interfaceClass = ClassLoadUtil.classFromName(className); 
            classRef.put(className, interfaceClass); 
          } 
     
         service.afterPseudoCreateProxy(objectName, interfaceClass, null, newObject); 
     
        } catch (Exception e) { 
          if (logger.isDebugEnabled()) { 
            logger.debug("{}: Aggregation Failed failed for {} With Exception {}", THIS_COMPONENT, objectName, e); 
          } 
        } 
      } 

}
