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

import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.ClassLoadUtils;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Cache listener on local monitoring region to get Manager's local MBean updates. This acts as an
 * aggregator interface for Manager. Its used only on Manager's side.
 *
 *
 */
public class MonitoringRegionCacheListener extends CacheListenerAdapter<String, Object> {

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
        interfaceClass = ClassLoadUtils.classFromName(className);
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
        interfaceClass = ClassLoadUtils.classFromName(className);
        classRef.put(className, interfaceClass);
      }

      service.afterPseudoCreateProxy(objectName, interfaceClass, null, newObject);

    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Aggregation Failed failed for {} With Exception {}", THIS_COMPONENT,
            objectName, e);
      }
    }
  }

}
