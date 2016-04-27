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
package com.gemstone.gemfire.cache.query.internal.aggregate.uda;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.UDAExistsException;
import com.gemstone.gemfire.cache.query.internal.aggregate.uda.UDADistributionAdvisor.UDAProfile;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.UpdateAttributesProcessor;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

public class UDAManagerImpl implements DistributionAdvisee, UDAManager {

  private ConcurrentMap<String, Class<Aggregator>> map;

  private final DistributionAdvisor advisor;
  private static final Logger logger = LogService.getLogger();
  private static ThreadLocal<Map<String, Class<Aggregator>>> localOnlyUDAS = new ThreadLocal<Map<String, Class<Aggregator>>>();

  public UDAManagerImpl() {
    this.map = new ConcurrentHashMap<String, Class<Aggregator>>();
    this.advisor = UDADistributionAdvisor.createUDAAdvisor(this);

  }

  public void createUDA(String name, String fqClass) throws UDAExistsException, NameResolutionException {
    createUDALocally(name, fqClass);
    new UDAMessage(name, fqClass).send();
  }

  public Map<String, String> getUDAs() {
    Map<String, String> mapping = new HashMap<String, String>();
    for (Map.Entry<String, Class<Aggregator>> entry : this.map.entrySet()) {
      mapping.put(entry.getKey(), entry.getValue().getName());
    }
    return mapping;
  }

  public void createUDALocally(String name, String fqClass) throws NameResolutionException, UDAExistsException {
    synchronized (this) {
      if (!this.map.containsKey(name)) {
        try {
          Class<Aggregator> aggregatorClass = (Class<Aggregator>) Class.forName(fqClass);
          this.map.put(name, aggregatorClass);
        } catch (ClassNotFoundException cnfe) {
          throw new NameResolutionException(LocalizedStrings.UDA_MANAGER_Class_Not_Found.toLocalizedString(fqClass, name), cnfe);
        }
      } else {
        throw new UDAExistsException(LocalizedStrings.UDA_MANAGER_Uda_Exists.toLocalizedString(name));
      }
    }
  }

  public void collectUDAsFromRemote() {
    int numTries = 5;
    boolean collectedUDAs = false;
    for (int i = 0; i < numTries; ++i) {
      try {
        new UpdateAttributesProcessor(this).collect(DataSerializableFixedID.UDA_PROFILE);
        collectedUDAs = true;
        break;
      } catch (Exception e) {
        this.getDistributionManager().getCancelCriterion().checkCancelInProgress(e);
      }
    }
    if (!collectedUDAs) {
      if (logger.isErrorEnabled()) {
        logger.error(LocalizedStrings.UDA_MANAGER_Udas_Not_Collected.toLocalizedString());
      }
    }
  }

  void addUDAs(Map<String, String> udas, boolean checkforOnlyLocal) {
    // Get those UDAs which are present only locally
    final Map<String, Class<Aggregator>> onlyLocalUDAs = new HashMap<String, Class<Aggregator>>();
    if (logger.isInfoEnabled()) {
      logger.info("UDAManagerImpl::addUDAs: adding remote collected UDAs=" + udas + " check for local only =" + checkforOnlyLocal);
    }
    synchronized (this) {
      if (checkforOnlyLocal) {
        for (Map.Entry<String, Class<Aggregator>> entry : this.map.entrySet()) {
          if (!udas.containsKey(entry.getKey())) {
            onlyLocalUDAs.put(entry.getKey(), entry.getValue());
          }
        }
      }
      for (Map.Entry<String, String> entry : udas.entrySet()) {
        String udaName = entry.getKey();
        String udaClass = entry.getValue();
        Class<Aggregator> existingUDAClass = this.map.get(udaName);
        if (existingUDAClass == null) {
          try {
            Class<Aggregator> aggregatorClass = (Class<Aggregator>) Class.forName(udaClass);
            this.map.put(udaName, aggregatorClass);
          } catch (ClassNotFoundException cnfe) {
            logger.error(LocalizedStrings.UDA_MANAGER_Class_Not_Found.toLocalizedString(udaClass, udaName));
          }
        } else {
          // check if the classes are same
          if (!udaClass.equals(existingUDAClass.getName())) {
            logger.error(LocalizedStrings.UDA_MANAGER_Class_Conflict.toLocalizedString(udaName, existingUDAClass.getName(), udaClass));
          }
        }
      }
    }

    if (checkforOnlyLocal && !onlyLocalUDAs.isEmpty()) {
      if (logger.isInfoEnabled()) {
        logger.info("UDAManagerImpl::addUDAs:Only local UDAs=" + onlyLocalUDAs);
      }
      localOnlyUDAS.set(onlyLocalUDAs);
      new UpdateAttributesProcessor(UDAManagerImpl.this).sendProfileUpdate(false);
    }

  }

  public Class<Aggregator> getUDAClass(String name) throws NameResolutionException {
    Class<Aggregator> aggClass = this.map.get(name);
    if (aggClass == null) {
      throw new NameResolutionException(LocalizedStrings.UDA_MANAGER_Aggregator_Not_Found.toLocalizedString(name));
    }
    return aggClass;
  }

  public synchronized void clear() {
    this.map.clear();
  }

  @Override
  public void removeUDA(String udaName) {
    this.removeUDALocally(udaName);
    new UDAMessage(udaName).send();
  }

  void removeUDALocally(String udaName) {
    synchronized (this) {
      this.map.remove(udaName);
    }
  }

  @Override
  public DM getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return null;
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {

    return this.advisor;
  }

  @Override
  public Profile getProfile() {
    return this.advisor.createProfile();
  }

  @Override
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return InternalDistributedSystem.getConnectedInstance();
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public String getFullPath() {
    return null;
  }

  @Override
  public void fillInProfile(Profile profile) {
    UDAProfile udap = (UDAProfile) profile;
    synchronized (this) {
      Map<String, Class<Aggregator>> localOnly = localOnlyUDAS.get();
      localOnlyUDAS.set(null);
      Map<String, Class<Aggregator>> toSend = localOnly != null ? localOnly : this.map;
      for (Map.Entry<String, Class<Aggregator>> entry : toSend.entrySet()) {
        udap.putInProfile(entry.getKey(), entry.getValue().getName());
      }
    }

  }

  @Override
  public int getSerialNumber() {
    return 0;
  }
}
