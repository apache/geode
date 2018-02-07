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
package org.apache.geode.management.internal.cli.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.ClassName;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.RegionPath;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

/**
 * Function used by the 'alter region' gfsh command to alter a region on each member.
 *
 * @since GemFire 8.0
 */
public class RegionAlterFunction implements Function, InternalEntity {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = -4846425364943216425L;

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = context.getCache();
    String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    RegionFunctionArgs regionAlterArgs = (RegionFunctionArgs) context.getArguments();
    try {
      Region<?, ?> alteredRegion = alterRegion(cache, regionAlterArgs);
      XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", alteredRegion.getName());
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity,
          CliStrings.format(CliStrings.ALTER_REGION__MSG__REGION_0_ALTERED_ON_1,
              new Object[] {alteredRegion.getFullPath(), memberNameOrId})));

    } catch (IllegalStateException e) {
      logger.error(e.getMessage(), e);

      resultSender.lastResult(new CliFunctionResult(memberNameOrId, false, e.getMessage()));
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage(), e);

      resultSender.lastResult(new CliFunctionResult(memberNameOrId, false, e.getMessage()));
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable th) {
      SystemFailure.checkFailure();
      logger.error(th.getMessage(), th);

      String exceptionMsg = th.getMessage();
      if (exceptionMsg == null) {
        exceptionMsg = CliUtil.stackTraceAsString(th);
      }
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, false, exceptionMsg));
    }
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singleton(ResourcePermissions.DATA_MANAGE);
  }

  private <K, V> Region<?, ?> alterRegion(Cache cache, RegionFunctionArgs regionAlterArgs) {
    final String regionPathString = regionAlterArgs.getRegionPath();

    RegionPath regionPath = new RegionPath(regionPathString);
    AbstractRegion region = (AbstractRegion) cache.getRegion(regionPathString);
    if (region == null) {
      throw new IllegalArgumentException(CliStrings.format(
          CliStrings.ALTER_REGION__MSG__REGION_DOES_NOT_EXIST_0, new Object[] {regionPath}));
    }

    AttributesMutator mutator = region.getAttributesMutator();

    if (regionAlterArgs.isCloningEnabled() != null) {
      mutator.setCloningEnabled(regionAlterArgs.isCloningEnabled());
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cloning");
      }
    }

    if (regionAlterArgs.getEvictionMax() != null) {
      mutator.getEvictionAttributesMutator().setMaximum(regionAlterArgs.getEvictionMax());
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - eviction attributes max");
      }
    }

    // Alter expiration attributes
    final RegionFunctionArgs.ExpirationAttrs newEntryExpirationIdleTime =
        regionAlterArgs.getEntryExpirationIdleTime();
    if (newEntryExpirationIdleTime.isTimeOrActionSet()) {
      mutator.setEntryIdleTimeout(
          newEntryExpirationIdleTime.getExpirationAttributes(region.getEntryIdleTimeout()));
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - entry idle timeout");
      }
    }

    final RegionFunctionArgs.ExpirationAttrs newEntryExpirationTTL =
        regionAlterArgs.getEntryExpirationTTL();
    if (newEntryExpirationTTL.isTimeOrActionSet()) {
      mutator.setEntryTimeToLive(
          newEntryExpirationTTL.getExpirationAttributes(region.getEntryTimeToLive()));
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - entry TTL");
      }
    }

    final RegionFunctionArgs.ExpirationAttrs newRegionExpirationIdleTime =
        regionAlterArgs.getRegionExpirationIdleTime();
    if (newRegionExpirationIdleTime.isTimeOrActionSet()) {
      mutator.setRegionIdleTimeout(
          newRegionExpirationIdleTime.getExpirationAttributes(region.getRegionIdleTimeout()));
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - region idle timeout");
      }
    }

    final RegionFunctionArgs.ExpirationAttrs newRegionExpirationTTL =
        regionAlterArgs.getRegionExpirationTTL();
    if (newRegionExpirationTTL.isTimeOrActionSet()) {
      mutator.setRegionTimeToLive(
          newRegionExpirationTTL.getExpirationAttributes(region.getRegionTimeToLive()));
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - region TTL");
      }
    }

    // Alter Gateway Sender Ids
    final Set<String> newGatewaySenderIds = regionAlterArgs.getGatewaySenderIds();
    if (newGatewaySenderIds != null) {

      // Remove old gateway sender ids that aren't in the new list
      Set<String> oldGatewaySenderIds = region.getGatewaySenderIds();
      if (!oldGatewaySenderIds.isEmpty()) {
        for (String gatewaySenderId : oldGatewaySenderIds) {
          if (!newGatewaySenderIds.contains(gatewaySenderId)) {
            mutator.removeGatewaySenderId(gatewaySenderId);
          }
        }
      }

      // Add new gateway sender ids that don't already exist
      for (String gatewaySenderId : newGatewaySenderIds) {
        if (!oldGatewaySenderIds.contains(gatewaySenderId)) {
          mutator.addGatewaySenderId(gatewaySenderId);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - gateway sender IDs");
      }
    }

    // Alter Async Queue Ids
    final Set<String> newAsyncEventQueueIds = regionAlterArgs.getAsyncEventQueueIds();
    if (newAsyncEventQueueIds != null) {

      // Remove old async event queue ids that aren't in the new list
      Set<String> oldAsyncEventQueueIds = region.getAsyncEventQueueIds();
      if (!oldAsyncEventQueueIds.isEmpty()) {
        for (String asyncEventQueueId : oldAsyncEventQueueIds) {
          if (!newAsyncEventQueueIds.contains(asyncEventQueueId)) {
            mutator.removeAsyncEventQueueId(asyncEventQueueId);
          }
        }
      }

      // Add new async event queue ids that don't already exist
      for (String asyncEventQueueId : newAsyncEventQueueIds) {
        if (!oldAsyncEventQueueIds.contains(asyncEventQueueId)) {
          mutator.addAsyncEventQueueId(asyncEventQueueId);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - async event queue IDs");
      }
    }

    // Alter Cache Listeners
    final Set<ClassName<CacheListener>> newCacheListeners = regionAlterArgs.getCacheListeners();

    // user specified a new set of cache listeners
    if (newCacheListeners != null) {
      // remove the old ones, even if the new set includes the same class name, the init properties
      // might be different
      CacheListener[] oldCacheListeners = region.getCacheListeners();
      for (CacheListener oldCacheListener : oldCacheListeners) {
        mutator.removeCacheListener(oldCacheListener);
      }

      // Add new cache listeners
      for (ClassName<CacheListener> newCacheListener : newCacheListeners) {
        if (!newCacheListener.equals(ClassName.EMPTY)) {
          mutator.addCacheListener(newCacheListener.newInstance());
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cache listeners");
      }
    }

    final ClassName<CacheLoader> cacheLoader = regionAlterArgs.getCacheLoader();
    if (cacheLoader != null) {
      if (cacheLoader.equals(ClassName.EMPTY)) {
        mutator.setCacheLoader(null);
      } else {
        mutator.setCacheLoader(cacheLoader.newInstance());
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cache loader");
      }
    }

    final ClassName<CacheWriter> cacheWriter = regionAlterArgs.getCacheWriter();
    if (cacheWriter != null) {
      if (cacheWriter.equals(ClassName.EMPTY)) {
        mutator.setCacheWriter(null);
      } else {
        mutator.setCacheWriter(cacheWriter.newInstance());
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cache writer");
      }
    }

    return region;
  }

  @SuppressWarnings("unchecked")
  private static <K> Class<K> forName(String classToLoadName, String neededFor) {
    Class<K> loadedClass = null;
    try {
      // Set Constraints
      ClassPathLoader classPathLoader = ClassPathLoader.getLatest();
      if (classToLoadName != null && !classToLoadName.isEmpty()) {
        loadedClass = (Class<K>) classPathLoader.forName(classToLoadName);
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          CliStrings.format(CliStrings.ALTER_REGION__MSG__COULD_NOT_FIND_CLASS_0_SPECIFIED_FOR_1,
              classToLoadName, neededFor),
          e);
    } catch (ClassCastException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.ALTER_REGION__MSG__CLASS_SPECIFIED_FOR_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE,
          classToLoadName, neededFor), e);
    }

    return loadedClass;
  }

  private static <K> K newInstance(Class<K> klass, String neededFor) {
    K instance = null;
    try {
      instance = klass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.ALTER_REGION__MSG__COULD_NOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1, klass,
          neededFor), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          CliStrings.format(CliStrings.ALTER_REGION__MSG__COULD_NOT_ACCESS_CLASS_0_SPECIFIED_FOR_1,
              klass, neededFor),
          e);
    }

    return instance;
  }

  @Override
  public String getId() {
    return RegionAlterFunction.class.getName();
  }
}
