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
package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.AbstractRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.RegionPath;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/**
 * Function used by the 'alter region' gfsh command to alter a region on each
 * member.
 * 
 * @since GemFire 8.0
 */
public class RegionAlterFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = -4846425364943216425L;

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = CacheFactory.getAnyInstance();
    String memberNameOrId = CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    RegionFunctionArgs regionAlterArgs = (RegionFunctionArgs) context.getArguments();
    try {
      Region<?, ?> alteredRegion = alterRegion(cache, regionAlterArgs);
      XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", alteredRegion.getName());
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity, CliStrings.format(
          CliStrings.ALTER_REGION__MSG__REGION_0_ALTERED_ON_1, new Object[] { alteredRegion.getFullPath(), memberNameOrId })));
      
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

  private <K, V> Region<?, ?> alterRegion(Cache cache, RegionFunctionArgs regionAlterArgs) {
    final String regionPathString = regionAlterArgs.getRegionPath();
    
    RegionPath regionPath = new RegionPath(regionPathString);
    AbstractRegion region = (AbstractRegion) cache.getRegion(regionPathString);
    if (region == null) {
      throw new IllegalArgumentException(CliStrings.format(CliStrings.ALTER_REGION__MSG__REGION_DOESNT_EXIST_0,
          new Object[] { regionPath }));
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
    final RegionFunctionArgs.ExpirationAttrs newEntryExpirationIdleTime = regionAlterArgs.getEntryExpirationIdleTime();
    if (newEntryExpirationIdleTime != null) {
      mutator.setEntryIdleTimeout(parseExpirationAttributes(newEntryExpirationIdleTime, region.getEntryIdleTimeout()));
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - entry idle timeout");
      }
    }

    final RegionFunctionArgs.ExpirationAttrs newEntryExpirationTTL = regionAlterArgs.getEntryExpirationTTL();
    if (newEntryExpirationTTL != null) {
      mutator.setEntryTimeToLive(parseExpirationAttributes(newEntryExpirationTTL, region.getEntryTimeToLive()));
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - entry TTL");
      }
    }

    final RegionFunctionArgs.ExpirationAttrs newRegionExpirationIdleTime = regionAlterArgs.getRegionExpirationIdleTime();
    if (newRegionExpirationIdleTime != null) {
      mutator.setRegionIdleTimeout(parseExpirationAttributes(newRegionExpirationIdleTime, region.getRegionIdleTimeout()));
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - region idle timeout");
      }
    }

    final RegionFunctionArgs.ExpirationAttrs newRegionExpirationTTL = regionAlterArgs.getRegionExpirationTTL();
    if (newRegionExpirationTTL != null) {
      mutator.setRegionTimeToLive(parseExpirationAttributes(newRegionExpirationTTL, region.getRegionTimeToLive()));
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
    final Set<String> newCacheListenerNames = regionAlterArgs.getCacheListeners();
    if (newCacheListenerNames != null) {
      
      // Remove old cache listeners that aren't in the new list
      CacheListener[] oldCacheListeners = region.getCacheListeners();
      for (CacheListener oldCacheListener : oldCacheListeners) {
        if (!newCacheListenerNames.contains(oldCacheListener.getClass().getName())) {
          mutator.removeCacheListener(oldCacheListener);
        }
      }
      
      // Add new cache listeners that don't already exist
      for (String newCacheListenerName : newCacheListenerNames) {
        boolean nameFound = false;
        for (CacheListener oldCacheListener : oldCacheListeners) {
          if (oldCacheListener.getClass().getName().equals(newCacheListenerName)) {
            nameFound = true;
            break;
          }
        }

        if (!nameFound) {
          Class<CacheListener<K, V>> cacheListenerKlass = forName(newCacheListenerName, CliStrings.ALTER_REGION__CACHELISTENER);
          mutator.addCacheListener(newInstance(cacheListenerKlass, CliStrings.ALTER_REGION__CACHELISTENER));
        }
      }
      
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cache listeners");
      }
    }

    final String cacheLoader = regionAlterArgs.getCacheLoader();
    if (cacheLoader != null) {
      if (cacheLoader.isEmpty()) {
        mutator.setCacheLoader(null);
      } else {
        Class<CacheLoader<K, V>> cacheLoaderKlass = forName(cacheLoader, CliStrings.ALTER_REGION__CACHELOADER);
        mutator.setCacheLoader(newInstance(cacheLoaderKlass, CliStrings.ALTER_REGION__CACHELOADER));
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cache loader");
      }
    }

    final String cacheWriter = regionAlterArgs.getCacheWriter();
    if (cacheWriter != null) {
      if (cacheWriter.isEmpty()) {
        mutator.setCacheWriter(null);
      } else {
        Class<CacheWriter<K, V>> cacheWriterKlass = forName(cacheWriter, CliStrings.ALTER_REGION__CACHEWRITER);
        mutator.setCacheWriter(newInstance(cacheWriterKlass, CliStrings.ALTER_REGION__CACHEWRITER));
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cache writer");
      }
    }

    return region;
  }

  /**
   * Converts the expiration attributes passed as arguments from the command to
   * the function into a type suitable for applying to a Region.
   * 
   * @param newExpirationAttrs
   *          Attributes supplied by the command
   * @param oldExpirationAttributes
   *          Attributes currently applied to the Region.
   * 
   * @return A new pair of expiration attributes taken from the command if it
   *         was given or the current value from the Region if it was not.
   */
  private ExpirationAttributes parseExpirationAttributes(RegionFunctionArgs.ExpirationAttrs newExpirationAttrs,
      ExpirationAttributes oldExpirationAttributes) {

    ExpirationAction action = oldExpirationAttributes.getAction();
    int timeout = oldExpirationAttributes.getTimeout();

    if (newExpirationAttrs.getTime() != null) {
      timeout = newExpirationAttrs.getTime();
    }
    if (newExpirationAttrs.getAction() != null) {
      action = newExpirationAttrs.getAction();
    }

    return new ExpirationAttributes(timeout, action);
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
      throw new RuntimeException(CliStrings.format(CliStrings.ALTER_REGION__MSG__COULDNOT_FIND_CLASS_0_SPECIFIED_FOR_1,
          new Object[] { classToLoadName, neededFor }), e);
    } catch (ClassCastException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.ALTER_REGION__MSG__CLASS_SPECIFIED_FOR_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE, new Object[] {
              classToLoadName, neededFor }), e);
    }

    return loadedClass;
  }

  private static <K> K newInstance(Class<K> klass, String neededFor) {
    K instance = null;
    try {
      instance = klass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.ALTER_REGION__MSG__COULDNOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1,
          new Object[] { klass, neededFor }), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.ALTER_REGION__MSG__COULDNOT_ACCESS_CLASS_0_SPECIFIED_FOR_1,
          new Object[] { klass, neededFor }), e);
    }

    return instance;
  }

  @Override
  public String getId() {
    return RegionAlterFunction.class.getName();
  }
}
