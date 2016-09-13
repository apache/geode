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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.commands.CreateAlterDestroyRegionCommands;
import com.gemstone.gemfire.management.internal.cli.exceptions.CreateSubregionException;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.RegionPath;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/**
 *
 * @since GemFire 7.0
 */
public class RegionCreateFunction extends FunctionAdapter implements InternalEntity {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 8746830191680509335L;

  private static final String ID = RegionCreateFunction.class.getName();

  public static RegionCreateFunction INSTANCE = new RegionCreateFunction();

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache  cache          = CacheFactory.getAnyInstance();
    String memberNameOrId = CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    RegionFunctionArgs regionCreateArgs = (RegionFunctionArgs) context.getArguments();
    
    if (regionCreateArgs.isSkipIfExists()) {
      Region<Object, Object> region = cache.getRegion(regionCreateArgs.getRegionPath());
      if (region != null) {
        resultSender.lastResult(new CliFunctionResult(memberNameOrId, true, CliStrings.format(CliStrings.CREATE_REGION__MSG__SKIPPING_0_REGION_PATH_1_ALREADY_EXISTS, new Object[] {memberNameOrId, regionCreateArgs.getRegionPath()})));
        return;
      }
    }

    try {
      Region<?, ?> createdRegion = createRegion(cache, regionCreateArgs);
      XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", createdRegion.getName());
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity, CliStrings.format(CliStrings.CREATE_REGION__MSG__REGION_0_CREATED_ON_1, new Object[] {createdRegion.getFullPath(), memberNameOrId})));
    } catch (IllegalStateException e) {
      String exceptionMsg = e.getMessage();
      String localizedString = LocalizedStrings.DiskStore_IS_USED_IN_NONPERSISTENT_REGION.toLocalizedString();
      if (localizedString.equals(e.getMessage())) {
        exceptionMsg = exceptionMsg
            + " " + CliStrings.format(CliStrings.CREATE_REGION__MSG__USE_ONE_OF_THESE_SHORTCUTS_0,
                    new Object[] { String.valueOf(CreateAlterDestroyRegionCommands.PERSISTENT_OVERFLOW_SHORTCUTS)});
      }
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, null/*do not log*/));
    } catch (IllegalArgumentException e) {
      resultSender.lastResult(handleException(memberNameOrId, e.getMessage(), e));
    } catch (RegionExistsException e) {
      String exceptionMsg = CliStrings.format(CliStrings.CREATE_REGION__MSG__REGION_PATH_0_ALREADY_EXISTS_ON_1, new Object[] {regionCreateArgs.getRegionPath(), memberNameOrId});
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, e));
    } catch (CreateSubregionException e) {
      resultSender.lastResult(handleException(memberNameOrId, e.getMessage(), e));
    } catch (Exception e) {
      String exceptionMsg = e.getMessage();
      if (exceptionMsg == null) {
        exceptionMsg = CliUtil.stackTraceAsString(e);
      }
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, e));
    }
  }

  private CliFunctionResult handleException(final String memberNameOrId,
      final String exceptionMsg, final Exception e) {
    if (e != null && logger.isDebugEnabled()) {
      logger.debug(e.getMessage(), e);
    }
    if (exceptionMsg != null) {
      return new CliFunctionResult(memberNameOrId, false, exceptionMsg);
    }
    
    return new CliFunctionResult(memberNameOrId);
  }

  public static <K, V> Region<?, ?> createRegion(Cache cache, RegionFunctionArgs regionCreateArgs) {
    Region<K, V> createdRegion = null;
    
    final String regionPath = regionCreateArgs.getRegionPath();
    final RegionShortcut regionShortcut = regionCreateArgs.getRegionShortcut();
    final String useAttributesFrom      = regionCreateArgs.getUseAttributesFrom();

    // If a region path indicates a sub-region, check whether the parent region exists
    RegionPath regionPathData = new RegionPath(regionPath);
    String parentRegionPath = regionPathData.getParent();
    Region<?, ?> parentRegion = null;
    if (parentRegionPath != null && !Region.SEPARATOR.equals(parentRegionPath)) {
      parentRegion = cache.getRegion(parentRegionPath);
      if (parentRegion == null) {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__PARENT_REGION_FOR_0_DOESNOT_EXIST, new Object[] {regionPath}));
      }

      if (parentRegion.getAttributes().getPartitionAttributes() != null) {
        // For a PR, sub-regions are not supported.
        throw new CreateSubregionException(
            CliStrings.format(CliStrings.CREATE_REGION__MSG__0_IS_A_PR_CANNOT_HAVE_SUBREGIONS, parentRegion.getFullPath()));
      }
    }

    // One of Region Shortcut OR Use Attributes From has to be given
    if (regionShortcut == null && useAttributesFrom == null) {
      throw new IllegalArgumentException(CliStrings.CREATE_REGION__MSG__ONE_OF_REGIONSHORTCUT_AND_USEATTRIBUESFROM_IS_REQUIRED);
    }

    boolean isPartitioned = false;
    RegionFactory<K, V> factory = null;
    RegionAttributes<K, V> regionAttributes = null;
    if (regionShortcut != null) {
      regionAttributes = cache.getRegionAttributes(regionShortcut.toString());
      if (logger.isDebugEnabled()) {
        logger.debug("Using shortcut {} for {} region attributes : {}", regionShortcut, regionPath, regionAttributes);
      }

      if (regionAttributes == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Shortcut {} doesn't have attributes in {}", regionShortcut, cache.listRegionAttributes());
        }
        throw new IllegalStateException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COULDNOT_LOAD_REGION_ATTRIBUTES_FOR_SHORTCUT_0, regionShortcut));
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Using Manager's region attributes for {}", regionPath);
      }
      regionAttributes = regionCreateArgs.getRegionAttributes();
      if (logger.isDebugEnabled()) {
        logger.debug("Using Attributes : {}", regionAttributes);
      }
    }
    isPartitioned = regionAttributes.getPartitionAttributes() != null;

    factory = cache.createRegionFactory(regionAttributes);

    if (!isPartitioned && regionCreateArgs.hasPartitionAttributes()) {
      throw new IllegalArgumentException(
          CliStrings.format(
                  CliStrings.CREATE_REGION__MSG__OPTION_0_CAN_BE_USED_ONLY_FOR_PARTITIONEDREGION,
                  regionCreateArgs.getPartitionArgs().getUserSpecifiedPartitionAttributes()));
    }

    if (isPartitioned) {
      PartitionAttributes<K, V> partitionAttributes = extractPartitionAttributes(cache, regionAttributes, regionCreateArgs);

      DataPolicy originalDataPolicy = regionAttributes.getDataPolicy();
      factory.setPartitionAttributes(partitionAttributes);
      // We have to do this because AttributesFactory.setPartitionAttributes()
      // checks RegionAttributes.hasDataPolicy() which is set only when the data
      // policy is set explicitly
      factory.setDataPolicy(originalDataPolicy);      
    }
       
    // Set Constraints
    final String keyConstraint = regionCreateArgs.getKeyConstraint();
    final String valueConstraint = regionCreateArgs.getValueConstraint();
    if (keyConstraint != null && !keyConstraint.isEmpty()) {
      Class<K> keyConstraintClass = CliUtil.forName(keyConstraint, CliStrings.CREATE_REGION__KEYCONSTRAINT);
      factory.setKeyConstraint(keyConstraintClass);
    }

    if (valueConstraint != null && !valueConstraint.isEmpty()) {
      Class<V> valueConstraintClass = CliUtil.forName(valueConstraint, CliStrings.CREATE_REGION__VALUECONSTRAINT);
      factory.setValueConstraint(valueConstraintClass);
    }

    // Expiration attributes
    final RegionFunctionArgs.ExpirationAttrs entryExpirationIdleTime = regionCreateArgs.getEntryExpirationIdleTime();
    if (entryExpirationIdleTime != null) {
      factory.setEntryIdleTimeout(entryExpirationIdleTime.convertToExpirationAttributes());
    }
    final RegionFunctionArgs.ExpirationAttrs entryExpirationTTL = regionCreateArgs.getEntryExpirationTTL();
    if (entryExpirationTTL != null) {
      factory.setEntryTimeToLive(entryExpirationTTL.convertToExpirationAttributes());
    }
    final RegionFunctionArgs.ExpirationAttrs regionExpirationIdleTime = regionCreateArgs.getRegionExpirationIdleTime();
    if (regionExpirationIdleTime != null) {
      factory.setEntryIdleTimeout(regionExpirationIdleTime.convertToExpirationAttributes());
    }
    final RegionFunctionArgs.ExpirationAttrs regionExpirationTTL = regionCreateArgs.getRegionExpirationTTL();
    if (regionExpirationTTL != null) {
      factory.setEntryTimeToLive(regionExpirationTTL.convertToExpirationAttributes());
    }

    // Associate a Disk Store
    final String diskStore = regionCreateArgs.getDiskStore();
    if (diskStore != null && !diskStore.isEmpty()) {
      factory.setDiskStoreName(diskStore);
    }
    if (regionCreateArgs.isSetDiskSynchronous()) {
      factory.setDiskSynchronous(regionCreateArgs.isDiskSynchronous());
    }

    if (regionCreateArgs.isSetOffHeap()) {
      factory.setOffHeap(regionCreateArgs.isOffHeap());
    }

    // Set stats enabled
    if (regionCreateArgs.isSetStatisticsEnabled()) {
      factory.setStatisticsEnabled(regionCreateArgs.isStatisticsEnabled());
    }

    // Set conflation
    if (regionCreateArgs.isSetEnableAsyncConflation()) {
      factory.setEnableAsyncConflation(regionCreateArgs.isEnableAsyncConflation());
    }
    if (regionCreateArgs.isSetEnableSubscriptionConflation()) {
      factory.setEnableSubscriptionConflation(regionCreateArgs.isEnableSubscriptionConflation());
    }

    // Gateway Sender Ids
    final Set<String> gatewaySenderIds = regionCreateArgs.getGatewaySenderIds();
    if (gatewaySenderIds != null && !gatewaySenderIds.isEmpty()) {
      for (String gatewaySenderId : gatewaySenderIds) {
        factory.addGatewaySenderId(gatewaySenderId);
      }
    }

    // Async Queue Ids
    final Set<String> asyncEventQueueIds = regionCreateArgs.getAsyncEventQueueIds();
    if (asyncEventQueueIds != null && !asyncEventQueueIds.isEmpty()) {
      for (String asyncEventQueueId : asyncEventQueueIds) {
        factory.addAsyncEventQueueId(asyncEventQueueId);
      }
    }

    // concurrency check enabled & concurrency level
    if (regionCreateArgs.isSetConcurrencyChecksEnabled()) {
      factory.setConcurrencyChecksEnabled(regionCreateArgs.isConcurrencyChecksEnabled());
    }
    if (regionCreateArgs.isSetConcurrencyLevel()) {
      factory.setConcurrencyLevel(regionCreateArgs.getConcurrencyLevel());
    }

    // cloning enabled for delta
    if (regionCreateArgs.isSetCloningEnabled()) {
      factory.setCloningEnabled(regionCreateArgs.isCloningEnabled());
    }
    
    // multicast enabled for replication
    if (regionCreateArgs.isSetMcastEnabled()) {
      factory.setMulticastEnabled(regionCreateArgs.isMcastEnabled());
    }

    // Set plugins
    final Set<String> cacheListeners = regionCreateArgs.getCacheListeners();
    if (cacheListeners != null && !cacheListeners.isEmpty()) {
      for (String cacheListener : cacheListeners) {
        Class<CacheListener<K, V>> cacheListenerKlass = CliUtil.forName(cacheListener, CliStrings.CREATE_REGION__CACHELISTENER);
        factory.addCacheListener(CliUtil.newInstance(cacheListenerKlass, CliStrings.CREATE_REGION__CACHELISTENER));
      }
    }

    // Compression provider
    if(regionCreateArgs.isSetCompressor()) {
      Class<Compressor> compressorKlass = CliUtil.forName(regionCreateArgs.getCompressor(), CliStrings.CREATE_REGION__COMPRESSOR);
      factory.setCompressor(CliUtil.newInstance(compressorKlass, CliStrings.CREATE_REGION__COMPRESSOR));
    }
    
    final String cacheLoader = regionCreateArgs.getCacheLoader();
    if (cacheLoader != null) {
      Class<CacheLoader<K, V>> cacheLoaderKlass = CliUtil.forName(cacheLoader, CliStrings.CREATE_REGION__CACHELOADER);
      factory.setCacheLoader(CliUtil.newInstance(cacheLoaderKlass, CliStrings.CREATE_REGION__CACHELOADER));
    }

    final String cacheWriter = regionCreateArgs.getCacheWriter();
    if (cacheWriter != null) {
      Class<CacheWriter<K, V>> cacheWriterKlass = CliUtil.forName(cacheWriter, CliStrings.CREATE_REGION__CACHEWRITER);
      factory.setCacheWriter(CliUtil.newInstance(cacheWriterKlass, CliStrings.CREATE_REGION__CACHEWRITER));
    }
    
    String regionName = regionPathData.getName();
    
    if (parentRegion != null) {
      createdRegion = factory.createSubregion(parentRegion, regionName);
    } else {
      createdRegion = factory.create(regionName);
    }

    return createdRegion;
  }

  @SuppressWarnings("unchecked")
  private static <K, V> PartitionAttributes<K, V> extractPartitionAttributes(Cache cache, RegionAttributes<K, V> regionAttributes, RegionFunctionArgs regionCreateArgs) {
    RegionFunctionArgs.PartitionArgs partitionArgs = regionCreateArgs.getPartitionArgs();

    PartitionAttributesFactory<K, V> prAttrFactory = null;

    PartitionAttributes<K, V> partitionAttributes = regionAttributes.getPartitionAttributes();
    if (partitionAttributes != null) {
      prAttrFactory = new PartitionAttributesFactory<K, V>(partitionAttributes);
    } else {
      prAttrFactory = new PartitionAttributesFactory<K, V>();
    }

    String colocatedWith = partitionArgs.getPrColocatedWith();
    if (colocatedWith != null) {
      Region<Object, Object> colocatedWithRegion = cache.getRegion(colocatedWith);
      if (colocatedWithRegion == null) {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COLOCATEDWITH_REGION_0_DOESNOT_EXIST, colocatedWith));
      }
      if (!colocatedWithRegion.getAttributes().getDataPolicy().withPartitioning()) {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COLOCATEDWITH_REGION_0_IS_NOT_PARTITIONEDREGION, colocatedWith));
      }
      prAttrFactory.setColocatedWith(colocatedWith);
    }
    if (partitionArgs.isSetPRLocalMaxMemory()) {
      prAttrFactory.setLocalMaxMemory(partitionArgs.getPrLocalMaxMemory());
    }
    if (partitionArgs.isSetPRTotalMaxMemory()) {
      prAttrFactory.setTotalMaxMemory(partitionArgs.getPrTotalMaxMemory());
    }
    if (partitionArgs.isSetPRTotalNumBuckets()) {
      prAttrFactory.setTotalNumBuckets(partitionArgs.getPrTotalNumBuckets());
    }
    if (partitionArgs.isSetPRRedundantCopies()) {
      prAttrFactory.setRedundantCopies(partitionArgs.getPrRedundantCopies());
    }
    if (partitionArgs.isSetPRRecoveryDelay()) {
      prAttrFactory.setRecoveryDelay(partitionArgs.getPrRecoveryDelay());
    }
    if (partitionArgs.isSetPRStartupRecoveryDelay()) {
      prAttrFactory.setStartupRecoveryDelay(partitionArgs.getPrStartupRecoveryDelay());
    }

    return prAttrFactory.create();
  }

  @Override
  public String getId() {
    return ID;
  }
}
