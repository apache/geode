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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.commands.RegionCommandsUtils;
import org.apache.geode.management.internal.cli.domain.ClassName;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.RegionPath;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/**
 *
 * @since GemFire 7.0
 */
public class RegionCreateFunction implements InternalFunction {

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

    Cache cache = context.getCache();
    String memberNameOrId = context.getMemberName();

    RegionFunctionArgs regionCreateArgs = (RegionFunctionArgs) context.getArguments();

    if (regionCreateArgs.isIfNotExists()) {
      Region<Object, Object> region = cache.getRegion(regionCreateArgs.getRegionPath());
      if (region != null) {
        resultSender.lastResult(new CliFunctionResult(memberNameOrId, true,
            CliStrings.format(
                CliStrings.CREATE_REGION__MSG__SKIPPING_0_REGION_PATH_1_ALREADY_EXISTS,
                memberNameOrId, regionCreateArgs.getRegionPath())));
        return;
      }
    }

    try {
      Region<?, ?> createdRegion = createRegion(cache, regionCreateArgs);
      XmlEntity xmlEntity = getXmlEntityForRegion(createdRegion);

      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity,
          CliStrings.format(CliStrings.CREATE_REGION__MSG__REGION_0_CREATED_ON_1,
              createdRegion.getFullPath(), memberNameOrId)));
    } catch (IllegalStateException e) {
      String exceptionMsg = e.getMessage();
      String localizedString =
          "Only regions with persistence or overflow to disk can specify DiskStore";
      if (localizedString.equals(e.getMessage())) {
        exceptionMsg = exceptionMsg + " "
            + CliStrings.format(CliStrings.CREATE_REGION__MSG__USE_ONE_OF_THESE_SHORTCUTS_0,
                new Object[] {String.valueOf(RegionCommandsUtils.PERSISTENT_OVERFLOW_SHORTCUTS)});
      }
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, null/* do not log */));
    } catch (IllegalArgumentException e) {
      resultSender.lastResult(handleException(memberNameOrId, e.getMessage(), e));
    } catch (RegionExistsException e) {
      String exceptionMsg =
          CliStrings.format(CliStrings.CREATE_REGION__MSG__REGION_PATH_0_ALREADY_EXISTS_ON_1,
              regionCreateArgs.getRegionPath(), memberNameOrId);
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, e));
    } catch (Exception e) {
      String exceptionMsg = e.getMessage();
      if (exceptionMsg == null) {
        exceptionMsg = ExceptionUtils.getStackTrace(e);
      }
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, e));
    }
  }

  private CliFunctionResult handleException(final String memberNameOrId, final String exceptionMsg,
      final Exception e) {
    if (e != null && logger.isDebugEnabled()) {
      logger.debug(e.getMessage(), e);
    }

    if (exceptionMsg != null) {
      return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
          exceptionMsg);
    }

    return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR);
  }

  private XmlEntity getXmlEntityForRegion(Region<?, ?> region) {
    Region<?, ?> curRegion = region;
    while (curRegion != null && curRegion.getParentRegion() != null) {
      curRegion = curRegion.getParentRegion();
    }

    return new XmlEntity(CacheXml.REGION, "name", curRegion.getName());
  }

  private <K, V> Region<?, ?> createRegion(Cache cache, RegionFunctionArgs regionCreateArgs) {
    Region<K, V> createdRegion = null;

    final RegionShortcut regionShortcut = regionCreateArgs.getRegionShortcut();

    // create the region factory using the arguments
    boolean isPartitioned = false;
    RegionFactory<K, V> factory = null;
    RegionAttributes<K, V> regionAttributes = null;
    if (regionShortcut != null) {
      regionAttributes = cache.getRegionAttributes(regionShortcut.toString());
      regionCreateArgs.setRegionAttributes(regionAttributes);
    } else {
      regionAttributes = regionCreateArgs.getRegionAttributes();
    }

    isPartitioned = regionAttributes.getPartitionAttributes() != null;
    factory = cache.createRegionFactory(regionAttributes);

    if (isPartitioned) {
      PartitionAttributes<K, V> partitionAttributes =
          extractPartitionAttributes(cache, regionAttributes, regionCreateArgs);

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
      Class<K> keyConstraintClass =
          CliUtil.forName(keyConstraint, CliStrings.CREATE_REGION__KEYCONSTRAINT);
      factory.setKeyConstraint(keyConstraintClass);
    }

    if (valueConstraint != null && !valueConstraint.isEmpty()) {
      Class<V> valueConstraintClass =
          CliUtil.forName(valueConstraint, CliStrings.CREATE_REGION__VALUECONSTRAINT);
      factory.setValueConstraint(valueConstraintClass);
    }

    // Expiration attributes
    final RegionFunctionArgs.ExpirationAttrs entryExpirationIdleTime =
        regionCreateArgs.getEntryExpirationIdleTime();
    if (entryExpirationIdleTime.isTimeOrActionSet()) {
      factory.setEntryIdleTimeout(entryExpirationIdleTime.getExpirationAttributes());
    }

    if (regionCreateArgs.getEntryIdleTimeCustomExpiry() != null) {
      factory.setCustomEntryIdleTimeout(
          regionCreateArgs.getEntryIdleTimeCustomExpiry().newInstance(cache));
    }

    if (regionCreateArgs.getEntryTTLCustomExpiry() != null) {
      factory
          .setCustomEntryTimeToLive(regionCreateArgs.getEntryTTLCustomExpiry().newInstance(cache));
    }

    final RegionFunctionArgs.ExpirationAttrs entryExpirationTTL =
        regionCreateArgs.getEntryExpirationTTL();
    if (entryExpirationTTL.isTimeOrActionSet()) {
      factory.setEntryTimeToLive(entryExpirationTTL.getExpirationAttributes());
    }
    final RegionFunctionArgs.ExpirationAttrs regionExpirationIdleTime =
        regionCreateArgs.getRegionExpirationIdleTime();
    if (regionExpirationIdleTime.isTimeOrActionSet()) {
      factory.setRegionIdleTimeout(regionExpirationIdleTime.getExpirationAttributes());
    }
    final RegionFunctionArgs.ExpirationAttrs regionExpirationTTL =
        regionCreateArgs.getRegionExpirationTTL();
    if (regionExpirationTTL.isTimeOrActionSet()) {
      factory.setRegionTimeToLive(regionExpirationTTL.getExpirationAttributes());
    }

    EvictionAttributes evictionAttributes = regionCreateArgs.getEvictionAttributes();
    if (evictionAttributes != null) {
      ObjectSizer sizer = evictionAttributes.getObjectSizer();
      if (sizer != null && !(sizer instanceof Declarable)) {
        throw new IllegalArgumentException(
            CliStrings.CREATE_REGION__MSG__OBJECT_SIZER_MUST_BE_OBJECTSIZER_AND_DECLARABLE);
      }
      factory.setEvictionAttributes(evictionAttributes);
    }

    // Associate a Disk Store
    final String diskStore = regionCreateArgs.getDiskStore();
    if (diskStore != null && !diskStore.isEmpty()) {
      factory.setDiskStoreName(diskStore);
    }

    if (regionCreateArgs.isDiskSynchronous() != null) {
      factory.setDiskSynchronous(regionCreateArgs.isDiskSynchronous());
    }

    if (regionCreateArgs.isOffHeap() != null) {
      factory.setOffHeap(regionCreateArgs.isOffHeap());
    }

    if (regionCreateArgs.isStatisticsEnabled() != null) {
      factory.setStatisticsEnabled(regionCreateArgs.isStatisticsEnabled());
    }

    if (regionCreateArgs.isEnableAsyncConflation() != null) {
      factory.setEnableAsyncConflation(regionCreateArgs.isEnableAsyncConflation());
    }

    if (regionCreateArgs.isEnableSubscriptionConflation() != null) {
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

    if (regionCreateArgs.isConcurrencyChecksEnabled() != null) {
      factory.setConcurrencyChecksEnabled(regionCreateArgs.isConcurrencyChecksEnabled());
    }

    if (regionCreateArgs.getConcurrencyLevel() != null) {
      factory.setConcurrencyLevel(regionCreateArgs.getConcurrencyLevel());
    }

    if (regionCreateArgs.isCloningEnabled() != null) {
      factory.setCloningEnabled(regionCreateArgs.isCloningEnabled());
    }

    if (regionCreateArgs.isMcastEnabled() != null) {
      factory.setMulticastEnabled(regionCreateArgs.isMcastEnabled());
    }

    // Set plugins
    final Set<ClassName<CacheListener>> cacheListeners = regionCreateArgs.getCacheListeners();
    if (cacheListeners != null && !cacheListeners.isEmpty()) {
      List<CacheListener<K, V>> newListeners = new ArrayList<>();
      for (ClassName<CacheListener> cacheListener : cacheListeners) {
        newListeners.add(cacheListener.newInstance(cache));
      }
      factory.initCacheListeners(newListeners.toArray(new CacheListener[0]));
    }

    // Compression provider
    if (regionCreateArgs.getCompressor() != null) {
      Class<Compressor> compressorKlass =
          CliUtil.forName(regionCreateArgs.getCompressor(), CliStrings.CREATE_REGION__COMPRESSOR);
      factory.setCompressor(
          CliUtil.newInstance(compressorKlass, CliStrings.CREATE_REGION__COMPRESSOR));
    }

    final ClassName<CacheLoader> cacheLoader = regionCreateArgs.getCacheLoader();
    if (cacheLoader != null) {
      factory.setCacheLoader(cacheLoader.newInstance(cache));
    }

    final ClassName<CacheWriter> cacheWriter = regionCreateArgs.getCacheWriter();
    if (cacheWriter != null) {
      factory.setCacheWriter(cacheWriter.newInstance(cache));
    }

    // If a region path indicates a sub-region,
    final String regionPath = regionCreateArgs.getRegionPath();
    RegionPath regionPathData = new RegionPath(regionPath);
    String regionName = regionPathData.getName();
    String parentRegionPath = regionPathData.getParent();
    if (parentRegionPath != null && !Region.SEPARATOR.equals(parentRegionPath)) {
      Region<?, ?> parentRegion = cache.getRegion(parentRegionPath);
      createdRegion = factory.createSubregion(parentRegion, regionName);
    } else {
      createdRegion = factory.create(regionName);
    }

    return createdRegion;
  }

  @SuppressWarnings("unchecked")
  private static <K, V> PartitionAttributes<K, V> extractPartitionAttributes(Cache cache,
      RegionAttributes<K, V> regionAttributes, RegionFunctionArgs regionCreateArgs) {
    RegionFunctionArgs.PartitionArgs partitionArgs = regionCreateArgs.getPartitionArgs();

    PartitionAttributesFactory<K, V> prAttrFactory = null;

    PartitionAttributes<K, V> partitionAttributes = regionAttributes.getPartitionAttributes();
    if (partitionAttributes != null) {
      prAttrFactory = new PartitionAttributesFactory<>(partitionAttributes);
    } else {
      prAttrFactory = new PartitionAttributesFactory<>();
    }

    String colocatedWith = partitionArgs.getPrColocatedWith();
    if (colocatedWith != null) {
      Region<Object, Object> colocatedWithRegion = cache.getRegion(colocatedWith);
      if (colocatedWithRegion == null) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__COLOCATEDWITH_REGION_0_DOES_NOT_EXIST, colocatedWith));
      }
      if (!colocatedWithRegion.getAttributes().getDataPolicy().withPartitioning()) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__COLOCATEDWITH_REGION_0_IS_NOT_PARTITIONEDREGION,
            colocatedWith));
      }
      prAttrFactory.setColocatedWith(colocatedWith);
    }
    if (partitionArgs.getPrLocalMaxMemory() != null) {
      prAttrFactory.setLocalMaxMemory(partitionArgs.getPrLocalMaxMemory());
    }
    if (partitionArgs.getPrTotalMaxMemory() != null) {
      prAttrFactory.setTotalMaxMemory(partitionArgs.getPrTotalMaxMemory());
    }
    if (partitionArgs.getPrTotalNumBuckets() != null) {
      prAttrFactory.setTotalNumBuckets(partitionArgs.getPrTotalNumBuckets());
    }
    if (partitionArgs.getPrRedundantCopies() != null) {
      prAttrFactory.setRedundantCopies(partitionArgs.getPrRedundantCopies());
    }
    if (partitionArgs.getPrRecoveryDelay() != null) {
      prAttrFactory.setRecoveryDelay(partitionArgs.getPrRecoveryDelay());
    }
    if (partitionArgs.getPrStartupRecoveryDelay() != null) {
      prAttrFactory.setStartupRecoveryDelay(partitionArgs.getPrStartupRecoveryDelay());
    }

    if (regionCreateArgs.getPartitionArgs().getPartitionResolver() != null) {
      Class<PartitionResolver> partitionResolverClass =
          forName(regionCreateArgs.getPartitionArgs().getPartitionResolver(),
              CliStrings.CREATE_REGION__PARTITION_RESOLVER);
      prAttrFactory
          .setPartitionResolver((PartitionResolver<K, V>) newInstance(partitionResolverClass,
              CliStrings.CREATE_REGION__PARTITION_RESOLVER));
    }
    return prAttrFactory.create();
  }


  private static Class<PartitionResolver> forName(String className, String neededFor) {
    if (StringUtils.isBlank(className)) {
      throw new IllegalArgumentException(CliStrings
          .format(CliStrings.CREATE_REGION__MSG__INVALID_PARTITION_RESOLVER, className, neededFor));
    }
    try {
      return (Class<PartitionResolver>) ClassPathLoader.getLatest().forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_REGION_PARTITION_RESOLVER__MSG__COULD_NOT_FIND_CLASS_0_SPECIFIED_FOR_1,
          className, neededFor), e);
    } catch (ClassCastException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__PARTITION_RESOLVER__CLASS_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE,
          className, neededFor), e);
    }
  }

  private static PartitionResolver newInstance(Class<PartitionResolver> klass, String neededFor) {
    try {
      return klass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__PARTITION_RESOLVER__COULD_NOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1,
          klass, neededFor), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__PARTITION_RESOLVER__COULD_NOT_ACCESS_CLASS_0_SPECIFIED_FOR_1,
          klass, neededFor), e);
    }
  }

  @Override
  public String getId() {
    return ID;
  }
}
