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
package org.apache.geode.management.internal.cli.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.compression.Compressor;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.RegionAttributesData;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.FetchRegionAttributesFunction;
import org.apache.geode.management.internal.cli.functions.RegionCreateFunction;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.util.RegionPath;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateRegionCommand implements GfshCommand {

  @CliCommand(value = CliStrings.CREATE_REGION, help = CliStrings.CREATE_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public Result createRegion(
      @CliOption(key = CliStrings.CREATE_REGION__REGION, mandatory = true,
          help = CliStrings.CREATE_REGION__REGION__HELP) String regionPath,
      @CliOption(key = CliStrings.CREATE_REGION__REGIONSHORTCUT,
          help = CliStrings.CREATE_REGION__REGIONSHORTCUT__HELP) RegionShortcut regionShortcut,
      @CliOption(key = CliStrings.CREATE_REGION__USEATTRIBUTESFROM,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.CREATE_REGION__USEATTRIBUTESFROM__HELP) String useAttributesFrom,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_REGION__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.CREATE_REGION__SKIPIFEXISTS, unspecifiedDefaultValue = "true",
          specifiedDefaultValue = "true",
          help = CliStrings.CREATE_REGION__SKIPIFEXISTS__HELP) boolean skipIfExists,

      // the following should all be in alphabetical order according to
      // their key string
      @CliOption(key = CliStrings.CREATE_REGION__ASYNCEVENTQUEUEID,
          help = CliStrings.CREATE_REGION__ASYNCEVENTQUEUEID__HELP) String[] asyncEventQueueIds,
      @CliOption(key = CliStrings.CREATE_REGION__CACHELISTENER,
          help = CliStrings.CREATE_REGION__CACHELISTENER__HELP) String[] cacheListener,
      @CliOption(key = CliStrings.CREATE_REGION__CACHELOADER,
          help = CliStrings.CREATE_REGION__CACHELOADER__HELP) String cacheLoader,
      @CliOption(key = CliStrings.CREATE_REGION__CACHEWRITER,
          help = CliStrings.CREATE_REGION__CACHEWRITER__HELP) String cacheWriter,
      @CliOption(key = CliStrings.CREATE_REGION__COLOCATEDWITH,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.CREATE_REGION__COLOCATEDWITH__HELP) String prColocatedWith,
      @CliOption(key = CliStrings.CREATE_REGION__COMPRESSOR,
          help = CliStrings.CREATE_REGION__COMPRESSOR__HELP) String compressor,
      @CliOption(key = CliStrings.CREATE_REGION__CONCURRENCYLEVEL,
          help = CliStrings.CREATE_REGION__CONCURRENCYLEVEL__HELP) Integer concurrencyLevel,
      @CliOption(key = CliStrings.CREATE_REGION__DISKSTORE,
          help = CliStrings.CREATE_REGION__DISKSTORE__HELP) String diskStore,
      @CliOption(key = CliStrings.CREATE_REGION__ENABLEASYNCCONFLATION,
          help = CliStrings.CREATE_REGION__ENABLEASYNCCONFLATION__HELP) Boolean enableAsyncConflation,
      @CliOption(key = CliStrings.CREATE_REGION__CLONINGENABLED,
          help = CliStrings.CREATE_REGION__CLONINGENABLED__HELP) Boolean cloningEnabled,
      @CliOption(key = CliStrings.CREATE_REGION__CONCURRENCYCHECKSENABLED,
          help = CliStrings.CREATE_REGION__CONCURRENCYCHECKSENABLED__HELP) Boolean concurrencyChecksEnabled,
      @CliOption(key = CliStrings.CREATE_REGION__MULTICASTENABLED,
          help = CliStrings.CREATE_REGION__MULTICASTENABLED__HELP) Boolean mcastEnabled,
      @CliOption(key = CliStrings.CREATE_REGION__STATISTICSENABLED,
          help = CliStrings.CREATE_REGION__STATISTICSENABLED__HELP) Boolean statisticsEnabled,
      @CliOption(key = CliStrings.CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION,
          help = CliStrings.CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION__HELP) Boolean enableSubscriptionConflation,
      @CliOption(key = CliStrings.CREATE_REGION__DISKSYNCHRONOUS,
          help = CliStrings.CREATE_REGION__DISKSYNCHRONOUS__HELP) Boolean diskSynchronous,
      @CliOption(key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME,
          help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME__HELP) Integer entryExpirationIdleTime,
      @CliOption(key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION,
          help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION__HELP) String entryExpirationIdleTimeAction,
      @CliOption(key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE,
          help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE__HELP) Integer entryExpirationTTL,
      @CliOption(key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION,
          help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION__HELP) String entryExpirationTTLAction,
      @CliOption(key = CliStrings.CREATE_REGION__GATEWAYSENDERID,
          help = CliStrings.CREATE_REGION__GATEWAYSENDERID__HELP) String[] gatewaySenderIds,
      @CliOption(key = CliStrings.CREATE_REGION__KEYCONSTRAINT,
          help = CliStrings.CREATE_REGION__KEYCONSTRAINT__HELP) String keyConstraint,
      @CliOption(key = CliStrings.CREATE_REGION__LOCALMAXMEMORY,
          help = CliStrings.CREATE_REGION__LOCALMAXMEMORY__HELP) Integer prLocalMaxMemory,
      @CliOption(key = CliStrings.CREATE_REGION__OFF_HEAP, specifiedDefaultValue = "true",
          help = CliStrings.CREATE_REGION__OFF_HEAP__HELP) Boolean offHeap,
      @CliOption(key = CliStrings.CREATE_REGION__PARTITION_RESOLVER,
          help = CliStrings.CREATE_REGION__PARTITION_RESOLVER__HELP) String partitionResolver,
      @CliOption(key = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIME,
          help = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIME__HELP) Integer regionExpirationIdleTime,
      @CliOption(key = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION,
          help = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION__HELP) String regionExpirationIdleTimeAction,
      @CliOption(key = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL,
          help = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL__HELP) Integer regionExpirationTTL,
      @CliOption(key = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION,
          help = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION__HELP) String regionExpirationTTLAction,
      @CliOption(key = CliStrings.CREATE_REGION__RECOVERYDELAY,
          help = CliStrings.CREATE_REGION__RECOVERYDELAY__HELP) Long prRecoveryDelay,
      @CliOption(key = CliStrings.CREATE_REGION__REDUNDANTCOPIES,
          help = CliStrings.CREATE_REGION__REDUNDANTCOPIES__HELP) Integer prRedundantCopies,
      @CliOption(key = CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY,
          help = CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY__HELP) Long prStartupRecoveryDelay,
      @CliOption(key = CliStrings.CREATE_REGION__TOTALMAXMEMORY,
          help = CliStrings.CREATE_REGION__TOTALMAXMEMORY__HELP) Long prTotalMaxMemory,
      @CliOption(key = CliStrings.CREATE_REGION__TOTALNUMBUCKETS,
          help = CliStrings.CREATE_REGION__TOTALNUMBUCKETS__HELP) Integer prTotalNumBuckets,
      @CliOption(key = CliStrings.CREATE_REGION__VALUECONSTRAINT,
          help = CliStrings.CREATE_REGION__VALUECONSTRAINT__HELP) String valueConstraint
  // NOTICE: keep the region attributes params in alphabetical order
  ) {
    Result result;
    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();

    try {
      InternalCache cache = getCache();

      if (regionShortcut != null && useAttributesFrom != null) {
        throw new IllegalArgumentException(
            CliStrings.CREATE_REGION__MSG__ONLY_ONE_OF_REGIONSHORTCUT_AND_USEATTRIBUESFROM_CAN_BE_SPECIFIED);
      } else if (regionShortcut == null && useAttributesFrom == null) {
        throw new IllegalArgumentException(
            CliStrings.CREATE_REGION__MSG__ONE_OF_REGIONSHORTCUT_AND_USEATTRIBUTESFROM_IS_REQUIRED);
      }

      validateRegionPathAndParent(cache, regionPath);
      RegionCommandsUtils.validateGroups(cache, groups);

      RegionFunctionArgs.ExpirationAttrs entryIdle = null;
      if (entryExpirationIdleTime != null) {
        entryIdle = new RegionFunctionArgs.ExpirationAttrs(
            RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_IDLE, entryExpirationIdleTime,
            entryExpirationIdleTimeAction);
      }
      RegionFunctionArgs.ExpirationAttrs entryTTL = null;
      if (entryExpirationTTL != null) {
        entryTTL = new RegionFunctionArgs.ExpirationAttrs(
            RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_TTL, entryExpirationTTL,
            entryExpirationTTLAction);
      }
      RegionFunctionArgs.ExpirationAttrs regionIdle = null;
      if (regionExpirationIdleTime != null) {
        regionIdle = new RegionFunctionArgs.ExpirationAttrs(
            RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_IDLE, regionExpirationIdleTime,
            regionExpirationIdleTimeAction);
      }
      RegionFunctionArgs.ExpirationAttrs regionTTL = null;
      if (regionExpirationTTL != null) {
        regionTTL = new RegionFunctionArgs.ExpirationAttrs(
            RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_TTL, regionExpirationTTL,
            regionExpirationTTLAction);
      }

      RegionFunctionArgs regionFunctionArgs;
      if (useAttributesFrom != null) {
        if (!regionExists(cache, useAttributesFrom)) {
          throw new IllegalArgumentException(CliStrings.format(
              CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND,
              CliStrings.CREATE_REGION__USEATTRIBUTESFROM, useAttributesFrom));
        }

        FetchRegionAttributesFunction.FetchRegionAttributesFunctionResult<Object, Object> regionAttributesResult =
            getRegionAttributes(cache, useAttributesFrom);
        RegionAttributes<?, ?> regionAttributes = regionAttributesResult.getRegionAttributes();

        // give preference to user specified plugins than the ones retrieved from other region
        String[] cacheListenerClasses = cacheListener != null && cacheListener.length != 0
            ? cacheListener : regionAttributesResult.getCacheListenerClasses();
        String cacheLoaderClass =
            cacheLoader != null ? cacheLoader : regionAttributesResult.getCacheLoaderClass();
        String cacheWriterClass =
            cacheWriter != null ? cacheWriter : regionAttributesResult.getCacheWriterClass();

        regionFunctionArgs = new RegionFunctionArgs(regionPath, useAttributesFrom, skipIfExists,
            keyConstraint, valueConstraint, statisticsEnabled, entryIdle, entryTTL, regionIdle,
            regionTTL, diskStore, diskSynchronous, enableAsyncConflation,
            enableSubscriptionConflation, cacheListenerClasses, cacheLoaderClass, cacheWriterClass,
            asyncEventQueueIds, gatewaySenderIds, concurrencyChecksEnabled, cloningEnabled,
            concurrencyLevel, prColocatedWith, prLocalMaxMemory, prRecoveryDelay, prRedundantCopies,
            prStartupRecoveryDelay, prTotalMaxMemory, prTotalNumBuckets, offHeap, mcastEnabled,
            regionAttributes, partitionResolver);

        if (regionAttributes.getPartitionAttributes() == null
            && regionFunctionArgs.hasPartitionAttributes()) {
          throw new IllegalArgumentException(CliStrings.format(
              CliStrings.CREATE_REGION__MSG__OPTION_0_CAN_BE_USED_ONLY_FOR_PARTITIONEDREGION,
              regionFunctionArgs.getPartitionArgs().getUserSpecifiedPartitionAttributes()) + " "
              + CliStrings.format(CliStrings.CREATE_REGION__MSG__0_IS_NOT_A_PARITIONEDREGION,
                  useAttributesFrom));
        }
      } else {
        regionFunctionArgs = new RegionFunctionArgs(regionPath, regionShortcut, useAttributesFrom,
            skipIfExists, keyConstraint, valueConstraint, statisticsEnabled, entryIdle, entryTTL,
            regionIdle, regionTTL, diskStore, diskSynchronous, enableAsyncConflation,
            enableSubscriptionConflation, cacheListener, cacheLoader, cacheWriter,
            asyncEventQueueIds, gatewaySenderIds, concurrencyChecksEnabled, cloningEnabled,
            concurrencyLevel, prColocatedWith, prLocalMaxMemory, prRecoveryDelay, prRedundantCopies,
            prStartupRecoveryDelay, prTotalMaxMemory, prTotalNumBuckets, null, compressor, offHeap,
            mcastEnabled, partitionResolver);

        if (!regionShortcut.name().startsWith("PARTITION")
            && regionFunctionArgs.hasPartitionAttributes()) {
          throw new IllegalArgumentException(CliStrings.format(
              CliStrings.CREATE_REGION__MSG__OPTION_0_CAN_BE_USED_ONLY_FOR_PARTITIONEDREGION,
              regionFunctionArgs.getPartitionArgs().getUserSpecifiedPartitionAttributes()) + " "
              + CliStrings.format(CliStrings.CREATE_REGION__MSG__0_IS_NOT_A_PARITIONEDREGION,
                  regionPath));
        }
      }

      // Do we prefer to validate or authorize first?
      validateRegionFunctionArgs(cache, regionFunctionArgs);
      if (isPersistentShortcut(regionFunctionArgs.getRegionShortcut())
          || isAttributePersistent(regionFunctionArgs.getRegionAttributes())) {
        getSecurityService().authorize(ResourcePermission.Resource.CLUSTER,
            ResourcePermission.Operation.WRITE, ResourcePermission.Target.DISK);
      }

      Set<DistributedMember> membersToCreateRegionOn;
      if (groups != null && groups.length != 0) {
        membersToCreateRegionOn = CliUtil.getDistributedMembersByGroup(cache, groups);
        // have only normal members from the group
        membersToCreateRegionOn
            .removeIf(distributedMember -> ((InternalDistributedMember) distributedMember)
                .getVmKind() == DistributionManager.LOCATOR_DM_TYPE);
      } else {
        membersToCreateRegionOn = CliUtil.getAllNormalMembers(cache);
      }

      if (membersToCreateRegionOn.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_CACHING_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> resultCollector = CliUtil.executeFunction(RegionCreateFunction.INSTANCE,
          regionFunctionArgs, membersToCreateRegionOn);
      @SuppressWarnings("unchecked")
      List<CliFunctionResult> regionCreateResults =
          (List<CliFunctionResult>) resultCollector.getResult();

      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      final String errorPrefix = "ERROR: ";
      for (CliFunctionResult regionCreateResult : regionCreateResults) {
        boolean success = regionCreateResult.isSuccessful();
        tabularResultData.accumulate("Member", regionCreateResult.getMemberIdOrName());
        tabularResultData.accumulate("Status",
            (success ? "" : errorPrefix) + regionCreateResult.getMessage());

        if (success) {
          xmlEntity.set(regionCreateResult.getXmlEntity());
        } else {
          tabularResultData.setStatus(Result.Status.ERROR);
        }
      }
      result = ResultBuilder.buildResult(tabularResultData);
      verifyDistributedRegionMbean(cache, regionPath);

    } catch (IllegalArgumentException | IllegalStateException e) {
      LogWrapper.getInstance().info(e.getMessage());
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    }

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), groups));
    }

    return result;
  }

  private boolean verifyDistributedRegionMbean(InternalCache cache, String regionName) {
    int federationInterval =
        cache.getInternalDistributedSystem().getConfig().getJmxManagerUpdateRate();
    long timeEnd = System.currentTimeMillis() + federationInterval + 50;

    for (; System.currentTimeMillis() <= timeEnd;) {
      try {
        DistributedRegionMXBean bean =
            ManagementService.getManagementService(cache).getDistributedRegionMXBean(regionName);
        if (bean == null) {
          bean = ManagementService.getManagementService(cache)
              .getDistributedRegionMXBean(Region.SEPARATOR + regionName);
        }
        if (bean != null) {
          return true;
        } else {
          Thread.sleep(2);
        }
      } catch (Exception ignored) {
      }
    }
    return false;
  }

  void validateRegionFunctionArgs(InternalCache cache, RegionFunctionArgs regionFunctionArgs) {
    if (regionFunctionArgs.getRegionPath() == null) {
      throw new IllegalArgumentException(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH);
    }

    DistributedSystemMXBean dsMBean = getDSMBean(cache);

    String useAttributesFrom = regionFunctionArgs.getUseAttributesFrom();
    if (useAttributesFrom != null && !useAttributesFrom.isEmpty()
        && regionExists(cache, useAttributesFrom)) {
      if (!regionExists(cache, useAttributesFrom)) { // check already done in createRegion !!!
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND,
            CliStrings.CREATE_REGION__USEATTRIBUTESFROM, useAttributesFrom));
      }
      if (!regionFunctionArgs.isSetUseAttributesFrom()
          || regionFunctionArgs.getRegionAttributes() == null) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__COULD_NOT_RETRIEVE_REGION_ATTRS_FOR_PATH_0_VERIFY_REGION_EXISTS,
            useAttributesFrom));
      }
    }

    if (regionFunctionArgs.hasPartitionAttributes()) {
      RegionFunctionArgs.PartitionArgs partitionArgs = regionFunctionArgs.getPartitionArgs();
      String colocatedWith = partitionArgs.getPrColocatedWith();
      if (colocatedWith != null && !colocatedWith.isEmpty()) {
        String[] listAllRegionPaths = dsMBean.listAllRegionPaths();
        String foundRegionPath = null;
        for (String regionPath : listAllRegionPaths) {
          if (regionPath.equals(colocatedWith)) {
            foundRegionPath = regionPath;
            break;
          }
        }
        if (foundRegionPath == null) {
          throw new IllegalArgumentException(CliStrings.format(
              CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND,
              CliStrings.CREATE_REGION__COLOCATEDWITH, colocatedWith));
        }
        ManagementService mgmtService = ManagementService.getExistingManagementService(cache);
        DistributedRegionMXBean distributedRegionMXBean =
            mgmtService.getDistributedRegionMXBean(foundRegionPath);
        String regionType = distributedRegionMXBean.getRegionType();
        if (!(DataPolicy.PARTITION.toString().equals(regionType)
            || DataPolicy.PERSISTENT_PARTITION.toString().equals(regionType))) {
          throw new IllegalArgumentException(CliStrings.format(
              CliStrings.CREATE_REGION__MSG__COLOCATEDWITH_REGION_0_IS_NOT_PARTITIONEDREGION,
              new Object[] {colocatedWith}));
        }
      }
      if (partitionArgs.isSetPRLocalMaxMemory()) {
        int prLocalMaxMemory = partitionArgs.getPrLocalMaxMemory();
        if (prLocalMaxMemory < 0) {
          throw new IllegalArgumentException(
              LocalizedStrings.AttributesFactory_PARTITIONATTRIBUTES_LOCALMAXMEMORY_MUST_NOT_BE_NEGATIVE
                  .toLocalizedString());
        }
      }
      if (partitionArgs.isSetPRTotalMaxMemory()) {
        long prTotalMaxMemory = partitionArgs.getPrTotalMaxMemory();
        if (prTotalMaxMemory <= 0) {
          throw new IllegalArgumentException(
              LocalizedStrings.AttributesFactory_TOTAL_SIZE_OF_PARTITION_REGION_MUST_BE_0
                  .toLocalizedString());
        }
      }
      if (partitionArgs.isSetPRRedundantCopies()) {
        int prRedundantCopies = partitionArgs.getPrRedundantCopies();
        switch (prRedundantCopies) {
          case 0:
          case 1:
          case 2:
          case 3:
            break;
          default:
            throw new IllegalArgumentException(CliStrings.format(
                CliStrings.CREATE_REGION__MSG__REDUNDANT_COPIES_SHOULD_BE_ONE_OF_0123,
                new Object[] {prRedundantCopies}));
        }
      }
    }

    String keyConstraint = regionFunctionArgs.getKeyConstraint();
    if (keyConstraint != null && !RegionCommandsUtils.isClassNameValid(keyConstraint)) {
      throw new IllegalArgumentException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_KEYCONSTRAINT_0_IS_INVALID,
          new Object[] {keyConstraint}));
    }

    String valueConstraint = regionFunctionArgs.getValueConstraint();
    if (valueConstraint != null && !RegionCommandsUtils.isClassNameValid(valueConstraint)) {
      throw new IllegalArgumentException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_VALUECONSTRAINT_0_IS_INVALID,
          new Object[] {valueConstraint}));
    }

    Set<String> cacheListeners = regionFunctionArgs.getCacheListeners();
    if (cacheListeners != null && !cacheListeners.isEmpty()) {
      for (String cacheListener : cacheListeners) {
        if (!RegionCommandsUtils.isClassNameValid(cacheListener)) {
          throw new IllegalArgumentException(CliStrings.format(
              CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELISTENER_0_IS_INVALID,
              new Object[] {cacheListener}));
        }
      }
    }

    String cacheLoader = regionFunctionArgs.getCacheLoader();
    if (cacheLoader != null && !RegionCommandsUtils.isClassNameValid(cacheLoader)) {
      throw new IllegalArgumentException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELOADER_0_IS_INVALID,
          new Object[] {cacheLoader}));
    }

    String cacheWriter = regionFunctionArgs.getCacheWriter();
    if (cacheWriter != null && !RegionCommandsUtils.isClassNameValid(cacheWriter)) {
      throw new IllegalArgumentException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHEWRITER_0_IS_INVALID,
          new Object[] {cacheWriter}));
    }

    Set<String> gatewaySenderIds = regionFunctionArgs.getGatewaySenderIds();
    if (gatewaySenderIds != null && !gatewaySenderIds.isEmpty()) {
      String[] gatewaySenders = dsMBean.listGatewaySenders();
      if (gatewaySenders.length == 0) {
        throw new IllegalArgumentException(
            CliStrings.CREATE_REGION__MSG__NO_GATEWAYSENDERS_IN_THE_SYSTEM);
      } else {
        List<String> gatewaySendersList = new ArrayList<>(Arrays.asList(gatewaySenders));
        gatewaySenderIds = new HashSet<>(gatewaySenderIds);
        gatewaySenderIds.removeAll(gatewaySendersList);
        if (!gatewaySenderIds.isEmpty()) {
          throw new IllegalArgumentException(CliStrings.format(
              CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_GATEWAYSENDER_ID_UNKNOWN_0,
              new Object[] {gatewaySenderIds}));
        }
      }
    }

    if (regionFunctionArgs.isSetConcurrencyLevel()) {
      int concurrencyLevel = regionFunctionArgs.getConcurrencyLevel();
      if (concurrencyLevel < 0) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__SPECIFY_POSITIVE_INT_FOR_CONCURRENCYLEVEL_0_IS_NOT_VALID,
            new Object[] {concurrencyLevel}));
      }
    }

    String diskStore = regionFunctionArgs.getDiskStore();
    if (diskStore != null) {
      RegionShortcut regionShortcut = regionFunctionArgs.getRegionShortcut();
      if (regionShortcut != null
          && !RegionCommandsUtils.PERSISTENT_OVERFLOW_SHORTCUTS.contains(regionShortcut)) {
        String subMessage =
            LocalizedStrings.DiskStore_IS_USED_IN_NONPERSISTENT_REGION.toLocalizedString();
        String message = subMessage + ". "
            + CliStrings.format(CliStrings.CREATE_REGION__MSG__USE_ONE_OF_THESE_SHORTCUTS_0,
                new Object[] {String.valueOf(RegionCommandsUtils.PERSISTENT_OVERFLOW_SHORTCUTS)});

        throw new IllegalArgumentException(message);
      }

      RegionAttributes<?, ?> regionAttributes = regionFunctionArgs.getRegionAttributes();
      if (regionAttributes != null && !regionAttributes.getDataPolicy().withPersistence()) {
        String subMessage =
            LocalizedStrings.DiskStore_IS_USED_IN_NONPERSISTENT_REGION.toLocalizedString();
        String message = subMessage + ". "
            + CliStrings.format(
                CliStrings.CREATE_REGION__MSG__USE_ATTRIBUTES_FROM_REGION_0_IS_NOT_WITH_PERSISTENCE,
                new Object[] {String.valueOf(regionFunctionArgs.getUseAttributesFrom())});

        throw new IllegalArgumentException(message);
      }

      if (!diskStoreExists(cache, diskStore)) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_DISKSTORE_UNKNOWN_DISKSTORE_0,
            new Object[] {diskStore}));
      }
    }

    RegionFunctionArgs.ExpirationAttrs entryExpirationIdleTime =
        regionFunctionArgs.getEntryExpirationIdleTime();
    RegionFunctionArgs.ExpirationAttrs entryExpirationTTL =
        regionFunctionArgs.getEntryExpirationTTL();
    RegionFunctionArgs.ExpirationAttrs regionExpirationIdleTime =
        regionFunctionArgs.getRegionExpirationIdleTime();
    RegionFunctionArgs.ExpirationAttrs regionExpirationTTL =
        regionFunctionArgs.getRegionExpirationTTL();

    if ((!regionFunctionArgs.isSetStatisticsEnabled() || !regionFunctionArgs.isStatisticsEnabled())
        && (entryExpirationIdleTime != null || entryExpirationTTL != null
            || regionExpirationIdleTime != null || regionExpirationTTL != null)) {
      String message = LocalizedStrings.AttributesFactory_STATISTICS_MUST_BE_ENABLED_FOR_EXPIRATION
          .toLocalizedString();
      throw new IllegalArgumentException(message + ".");
    }

    boolean compressorFailure = false;
    if (regionFunctionArgs.isSetCompressor()) {
      String compressorClassName = regionFunctionArgs.getCompressor();
      Object compressor = null;
      try {
        Class<?> compressorClass = ClassPathLoader.getLatest().forName(compressorClassName);
        compressor = compressorClass.newInstance();
      } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
        compressorFailure = true;
      }

      if (compressorFailure || !(compressor instanceof Compressor)) {
        throw new IllegalArgumentException(
            CliStrings.format(CliStrings.CREATE_REGION__MSG__INVALID_COMPRESSOR,
                new Object[] {regionFunctionArgs.getCompressor()}));
      }
    }
  }

  private static <K, V> FetchRegionAttributesFunction.FetchRegionAttributesFunctionResult<K, V> getRegionAttributes(
      InternalCache cache, String regionPath) {
    if (!isClusterWideSameConfig(cache, regionPath)) {
      throw new IllegalStateException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__USE_ATTRIBUTES_FORM_REGIONS_EXISTS_BUT_DIFFERENT_SCOPE_OR_DATAPOLICY_USE_DESCRIBE_REGION_FOR_0,
          regionPath));
    }
    FetchRegionAttributesFunction.FetchRegionAttributesFunctionResult<K, V> attributes = null;

    // First check whether the region exists on a this manager, if yes then no
    // need to use FetchRegionAttributesFunction to fetch RegionAttributes
    try {
      attributes = FetchRegionAttributesFunction.getRegionAttributes(regionPath);
    } catch (IllegalArgumentException e) {
      /* region doesn't exist on the manager */
    }

    if (attributes == null) {
      // find first member which has the region
      Set<DistributedMember> regionAssociatedMembers =
          CliUtil.getRegionAssociatedMembers(regionPath, cache);
      if (regionAssociatedMembers != null && !regionAssociatedMembers.isEmpty()) {
        DistributedMember distributedMember = regionAssociatedMembers.iterator().next();
        ResultCollector<?, ?> resultCollector = CliUtil
            .executeFunction(FetchRegionAttributesFunction.INSTANCE, regionPath, distributedMember);
        List<?> resultsList = (List<?>) resultCollector.getResult();

        if (resultsList != null && !resultsList.isEmpty()) {
          for (Object object : resultsList) {
            if (object instanceof IllegalArgumentException) {
              throw (IllegalArgumentException) object;
            } else if (object instanceof Throwable) {
              Throwable th = (Throwable) object;
              LogWrapper.getInstance().info(CliUtil.stackTraceAsString((th)));
              throw new IllegalArgumentException(CliStrings.format(
                  CliStrings.CREATE_REGION__MSG__COULD_NOT_RETRIEVE_REGION_ATTRS_FOR_PATH_0_REASON_1,
                  regionPath, th.getMessage()));
            } else { // has to be RegionAttributes
              @SuppressWarnings("unchecked") // to avoid warning :(
              FetchRegionAttributesFunction.FetchRegionAttributesFunctionResult<K, V> regAttr =
                  ((FetchRegionAttributesFunction.FetchRegionAttributesFunctionResult<K, V>) object);
              if (attributes == null) {
                attributes = regAttr;
                break;
              } // attributes null check
            } // not IllegalArgumentException or other throwable
          } // iterate over list - there should be only one result in the list
        } // result list is not null or empty
      } // regionAssociatedMembers is not-empty
    } // attributes are null because do not exist on local member

    return attributes;
  }

  private static boolean isClusterWideSameConfig(InternalCache cache, String regionPath) {
    ManagementService managementService = ManagementService.getExistingManagementService(cache);

    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();

    Set<DistributedMember> allMembers = CliUtil.getAllNormalMembers(cache);

    RegionAttributesData regionAttributesToValidateAgainst = null;
    for (DistributedMember distributedMember : allMembers) {
      ObjectName regionObjectName;
      try {
        regionObjectName = dsMXBean
            .fetchRegionObjectName(CliUtil.getMemberNameOrId(distributedMember), regionPath);
        RegionMXBean regionMBean =
            managementService.getMBeanInstance(regionObjectName, RegionMXBean.class);
        RegionAttributesData regionAttributes = regionMBean.listRegionAttributes();

        if (regionAttributesToValidateAgainst == null) {
          regionAttributesToValidateAgainst = regionAttributes;
        } else if (!(regionAttributesToValidateAgainst.getScope()
            .equals(regionAttributes.getScope())
            || regionAttributesToValidateAgainst.getDataPolicy()
                .equals(regionAttributes.getDataPolicy()))) {
          return false;
        }
      } catch (Exception e) {
        // ignore
      }
    }

    return true;
  }

  static boolean regionExists(InternalCache cache, String regionPath) {
    if (regionPath == null || Region.SEPARATOR.equals(regionPath)) {
      return false;
    }

    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean dsMBean = managementService.getDistributedSystemMXBean();

    String[] allRegionPaths = dsMBean.listAllRegionPaths();
    return Arrays.stream(allRegionPaths).anyMatch(regionPath::equals);
  }

  private boolean diskStoreExists(InternalCache cache, String diskStoreName) {
    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();
    Map<String, String[]> diskstore = dsMXBean.listMemberDiskstore();

    Set<Map.Entry<String, String[]>> entrySet = diskstore.entrySet();

    for (Map.Entry<String, String[]> entry : entrySet) {
      String[] value = entry.getValue();
      if (CliUtil.contains(value, diskStoreName)) {
        return true;
      }
    }

    return false;
  }

  private void validateRegionPathAndParent(InternalCache cache, String regionPath) {
    if (StringUtils.isEmpty(regionPath)) {
      throw new IllegalArgumentException(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH);
    }
    // If a region path indicates a sub-region, check whether the parent region exists
    RegionPath regionPathData = new RegionPath(regionPath);
    String parentRegionPath = regionPathData.getParent();
    if (parentRegionPath != null && !Region.SEPARATOR.equals(parentRegionPath)) {
      if (!regionExists(cache, parentRegionPath)) {
        throw new IllegalArgumentException(
            CliStrings.format(CliStrings.CREATE_REGION__MSG__PARENT_REGION_FOR_0_DOES_NOT_EXIST,
                new Object[] {regionPath}));
      }
    }
  }

  private boolean isPersistentShortcut(RegionShortcut shortcut) {
    return shortcut == RegionShortcut.LOCAL_PERSISTENT
        || shortcut == RegionShortcut.LOCAL_PERSISTENT_OVERFLOW
        || shortcut == RegionShortcut.PARTITION_PERSISTENT
        || shortcut == RegionShortcut.PARTITION_PERSISTENT_OVERFLOW
        || shortcut == RegionShortcut.PARTITION_REDUNDANT_PERSISTENT
        || shortcut == RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW
        || shortcut == RegionShortcut.REPLICATE_PERSISTENT
        || shortcut == RegionShortcut.REPLICATE_PERSISTENT_OVERFLOW;
  }

  private boolean isAttributePersistent(RegionAttributes attributes) {
    return attributes != null && attributes.getDataPolicy() != null
        && attributes.getDataPolicy().toString().contains("PERSISTENT");
  }

  DistributedSystemMXBean getDSMBean(InternalCache cache) {
    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    return managementService.getDistributedSystemMXBean();
  }
}
