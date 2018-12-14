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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.ObjectName;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.RegionAttributesData;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.domain.ClassName;
import org.apache.geode.management.internal.cli.domain.PartitionArgs;
import org.apache.geode.management.internal.cli.domain.RegionConfigFactory;
import org.apache.geode.management.internal.cli.exceptions.EntityExistsException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateRegionFunctionArgs;
import org.apache.geode.management.internal.cli.functions.FetchRegionAttributesFunction;
import org.apache.geode.management.internal.cli.functions.RegionAttributesWrapper;
import org.apache.geode.management.internal.cli.functions.RegionCreateFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.RegionPath;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateRegionCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.CREATE_REGION, help = CliStrings.CREATE_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION,
      interceptor = "org.apache.geode.management.internal.cli.commands.CreateRegionCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel createRegion(
      @CliOption(key = CliStrings.CREATE_REGION__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.CREATE_REGION__REGION__HELP) String regionPath,
      @CliOption(key = CliStrings.CREATE_REGION__REGIONSHORTCUT,
          help = CliStrings.CREATE_REGION__REGIONSHORTCUT__HELP) RegionShortcut regionShortcut,
      @CliOption(key = CliStrings.CREATE_REGION__USEATTRIBUTESFROM,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.CREATE_REGION__USEATTRIBUTESFROM__HELP) String templateRegion,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_REGION__GROUP__HELP) String[] groups,
      @CliOption(key = {CliStrings.IFNOTEXISTS, CliStrings.CREATE_REGION__SKIPIFEXISTS},
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
          help = CliStrings.CREATE_REGION__IFNOTEXISTS__HELP) boolean ifNotExists,

      // the following should all be in alphabetical order according to
      // their key string
      @CliOption(key = CliStrings.CREATE_REGION__ASYNCEVENTQUEUEID,
          help = CliStrings.CREATE_REGION__ASYNCEVENTQUEUEID__HELP) String[] asyncEventQueueIds,
      @CliOption(key = CliStrings.CREATE_REGION__CACHELISTENER,
          // split the input only with "," outside of json string
          optionContext = "splittingRegex=,(?![^{]*\\})",
          help = CliStrings.CREATE_REGION__CACHELISTENER__HELP) ClassName<CacheListener>[] cacheListener,
      @CliOption(key = CliStrings.CREATE_REGION__CACHELOADER,
          help = CliStrings.CREATE_REGION__CACHELOADER__HELP) ClassName<CacheLoader> cacheLoader,
      @CliOption(key = CliStrings.CREATE_REGION__CACHEWRITER,
          help = CliStrings.CREATE_REGION__CACHEWRITER__HELP) ClassName<CacheWriter> cacheWriter,
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
          specifiedDefaultValue = "true",
          help = CliStrings.CREATE_REGION__ENABLEASYNCCONFLATION__HELP) Boolean enableAsyncConflation,
      @CliOption(key = CliStrings.CREATE_REGION__CLONINGENABLED, specifiedDefaultValue = "true",
          help = CliStrings.CREATE_REGION__CLONINGENABLED__HELP) Boolean cloningEnabled,
      @CliOption(key = CliStrings.CREATE_REGION__CONCURRENCYCHECKSENABLED,
          specifiedDefaultValue = "true",
          help = CliStrings.CREATE_REGION__CONCURRENCYCHECKSENABLED__HELP) Boolean concurrencyChecksEnabled,
      @CliOption(key = CliStrings.CREATE_REGION__MULTICASTENABLED, specifiedDefaultValue = "true",
          help = CliStrings.CREATE_REGION__MULTICASTENABLED__HELP) Boolean mcastEnabled,
      @CliOption(key = CliStrings.CREATE_REGION__STATISTICSENABLED, specifiedDefaultValue = "true",
          help = CliStrings.CREATE_REGION__STATISTICSENABLED__HELP) Boolean statisticsEnabled,
      @CliOption(key = CliStrings.CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION,
          specifiedDefaultValue = "true",
          help = CliStrings.CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION__HELP) Boolean enableSubscriptionConflation,
      @CliOption(key = CliStrings.CREATE_REGION__DISKSYNCHRONOUS, specifiedDefaultValue = "true",
          help = CliStrings.CREATE_REGION__DISKSYNCHRONOUS__HELP) Boolean diskSynchronous,
      @CliOption(key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME,
          help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME__HELP) Integer entryExpirationIdleTime,
      @CliOption(key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION,
          help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION__HELP) ExpirationAction entryExpirationIdleTimeAction,
      @CliOption(key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE,
          help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE__HELP) Integer entryExpirationTTL,
      @CliOption(key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION,
          help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION__HELP) ExpirationAction entryExpirationTTLAction,
      @CliOption(key = CliStrings.ENTRY_IDLE_TIME_CUSTOM_EXPIRY,
          help = CliStrings.ENTRY_IDLE_TIME_CUSTOM_EXPIRY_HELP) ClassName<CustomExpiry> entryIdleTimeCustomExpiry,
      @CliOption(key = CliStrings.ENTRY_TTL_CUSTOM_EXPIRY,
          help = CliStrings.ENTRY_TTL_CUSTOM_EXPIRY_HELP) ClassName<CustomExpiry> entryTTLCustomExpiry,
      @CliOption(key = CliStrings.CREATE_REGION__EVICTION_ACTION,
          help = CliStrings.CREATE_REGION__EVICTION_ACTION__HELP) String evictionAction,
      @CliOption(key = CliStrings.CREATE_REGION__EVICTION_ENTRY_COUNT,
          help = CliStrings.CREATE_REGION__EVICTION_ENTRY_COUNT__HELP) Integer evictionEntryCount,
      @CliOption(key = CliStrings.CREATE_REGION__EVICTION_MAX_MEMORY,
          help = CliStrings.CREATE_REGION__EVICTION_MAX_MEMORY__HELP) Integer evictionMaxMemory,
      @CliOption(key = CliStrings.CREATE_REGION__EVICTION_OBJECT_SIZER,
          help = CliStrings.CREATE_REGION__EVICTION_OBJECT_SIZER__HELP) String evictionObjectSizer,
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
          help = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION__HELP) ExpirationAction regionExpirationIdleTimeAction,
      @CliOption(key = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL,
          help = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL__HELP) Integer regionExpirationTTL,
      @CliOption(key = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION,
          help = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION__HELP) ExpirationAction regionExpirationTTLAction,
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
    if (regionShortcut != null && templateRegion != null) {
      return ResultModel.createError(
          CliStrings.CREATE_REGION__MSG__ONLY_ONE_OF_REGIONSHORTCUT_AND_USEATTRIBUESFROM_CAN_BE_SPECIFIED);
    }

    if (regionShortcut == null && templateRegion == null) {
      return ResultModel.createError(
          CliStrings.CREATE_REGION__MSG__ONE_OF_REGIONSHORTCUT_AND_USEATTRIBUTESFROM_IS_REQUIRED);
    }

    try {
      failIfRegionAlreadyExists(regionPath, regionShortcut, groups);
    } catch (EntityExistsException e) {
      return ifNotExists ? ResultModel.createInfo("Skipping: " + e.getMessage())
          : ResultModel.createError(e.getMessage());
    }

    InternalCache cache = (InternalCache) getCache();

    // validate the parent region
    RegionPath regionPathData = new RegionPath(regionPath);
    if (!regionPathData.isRoot() && !regionExists(regionPathData.getParent())) {
      return ResultModel.createError(
          CliStrings.format(CliStrings.CREATE_REGION__MSG__PARENT_REGION_FOR_0_DOES_NOT_EXIST,
              new Object[] {regionPath}));
    }

    // validate if partition args are supplied only for partitioned regions
    PartitionArgs partitionArgs =
        new PartitionArgs(prColocatedWith, prLocalMaxMemory, prRecoveryDelay,
            prRedundantCopies, prStartupRecoveryDelay, prTotalMaxMemory, prTotalNumBuckets,
            partitionResolver);
    if (regionShortcut != null && !regionShortcut.name().startsWith("PARTITION")
        && !partitionArgs.isEmpty()) {
      return ResultModel.createError(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__OPTION_0_CAN_BE_USED_ONLY_FOR_PARTITIONEDREGION,
          partitionArgs.getUserSpecifiedPartitionAttributes()) + " "
          + CliStrings.format(CliStrings.CREATE_REGION__MSG__0_IS_NOT_A_PARITIONEDREGION,
              regionPath));
    }

    // validate colocation for partitioned regions
    if (prColocatedWith != null) {
      DistributedRegionMXBean colocatedRegionBean =
          getManagementService().getDistributedRegionMXBean(prColocatedWith);

      if (colocatedRegionBean == null) {
        return ResultModel.createError(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND,
            CliStrings.CREATE_REGION__COLOCATEDWITH, prColocatedWith));
      }

      if (!colocatedRegionBean.getRegionType().equals("PARTITION") &&
          !colocatedRegionBean.getRegionType().equals("PERSISTENT_PARTITION")) {
        return ResultModel.createError(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__COLOCATEDWITH_REGION_0_IS_NOT_PARTITIONEDREGION,
            new String[] {prColocatedWith}));
      }
    }

    // validate gateway senders
    if (gatewaySenderIds != null) {
      Set<String> existingGatewaySenders =
          Arrays.stream(getDSMBean().listGatewaySenders()).collect(Collectors.toSet());
      if (existingGatewaySenders.isEmpty()) {
        return ResultModel
            .createError(CliStrings.CREATE_REGION__MSG__NO_GATEWAYSENDERS_IN_THE_SYSTEM);
      }

      if (Arrays.stream(gatewaySenderIds).anyMatch(id -> !existingGatewaySenders.contains(id))) {
        return ResultModel.createError(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_GATEWAYSENDER_ID_UNKNOWN_0,
            (Object[]) gatewaySenderIds));
      }
    }

    // validate if template region exists, if provided
    if (templateRegion != null && !regionExists(templateRegion)) {
      return ResultModel.createError(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND,
          CliStrings.CREATE_REGION__USEATTRIBUTESFROM, templateRegion));
    }

    // get predefined attributes for a template region
    RegionAttributesWrapper<?, ?> wrappedTemplateAttributes = null;
    if (templateRegion != null) {
      wrappedTemplateAttributes = getRegionAttributes(cache, templateRegion);
      if (wrappedTemplateAttributes == null) {
        return ResultModel.createError(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__COULD_NOT_RETRIEVE_REGION_ATTRS_FOR_PATH_0_VERIFY_REGION_EXISTS,
            templateRegion));
      }

      if (wrappedTemplateAttributes.getRegionAttributes().getPartitionAttributes() == null
          && !partitionArgs.isEmpty()) {
        return ResultModel.createError(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__OPTION_0_CAN_BE_USED_ONLY_FOR_PARTITIONEDREGION,
            partitionArgs.getUserSpecifiedPartitionAttributes()) + " "
            + CliStrings.format(CliStrings.CREATE_REGION__MSG__0_IS_NOT_A_PARITIONEDREGION,
                templateRegion));
      }
    }

    RegionAttributes<?, ?> regionAttributes;
    if (wrappedTemplateAttributes != null) {
      regionAttributes = wrappedTemplateAttributes.getRegionAttributes();
    } else {
      regionAttributes = cache.getRegionAttributes(regionShortcut.toString());
    }

    // validating diskstore with other attributes
    if (diskStore != null && !regionAttributes.getDataPolicy().withPersistence()) {
      String subMessage = "Only regions with persistence or overflow to disk can specify DiskStore";
      String message = subMessage + ". "
          + CliStrings.format(
              CliStrings.CREATE_REGION__MSG__USE_ATTRIBUTES_FROM_REGION_0_IS_NOT_WITH_PERSISTENCE,
              new Object[] {templateRegion});

      return ResultModel.createError(message);
    }

    if (diskStore != null && !diskStoreExists(diskStore)) {
      return ResultModel.createError(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_DISKSTORE_UNKNOWN_DISKSTORE_0,
          new Object[] {diskStore}));
    }

    // additional authorization
    if (isAttributePersistent(regionAttributes)) {
      authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.WRITE,
          ResourcePermission.Target.DISK);
    }

    // validating the groups
    Set<DistributedMember> membersToCreateRegionOn = findMembers(groups, null);
    if (membersToCreateRegionOn.isEmpty()) {
      if (groups == null || groups.length == 0) {
        return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      return ResultModel.createError(
          CliStrings.format(CliStrings.CREATE_REGION__MSG__GROUPS_0_ARE_INVALID,
              (Object[]) groups));
    }

    // generate the RegionConfig object for passing to distributed function and persisting
    Set<ClassName<CacheListener>> cacheListeners = new HashSet<>();
    if (cacheListener != null) {
      Arrays.stream(cacheListener).forEach(c -> cacheListeners.add(c));
    } else if (wrappedTemplateAttributes != null
        && wrappedTemplateAttributes.getCacheListenerClasses() != null) {
      cacheListeners.addAll(wrappedTemplateAttributes.getCacheListenerClasses());
    }

    ClassName<CacheLoader> cacheLoaderClassNameToPersist = null;
    if (cacheLoader != null) {
      cacheLoaderClassNameToPersist = cacheLoader;
    } else if (wrappedTemplateAttributes != null
        && wrappedTemplateAttributes.getCacheLoaderClass() != null) {
      cacheLoaderClassNameToPersist = wrappedTemplateAttributes.getCacheLoaderClass();
    }

    ClassName<CacheWriter> cacheWriterClassNameToPersist = null;
    if (cacheWriter != null) {
      cacheWriterClassNameToPersist = cacheWriter;
    } else if (wrappedTemplateAttributes != null
        && wrappedTemplateAttributes.getCacheWriterClass() != null) {
      cacheWriterClassNameToPersist = wrappedTemplateAttributes.getCacheWriterClass();
    }

    String compressorClassNameToPersist = null;
    if (compressor != null) {
      compressorClassNameToPersist = compressor;
    } else if (wrappedTemplateAttributes != null
        && wrappedTemplateAttributes.getCompressorClass() != null) {
      compressorClassNameToPersist = wrappedTemplateAttributes.getCompressorClass();
    }

    String keyConstraintToPersist = null;
    if (keyConstraint != null) {
      keyConstraintToPersist = keyConstraint;
    } else if (wrappedTemplateAttributes != null
        && wrappedTemplateAttributes.getKeyConstraintClass() != null) {
      keyConstraintToPersist = wrappedTemplateAttributes.getKeyConstraintClass();
    }

    String valueConstraintToPersist = null;
    if (valueConstraint != null) {
      valueConstraintToPersist = valueConstraint;
    } else if (wrappedTemplateAttributes != null
        && wrappedTemplateAttributes.getValueConstraintClass() != null) {
      valueConstraintToPersist = wrappedTemplateAttributes.getValueConstraintClass();
    }

    Set<String> asyncEventQueueIdSet = Optional.ofNullable(asyncEventQueueIds)
        .map(a -> Arrays.stream(a).collect(Collectors.toSet()))
        .orElse(null);
    Set<String> gatewaySenderIdSet = Optional.ofNullable(gatewaySenderIds)
        .map(a -> Arrays.stream(a).collect(Collectors.toSet()))
        .orElse(null);

    RegionConfig config = (new RegionConfigFactory()).generate(regionPath, keyConstraintToPersist,
        valueConstraintToPersist, statisticsEnabled, entryExpirationIdleTime,
        entryExpirationIdleTimeAction, entryExpirationTTL, entryExpirationTTLAction,
        entryIdleTimeCustomExpiry,
        entryTTLCustomExpiry, regionExpirationIdleTime, regionExpirationIdleTimeAction,
        regionExpirationTTL, regionExpirationTTLAction, evictionAction, evictionMaxMemory,
        evictionEntryCount, evictionObjectSizer, diskStore, diskSynchronous, enableAsyncConflation,
        enableSubscriptionConflation, cacheListeners, cacheLoaderClassNameToPersist,
        cacheWriterClassNameToPersist,
        asyncEventQueueIdSet, gatewaySenderIdSet, concurrencyChecksEnabled, cloningEnabled,
        mcastEnabled,
        concurrencyLevel, partitionArgs, compressorClassNameToPersist, offHeap, regionAttributes);

    // creating the RegionFunctionArgs
    CreateRegionFunctionArgs functionArgs =
        new CreateRegionFunctionArgs(regionPath, config, ifNotExists);

    List<CliFunctionResult> regionCreateResults = executeAndGetFunctionResult(
        RegionCreateFunction.INSTANCE, functionArgs, membersToCreateRegionOn);

    ResultModel resultModel = ResultModel.createMemberStatusResult(regionCreateResults);
    InternalConfigurationPersistenceService service = getConfigurationPersistenceService();
    if (service == null) {
      return resultModel;
    }

    if (resultModel.isSuccessful() && regionCreateResults.stream()
        .anyMatch(
            res -> res.getStatusMessage() != null && res.getStatusMessage().contains("Skipping"))) {
      return resultModel;
    }

    // persist the RegionConfig object if the function is successful on all members
    if (resultModel.isSuccessful()) {
      verifyDistributedRegionMbean(cache, regionPath);

      // the following is a temporary solution before lucene make the change to create region first
      // before creating the lucene index.
      // GEODE-3924
      // we will need to get the xml returned from the server to find out any custom xml nested
      // inside the region
      String regionXml = (String) regionCreateResults.stream()
          .filter(CliFunctionResult::isSuccessful)
          .findFirst().get().getResultObject();
      RegionConfig regionConfigFromServer =
          service.getJaxbService().unMarshall(regionXml, RegionConfig.class);
      List<CacheElement> extensions = regionConfigFromServer.getCustomRegionElements();
      config.getCustomRegionElements().addAll(extensions);

      resultModel.setConfigObject(new CreateRegionResult(config, regionPath));
    }

    return resultModel;
  }

  private class CreateRegionResult {
    RegionConfig getRegionConfig() {
      return regionConfig;
    }

    String getFullRegionPath() {
      return fullRegionPath;
    }

    private final RegionConfig regionConfig;
    private final String fullRegionPath;

    public CreateRegionResult(RegionConfig regionConfig, String fullRegionPath) {
      this.regionConfig = regionConfig;
      this.fullRegionPath = fullRegionPath;
    }
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    if (configObject == null) {
      return false;
    }

    CreateRegionResult regionResultConfigObject = (CreateRegionResult) configObject;
    RegionConfig regionConfig = regionResultConfigObject.getRegionConfig();
    String regionPath = regionResultConfigObject.getFullRegionPath();

    RegionPath regionPathData = new RegionPath(regionPath);
    if (regionPathData.getParent() == null) {
      config.getRegions().add(regionConfig);
      return true;
    }

    String[] regionsOnPath = regionPathData.getRegionsOnParentPath();
    RegionConfig rootConfig = config.getRegions().stream()
        .filter(r -> r.getName().equals(regionsOnPath[0]))
        .findFirst()
        .get();

    RegionConfig currentConfig = rootConfig;
    for (int i = 1; i < regionsOnPath.length; i++) {
      final String curRegionName = regionsOnPath[i];
      currentConfig = currentConfig.getRegions()
          .stream()
          .filter(r -> r.getName().equals(curRegionName))
          .findFirst()
          .get();
    }

    currentConfig.getRegions().add(regionConfig);

    return true;
  }

  boolean verifyDistributedRegionMbean(InternalCache cache, String regionName) {
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

  RegionAttributesWrapper getRegionAttributes(InternalCache cache, String regionPath) {
    if (!isClusterWideSameConfig(cache, regionPath)) {
      throw new IllegalStateException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__USE_ATTRIBUTES_FORM_REGIONS_EXISTS_BUT_DIFFERENT_SCOPE_OR_DATAPOLICY_USE_DESCRIBE_REGION_FOR_0,
          regionPath));
    }
    RegionAttributesWrapper attributes = null;

    // First check whether the region exists on a this manager, if yes then no
    // need to use FetchRegionAttributesFunction to fetch RegionAttributes
    try {
      attributes = FetchRegionAttributesFunction.getRegionAttributes(cache, regionPath);
    } catch (IllegalArgumentException e) {
      /* region doesn't exist on the manager */
    }

    if (attributes == null) {
      // find first member which has the region
      Set<DistributedMember> regionAssociatedMembers = findMembersForRegion(regionPath);
      if (regionAssociatedMembers != null && !regionAssociatedMembers.isEmpty()) {
        DistributedMember distributedMember = regionAssociatedMembers.iterator().next();
        ResultCollector<?, ?> resultCollector =
            executeFunction(FetchRegionAttributesFunction.INSTANCE, regionPath, distributedMember);
        List<?> resultsList = (List<?>) resultCollector.getResult();

        if (resultsList != null && !resultsList.isEmpty()) {
          for (Object object : resultsList) {
            if (object instanceof IllegalArgumentException) {
              throw (IllegalArgumentException) object;
            } else if (object instanceof Throwable) {
              Throwable th = (Throwable) object;
              LogWrapper.getInstance(getCache()).info(ExceptionUtils.getStackTrace((th)));
              throw new IllegalArgumentException(CliStrings.format(
                  CliStrings.CREATE_REGION__MSG__COULD_NOT_RETRIEVE_REGION_ATTRS_FOR_PATH_0_REASON_1,
                  regionPath, th.getMessage()));
            } else { // has to be RegionAttributes
              @SuppressWarnings("unchecked") // to avoid warning :(
              RegionAttributesWrapper regAttr = ((RegionAttributesWrapper) object);
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

  private void failIfRegionAlreadyExists(String regionPath, RegionShortcut regionShortcut,
      String[] groups) throws EntityExistsException {
    /*
     * Adding name collision check for regions created with regionShortcut only.
     * Regions can be categories as Proxy(replicate/partition), replicate/partition, and local
     * For concise purpose: we call existing region (E) and region to be created (C)
     */
    DistributedRegionMXBean regionBean =
        getManagementService().getDistributedRegionMXBean(regionPath);
    if (regionBean == null || regionShortcut == null) {
      return;
    }

    String existingDataPolicy = regionBean.getRegionType();
    // either C is local, or E is local or E and C are both non-proxy regions. this is to make
    // sure local, replicate or partition regions have unique names across the entire cluster
    if (regionShortcut.isLocal() || existingDataPolicy.equals("NORMAL") || !regionShortcut.isProxy()
        && (regionBean.getMemberCount() > regionBean.getEmptyNodes())) {
      throw new EntityExistsException(
          String.format("Region %s already exists on the cluster.", regionPath));
    }

    // after this, one of E and C is proxy region or both are proxy regions.

    // we first make sure E and C have the compatible data policy
    if (regionShortcut.isPartition() && !existingDataPolicy.contains("PARTITION")) {
      LogService.getLogger().info("Create region command: got EntityExists exception");
      throw new EntityExistsException("The existing region is not a partitioned region");
    }

    if (regionShortcut.isReplicate() && !existingDataPolicy.equals("EMPTY")
        && !existingDataPolicy.contains("REPLICATE") && !existingDataPolicy.contains("PRELOADED")) {
      throw new EntityExistsException("The existing region is not a replicate region");
    }

    // then we make sure E and C are on different members
    Set<String> membersWithThisRegion =
        Arrays.stream(regionBean.getMembers()).collect(Collectors.toSet());
    Set<String> membersWithinGroup = findMembers(groups, null).stream()
        .map(DistributedMember::getName).collect(Collectors.toSet());
    if (!Collections.disjoint(membersWithinGroup, membersWithThisRegion)) {
      throw new EntityExistsException(
          String.format("Region %s already exists on these members: %s.", regionPath,
              StringUtils.join(membersWithThisRegion, ",")));
    }
  }

  private boolean isClusterWideSameConfig(InternalCache cache, String regionPath) {
    ManagementService managementService = getManagementService();

    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();

    Set<DistributedMember> allMembers = getAllNormalMembers();

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

  boolean regionExists(String regionPath) {
    if (regionPath == null || Region.SEPARATOR.equals(regionPath)) {
      return false;
    }

    ManagementService managementService = getManagementService();
    DistributedSystemMXBean dsMBean = managementService.getDistributedSystemMXBean();

    String[] allRegionPaths = dsMBean.listAllRegionPaths();
    return Arrays.stream(allRegionPaths).anyMatch(regionPath::equals);
  }

  private boolean diskStoreExists(String diskStoreName) {
    ManagementService managementService = getManagementService();
    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();
    Map<String, String[]> diskstore = dsMXBean.listMemberDiskstore();

    Set<Map.Entry<String, String[]>> entrySet = diskstore.entrySet();

    for (Map.Entry<String, String[]> entry : entrySet) {
      String[] value = entry.getValue();
      if (diskStoreName != null && ArrayUtils.contains(value, diskStoreName)) {
        return true;
      }
    }

    return false;
  }

  private boolean isAttributePersistent(RegionAttributes attributes) {
    return attributes != null && attributes.getDataPolicy() != null
        && attributes.getDataPolicy().toString().contains("PERSISTENT");
  }

  DistributedSystemMXBean getDSMBean() {
    ManagementService managementService = getManagementService();
    return managementService.getDistributedSystemMXBean();
  }


  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Integer localMaxMemory =
          (Integer) parseResult.getParamValue(CliStrings.CREATE_REGION__LOCALMAXMEMORY);
      if (localMaxMemory != null) {
        if (localMaxMemory < 0) {
          return ResultBuilder.createUserErrorResult(
              "PartitionAttributes localMaxMemory must not be negative.");
        }
      }
      Long totalMaxMemory =
          (Long) parseResult.getParamValue(CliStrings.CREATE_REGION__TOTALMAXMEMORY);
      if (totalMaxMemory != null) {
        if (totalMaxMemory <= 0) {
          return ResultBuilder.createUserErrorResult(
              "Total size of partition region must be > 0.");
        }
      }
      Integer redundantCopies =
          (Integer) parseResult.getParamValue(CliStrings.CREATE_REGION__REDUNDANTCOPIES);
      if (redundantCopies != null) {
        if (redundantCopies < 0 || redundantCopies > 3) {
          return ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.CREATE_REGION__MSG__REDUNDANT_COPIES_SHOULD_BE_ONE_OF_0123,
              new Object[] {redundantCopies}));
        }
      }

      Integer concurrencyLevel =
          (Integer) parseResult.getParamValue(CliStrings.CREATE_REGION__CONCURRENCYLEVEL);
      if (concurrencyLevel != null) {
        if (concurrencyLevel < 0) {
          return ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.CREATE_REGION__MSG__SPECIFY_POSITIVE_INT_FOR_CONCURRENCYLEVEL_0_IS_NOT_VALID,
              new Object[] {concurrencyLevel}));
        }
      }

      String keyConstraint =
          parseResult.getParamValueAsString(CliStrings.CREATE_REGION__KEYCONSTRAINT);
      if (keyConstraint != null && !ClassName.isClassNameValid(keyConstraint)) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_KEYCONSTRAINT_0_IS_INVALID,
            new Object[] {keyConstraint}));
      }

      String valueConstraint =
          parseResult.getParamValueAsString(CliStrings.CREATE_REGION__VALUECONSTRAINT);
      if (valueConstraint != null && !ClassName.isClassNameValid(valueConstraint)) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(
            CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_VALUECONSTRAINT_0_IS_INVALID,
            new Object[] {valueConstraint}));
      }

      String compressor = parseResult.getParamValueAsString(CliStrings.CREATE_REGION__COMPRESSOR);
      if (compressor != null && !ClassName.isClassNameValid(compressor)) {
        return ResultBuilder.createUserErrorResult(CliStrings
            .format(CliStrings.CREATE_REGION__MSG__INVALID_COMPRESSOR, new Object[] {compressor}));
      }

      Boolean cloningEnabled =
          (Boolean) parseResult.getParamValue(CliStrings.CREATE_REGION__CLONINGENABLED);
      if (compressor != null && cloningEnabled != null && !cloningEnabled) {
        return ResultBuilder.createUserErrorResult(CliStrings
            .format(CliStrings.CREATE_REGION__MSG__CANNOT_DISABLE_CLONING_WITH_COMPRESSOR,
                new Object[] {compressor}));
      }

      String diskStore = parseResult.getParamValueAsString(CliStrings.CREATE_REGION__DISKSTORE);
      if (diskStore != null) {
        String regionShortcut =
            parseResult.getParamValueAsString(CliStrings.CREATE_REGION__REGIONSHORTCUT);
        if (regionShortcut != null && !RegionCommandsUtils.PERSISTENT_OVERFLOW_SHORTCUTS
            .contains(RegionShortcut.valueOf(regionShortcut))) {
          String subMessage =
              "Only regions with persistence or overflow to disk can specify DiskStore";
          String message = subMessage + ". "
              + CliStrings.format(CliStrings.CREATE_REGION__MSG__USE_ONE_OF_THESE_SHORTCUTS_0,
                  new Object[] {String.valueOf(RegionCommandsUtils.PERSISTENT_OVERFLOW_SHORTCUTS)});

          return ResultBuilder.createUserErrorResult(message);
        }
      }

      // if any expiration value is set, statistics must be enabled
      Boolean statisticsEnabled =
          (Boolean) parseResult.getParamValue(CliStrings.CREATE_REGION__STATISTICSENABLED);
      Integer entryIdle =
          (Integer) parseResult.getParamValue(CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME);
      Integer entryTtl =
          (Integer) parseResult.getParamValue(CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE);
      Integer regionIdle =
          (Integer) parseResult.getParamValue(CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIME);
      Integer regionTtl =
          (Integer) parseResult.getParamValue(CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL);
      ExpirationAction entryIdleAction = (ExpirationAction) parseResult
          .getParamValue(CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION);
      ExpirationAction entryTtlAction = (ExpirationAction) parseResult
          .getParamValue(CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION);
      ExpirationAction regionIdleAction = (ExpirationAction) parseResult
          .getParamValue(CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION);
      ExpirationAction regionTtlAction = (ExpirationAction) parseResult
          .getParamValue(CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION);
      ClassName entryIdleExpiry =
          (ClassName) parseResult.getParamValue(CliStrings.ENTRY_IDLE_TIME_CUSTOM_EXPIRY);
      ClassName entryTTTLExpiry =
          (ClassName) parseResult.getParamValue(CliStrings.ENTRY_TTL_CUSTOM_EXPIRY);

      if ((entryIdle != null || entryTtl != null || regionIdle != null || regionTtl != null
          || entryIdleAction != null || entryTtlAction != null || regionIdleAction != null
          || regionTtlAction != null || entryIdleExpiry != null || entryTTTLExpiry != null)
          && (statisticsEnabled == null || !statisticsEnabled)) {
        String message =
            "Statistics must be enabled for expiration";
        return ResultBuilder.createUserErrorResult(message + ".");
      }


      String maxMemory =
          parseResult.getParamValueAsString(CliStrings.CREATE_REGION__EVICTION_MAX_MEMORY);
      String maxEntry =
          parseResult.getParamValueAsString(CliStrings.CREATE_REGION__EVICTION_ENTRY_COUNT);
      String evictionAction =
          parseResult.getParamValueAsString(CliStrings.CREATE_REGION__EVICTION_ACTION);
      String evictionSizer =
          parseResult.getParamValueAsString(CliStrings.CREATE_REGION__EVICTION_OBJECT_SIZER);
      if (maxEntry != null && maxMemory != null) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.CREATE_REGION__MSG__BOTH_EVICTION_VALUES);
      }

      if ((maxEntry != null || maxMemory != null) && evictionAction == null) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.CREATE_REGION__MSG__MISSING_EVICTION_ACTION);
      }

      if (evictionSizer != null && maxEntry != null) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.CREATE_REGION__MSG__INVALID_EVICTION_OBJECT_SIZER_AND_ENTRY_COUNT);
      }

      if (evictionAction != null
          && EvictionAction.parseAction(evictionAction) == EvictionAction.NONE) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.CREATE_REGION__MSG__INVALID_EVICTION_ACTION);
      }

      return ResultBuilder.createInfoResult("");
    }
  }
}
