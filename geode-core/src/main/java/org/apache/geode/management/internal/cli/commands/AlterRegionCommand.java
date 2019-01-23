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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionAttributesType.EvictionAttributes;
import org.apache.geode.cache.configuration.RegionAttributesType.ExpirationAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.domain.ClassName;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.RegionAlterFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class AlterRegionCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.ALTER_REGION, help = CliStrings.ALTER_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  public ResultModel alterRegion(
      @CliOption(key = CliStrings.ALTER_REGION__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.ALTER_REGION__REGION__HELP) String regionPath,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.ALTER_REGION__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME,
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME__HELP) Integer entryExpirationIdleTime,
      @CliOption(key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION,
          specifiedDefaultValue = "INVALIDATE",
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION__HELP) ExpirationAction entryExpirationIdleTimeAction,
      @CliOption(key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE,
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE__HELP) Integer entryExpirationTTL,
      @CliOption(key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION,
          specifiedDefaultValue = "INVALIDATE",
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION__HELP) ExpirationAction entryExpirationTTLAction,
      @CliOption(key = CliStrings.ENTRY_IDLE_TIME_CUSTOM_EXPIRY, specifiedDefaultValue = "",
          help = CliStrings.ENTRY_IDLE_TIME_CUSTOM_EXPIRY_HELP) ClassName<CustomExpiry> entryIdleTimeCustomExpiry,
      @CliOption(key = CliStrings.ENTRY_TTL_CUSTOM_EXPIRY, specifiedDefaultValue = "",
          help = CliStrings.ENTRY_TTL_CUSTOM_EXPIRY_HELP) ClassName<CustomExpiry> entryTTLCustomExpiry,
      @CliOption(key = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME,
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME__HELP) Integer regionExpirationIdleTime,
      @CliOption(key = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION,
          specifiedDefaultValue = "INVALIDATE",
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION__HELP) ExpirationAction regionExpirationIdleTimeAction,
      @CliOption(key = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL,
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL__HELP) Integer regionExpirationTTL,
      @CliOption(key = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION,
          specifiedDefaultValue = "INVALIDATE",
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION__HELP) ExpirationAction regionExpirationTTLAction,
      @CliOption(key = CliStrings.ALTER_REGION__CACHELISTENER, specifiedDefaultValue = "",
          // split the input only with comma outside of json string
          optionContext = "splittingRegex=,(?![^{]*\\})",
          help = CliStrings.ALTER_REGION__CACHELISTENER__HELP) ClassName<CacheListener>[] cacheListeners,
      @CliOption(key = CliStrings.ALTER_REGION__CACHELOADER, specifiedDefaultValue = "",
          help = CliStrings.ALTER_REGION__CACHELOADER__HELP) ClassName<CacheLoader> cacheLoader,
      @CliOption(key = CliStrings.ALTER_REGION__CACHEWRITER, specifiedDefaultValue = "",
          help = CliStrings.ALTER_REGION__CACHEWRITER__HELP) ClassName<CacheWriter> cacheWriter,
      @CliOption(key = CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, specifiedDefaultValue = "",
          help = CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID__HELP) String[] asyncEventQueueIds,
      @CliOption(key = CliStrings.ALTER_REGION__GATEWAYSENDERID, specifiedDefaultValue = "",
          help = CliStrings.ALTER_REGION__GATEWAYSENDERID__HELP) String[] gatewaySenderIds,
      @CliOption(key = CliStrings.ALTER_REGION__CLONINGENABLED, specifiedDefaultValue = "true",
          help = CliStrings.ALTER_REGION__CLONINGENABLED__HELP) Boolean cloningEnabled,
      @CliOption(key = CliStrings.ALTER_REGION__EVICTIONMAX, specifiedDefaultValue = "0",
          help = CliStrings.ALTER_REGION__EVICTIONMAX__HELP) Integer evictionMax) {
    authorize(Resource.DATA, Operation.MANAGE, regionPath);

    Set<DistributedMember> targetMembers = findMembers(groups, null);

    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    ConfigurationPersistenceService ccService = getConfigurationPersistenceService();

    if (groups == null) {
      groups = new String[] {"cluster"};
    }

    // Check that the specified region is in all the groups (normally we should pass in just one
    // group for alter region command
    if (ccService != null) {
      for (String group : groups) {
        CacheConfig clusterConfig = ccService.getCacheConfig(group);
        RegionConfig regionConfig = null;
        if (clusterConfig != null) {
          // we always know that regionPath starts with a "/", so we need to strip it out before we
          // pass it in to look for the regionConfig
          regionConfig =
              CacheElement.findElement(clusterConfig.getRegions(), regionPath.substring(1));
        }

        if (regionConfig == null) {
          throw new EntityNotFoundException(
              String.format("%s does not exist in group %s", regionPath, group));
        }
      }
    }

    RegionConfig deltaConfig = new RegionConfig();
    deltaConfig.setName(regionPath);
    RegionAttributesType regionAttributesType = new RegionAttributesType();
    deltaConfig.setRegionAttributes(regionAttributesType);
    regionAttributesType.setEntryIdleTime(getExpirationAttributes(entryExpirationIdleTime,
        entryExpirationIdleTimeAction, entryIdleTimeCustomExpiry));
    regionAttributesType.setEntryTimeToLive(getExpirationAttributes(entryExpirationTTL,
        entryExpirationTTLAction, entryTTLCustomExpiry));
    regionAttributesType.setRegionIdleTime(
        getExpirationAttributes(regionExpirationIdleTime, regionExpirationIdleTimeAction, null));
    regionAttributesType.setRegionTimeToLive(
        getExpirationAttributes(regionExpirationTTL, regionExpirationTTLAction, null));
    if (cacheLoader != null) {
      regionAttributesType.setCacheLoader(
          new DeclarableType(cacheLoader.getClassName(), cacheLoader.getInitProperties()));
    }

    if (cacheWriter != null) {
      regionAttributesType.setCacheWriter(
          new DeclarableType(cacheLoader.getClassName(), cacheLoader.getInitProperties()));
    }

    if (cacheListeners != null) {
      regionAttributesType.getCacheListeners().addAll(
          Arrays.stream(cacheListeners)
              .map(l -> new DeclarableType(l.getClassName(), l.getInitProperties()))
              .collect(Collectors.toList()));
    }

    if (gatewaySenderIds != null) {
      regionAttributesType.setGatewaySenderIds(StringUtils.join(gatewaySenderIds, ","));
    }

    if (asyncEventQueueIds != null) {
      regionAttributesType.setAsyncEventQueueIds(StringUtils.join(asyncEventQueueIds, ","));
    }

    if (cloningEnabled != null) {
      regionAttributesType.setCloningEnabled(cloningEnabled);
    }

    if (evictionMax != null && evictionMax < 0) {
      throw new IllegalArgumentException(CliStrings.format(
          CliStrings.ALTER_REGION__MSG__SPECIFY_POSITIVE_INT_FOR_EVICTIONMAX_0_IS_NOT_VALID,
          evictionMax));
    }

    if (evictionMax != null) {
      EvictionAttributes evictionAttributes =
          new EvictionAttributes();
      EvictionAttributes.LruEntryCount lruEntryCount =
          new EvictionAttributes.LruEntryCount();
      lruEntryCount.setMaximum(evictionMax.toString());
      evictionAttributes.setLruEntryCount(lruEntryCount);
      regionAttributesType.setEvictionAttributes(evictionAttributes);
    }

    List<CliFunctionResult> regionAlterResults =
        executeAndGetFunctionResult(new RegionAlterFunction(), deltaConfig, targetMembers);
    ResultModel result = ResultModel.createMemberStatusResult(regionAlterResults);
    result.setConfigObject(deltaConfig);
    return result;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig cacheConfig, Object configObject) {
    RegionConfig deltaConfig = (RegionConfig) configObject;
    RegionConfig existingConfig = CacheElement.findElement(cacheConfig.getRegions(),
        deltaConfig.getId());

    RegionAttributesType deltaAttributes = deltaConfig.getRegionAttributes();
    RegionAttributesType existingAttributes = existingConfig.getRegionAttributes();

    existingAttributes.setEntryIdleTime(
        combine(existingAttributes.getEntryIdleTime(), deltaAttributes.getEntryIdleTime()));
    existingAttributes.setEntryTimeToLive(
        combine(existingAttributes.getEntryTimeToLive(), deltaAttributes.getEntryTimeToLive()));
    existingAttributes.setRegionIdleTime(
        combine(existingAttributes.getRegionIdleTime(), deltaAttributes.getRegionIdleTime()));
    existingAttributes.setRegionTimeToLive(
        combine(existingAttributes.getRegionTimeToLive(), deltaAttributes.getRegionTimeToLive()));

    if (deltaAttributes.getCacheLoader() != null) {
      if (deltaAttributes.getCacheLoader().equals(DeclarableType.EMPTY)) {
        existingAttributes.setCacheLoader(null);
      } else {
        existingAttributes.setCacheLoader(deltaAttributes.getCacheLoader());
      }
    }

    if (deltaAttributes.getCacheWriter() != null) {
      if (deltaAttributes.getCacheWriter().equals(DeclarableType.EMPTY)) {
        existingAttributes.setCacheWriter(null);
      } else {
        existingAttributes.setCacheWriter(deltaAttributes.getCacheWriter());
      }
    }

    if (!deltaAttributes.getCacheListeners().isEmpty()) {
      existingAttributes.getCacheListeners().clear();
      // only add the new cache listeners to the list when it's an EMPTY cache listener
      if (!deltaAttributes.getCacheListeners().get(0).equals(DeclarableType.EMPTY)) {
        existingAttributes.getCacheListeners().addAll(deltaAttributes.getCacheListeners());
      }
    }

    if (deltaAttributes.getGatewaySenderIds() != null) {
      existingAttributes.setGatewaySenderIds(deltaAttributes.getGatewaySenderIds());
    }

    if (deltaAttributes.getAsyncEventQueueIds() != null) {
      existingAttributes.setAsyncEventQueueIds(deltaAttributes.getAsyncEventQueueIds());
    }

    if (deltaAttributes.isCloningEnabled() != null) {
      existingAttributes.setCloningEnabled(deltaAttributes.isCloningEnabled());
    }

    EvictionAttributes evictionAttributes = deltaAttributes.getEvictionAttributes();
    if (evictionAttributes != null) {
      // we only set the max in the delta's lruEntryCount in the alter region command
      String newMax = evictionAttributes.getLruEntryCount().getMaximum();
      EvictionAttributes existingEviction = existingAttributes.getEvictionAttributes();

      // we only alter the max value if there is an existing eviction attributes
      if (existingEviction != null) {
        if (existingEviction.getLruEntryCount() != null) {
          existingEviction.getLruEntryCount().setMaximum(newMax);
        }

        if (existingEviction.getLruMemorySize() != null) {
          existingEviction.getLruMemorySize().setMaximum(newMax);
        }
      }
    }
    return true;
  }

  ExpirationAttributesType getExpirationAttributes(Integer timeout,
      ExpirationAction action, ClassName expiry) {
    if (timeout == null && action == null && expiry == null) {
      return null;
    }
    if (expiry != null) {
      return new ExpirationAttributesType(timeout, action,
          expiry.getClassName(), expiry.getInitProperties());
    } else {
      return new ExpirationAttributesType(timeout, action, null, null);
    }
  }

  // this is a helper method to combine the existing with the delta ExpirationAttributesType
  ExpirationAttributesType combine(ExpirationAttributesType existing,
      ExpirationAttributesType delta) {
    if (delta == null) {
      return existing;
    }

    if (existing == null) {
      existing = new ExpirationAttributesType();
      existing.setAction(ExpirationAction.INVALIDATE.toXmlString());
      existing.setTimeout("0");
    }

    if (delta.getTimeout() != null) {
      existing.setTimeout(delta.getTimeout());
    }
    if (delta.getAction() != null) {
      existing.setAction(delta.getAction());
    }
    if (delta.getCustomExpiry() != null) {
      if (delta.getCustomExpiry().equals(DeclarableType.EMPTY)) {
        existing.setCustomExpiry(null);
      } else {
        existing.setCustomExpiry(delta.getCustomExpiry());
      }
    }
    return existing;
  }

}
