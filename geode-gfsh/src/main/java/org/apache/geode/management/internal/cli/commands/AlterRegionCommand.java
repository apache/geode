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

import static org.apache.geode.lang.Identifiable.find;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionAttributesType.EvictionAttributes;
import org.apache.geode.cache.configuration.RegionAttributesType.ExpirationAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.management.internal.cli.functions.RegionAlterFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class AlterRegionCommand extends SingleGfshCommand {
  @ShellMethod(value = CliStrings.ALTER_REGION__HELP, key = CliStrings.ALTER_REGION)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  public ResultModel alterRegion(
      @ShellOption(value = CliStrings.ALTER_REGION__REGION,
          help = CliStrings.ALTER_REGION__REGION__HELP) String regionPath,
      @ShellOption(value = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.ALTER_REGION__GROUP__HELP) String[] groups,
      @ShellOption(value = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME,
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME__HELP) Integer entryExpirationIdleTime,
      @ShellOption(value = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION,
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION__HELP) ExpirationAction entryExpirationIdleTimeAction,
      @ShellOption(value = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE,
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE__HELP) Integer entryExpirationTTL,
      @ShellOption(value = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION,
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION__HELP) ExpirationAction entryExpirationTTLAction,
      @ShellOption(value = CliStrings.ENTRY_IDLE_TIME_CUSTOM_EXPIRY,
          help = CliStrings.ENTRY_IDLE_TIME_CUSTOM_EXPIRY_HELP) ClassName entryIdleTimeCustomExpiry,
      @ShellOption(value = CliStrings.ENTRY_TTL_CUSTOM_EXPIRY,
          help = CliStrings.ENTRY_TTL_CUSTOM_EXPIRY_HELP) ClassName entryTTLCustomExpiry,
      @ShellOption(value = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME,
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME__HELP) Integer regionExpirationIdleTime,
      @ShellOption(value = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION,
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION__HELP) ExpirationAction regionExpirationIdleTimeAction,
      @ShellOption(value = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL,
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL__HELP) Integer regionExpirationTTL,
      @ShellOption(value = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION,
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION__HELP) ExpirationAction regionExpirationTTLAction,
      @ShellOption(value = CliStrings.ALTER_REGION__CACHELISTENER, // split the input only with
                                                                   // comma outside of json
                                                                   // string,(?![^{]*\\})",
          help = CliStrings.ALTER_REGION__CACHELISTENER__HELP) ClassName[] cacheListeners,
      @ShellOption(value = CliStrings.ALTER_REGION__CACHELOADER,
          help = CliStrings.ALTER_REGION__CACHELOADER__HELP) ClassName cacheLoader,
      @ShellOption(value = CliStrings.ALTER_REGION__CACHEWRITER,
          help = CliStrings.ALTER_REGION__CACHEWRITER__HELP) ClassName cacheWriter,
      @ShellOption(value = CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
          help = CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID__HELP) String[] asyncEventQueueIds,
      @ShellOption(value = CliStrings.ALTER_REGION__GATEWAYSENDERID,
          help = CliStrings.ALTER_REGION__GATEWAYSENDERID__HELP) String[] gatewaySenderIds,
      @ShellOption(value = CliStrings.ALTER_REGION__CLONINGENABLED,
          help = CliStrings.ALTER_REGION__CLONINGENABLED__HELP) Boolean cloningEnabled,
      @ShellOption(value = CliStrings.ALTER_REGION__EVICTIONMAX,
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
              find(clusterConfig.getRegions(), regionPath.substring(1));
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
    regionAttributesType.setEntryIdleTime(ExpirationAttributesType.generate(entryExpirationIdleTime,
        (entryExpirationIdleTimeAction == null) ? null
            : entryExpirationIdleTimeAction.toXmlString(),
        entryIdleTimeCustomExpiry));
    regionAttributesType.setEntryTimeToLive(ExpirationAttributesType.generate(entryExpirationTTL,
        (entryExpirationTTLAction == null) ? null : entryExpirationTTLAction.toXmlString(),
        entryTTLCustomExpiry));
    regionAttributesType.setRegionIdleTime(
        ExpirationAttributesType.generate(regionExpirationIdleTime,
            (regionExpirationIdleTimeAction == null) ? null
                : regionExpirationIdleTimeAction.toXmlString(),
            null));
    regionAttributesType.setRegionTimeToLive(
        ExpirationAttributesType.generate(regionExpirationTTL,
            (regionExpirationTTLAction == null) ? null : regionExpirationTTLAction.toXmlString(),
            null));
    if (cacheLoader != null) {
      // Shell 3.x: Empty string converted to ClassName.EMPTY to signal removal
      if (cacheLoader.equals(ClassName.EMPTY)) {
        regionAttributesType.setCacheLoader(DeclarableType.EMPTY);
      } else {
        regionAttributesType.setCacheLoader(
            new DeclarableType(cacheLoader.getClassName(), cacheLoader.getInitProperties()));
      }
    }

    if (cacheWriter != null) {
      // Shell 3.x: Empty string converted to ClassName.EMPTY to signal removal
      if (cacheWriter.equals(ClassName.EMPTY)) {
        regionAttributesType.setCacheWriter(DeclarableType.EMPTY);
      } else {
        regionAttributesType.setCacheWriter(
            new DeclarableType(cacheWriter.getClassName(), cacheWriter.getInitProperties()));
      }
    }

    if (cacheListeners != null) {
      // Shell 3.x: Empty string in array converted to ClassName.EMPTY to signal removal
      regionAttributesType.getCacheListeners().addAll(
          Arrays.stream(cacheListeners)
              .map(l -> l.equals(ClassName.EMPTY) ? DeclarableType.EMPTY
                  : new DeclarableType(l.getClassName(), l.getInitProperties()))
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
    RegionConfig existingConfig = find(cacheConfig.getRegions(),
        deltaConfig.getId());

    RegionAttributesType deltaAttributes = deltaConfig.getRegionAttributes();
    RegionAttributesType existingAttributes = existingConfig.getRegionAttributes();

    existingAttributes.setEntryIdleTime(
        ExpirationAttributesType.combine(existingAttributes.getEntryIdleTime(),
            deltaAttributes.getEntryIdleTime()));
    existingAttributes.setEntryTimeToLive(
        ExpirationAttributesType.combine(existingAttributes.getEntryTimeToLive(),
            deltaAttributes.getEntryTimeToLive()));
    existingAttributes.setRegionIdleTime(
        ExpirationAttributesType.combine(existingAttributes.getRegionIdleTime(),
            deltaAttributes.getRegionIdleTime()));
    existingAttributes.setRegionTimeToLive(
        ExpirationAttributesType.combine(existingAttributes.getRegionTimeToLive(),
            deltaAttributes.getRegionTimeToLive()));

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
}
