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

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.ClassName;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.RegionAlterFunction;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class AlterRegionCommand extends InternalGfshCommand {
  @CliCommand(value = CliStrings.ALTER_REGION, help = CliStrings.ALTER_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  public Result alterRegion(
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
          unspecifiedDefaultValue = "false",
          help = CliStrings.ALTER_REGION__CLONINGENABLED__HELP) boolean cloningEnabled,
      @CliOption(key = CliStrings.ALTER_REGION__EVICTIONMAX, specifiedDefaultValue = "0",
          help = CliStrings.ALTER_REGION__EVICTIONMAX__HELP) Integer evictionMax) {
    Result result;

    authorize(Resource.DATA, Operation.MANAGE, regionPath);

    InternalCache cache = (InternalCache) getCache();

    if (groups != null) {
      RegionCommandsUtils.validateGroups(cache, groups);
    }

    RegionFunctionArgs regionFunctionArgs = new RegionFunctionArgs();
    regionFunctionArgs.setRegionPath(regionPath);
    regionFunctionArgs.setEntryExpirationIdleTime(entryExpirationIdleTime,
        entryExpirationIdleTimeAction);
    regionFunctionArgs.setEntryExpirationTTL(entryExpirationTTL, entryExpirationTTLAction);
    regionFunctionArgs.setEntryIdleTimeCustomExpiry(entryIdleTimeCustomExpiry);
    regionFunctionArgs.setEntryTTLCustomExpiry(entryTTLCustomExpiry);
    regionFunctionArgs.setRegionExpirationIdleTime(regionExpirationIdleTime,
        regionExpirationIdleTimeAction);
    regionFunctionArgs.setRegionExpirationTTL(regionExpirationTTL, regionExpirationTTLAction);
    regionFunctionArgs.setCacheListeners(cacheListeners);
    regionFunctionArgs.setCacheLoader(cacheLoader);
    regionFunctionArgs.setCacheWriter(cacheWriter);
    regionFunctionArgs.setAsyncEventQueueIds(asyncEventQueueIds);
    regionFunctionArgs.setGatewaySenderIds(gatewaySenderIds);
    regionFunctionArgs.setCloningEnabled(cloningEnabled);
    regionFunctionArgs.setEvictionMax(evictionMax);

    if (evictionMax != null && evictionMax < 0) {
      throw new IllegalArgumentException(CliStrings.format(
          CliStrings.ALTER_REGION__MSG__SPECIFY_POSITIVE_INT_FOR_EVICTIONMAX_0_IS_NOT_VALID,
          evictionMax));
    }

    Set<DistributedMember> targetMembers = findMembers(groups, null);

    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    List<CliFunctionResult> regionAlterResults =
        executeAndGetFunctionResult(new RegionAlterFunction(), regionFunctionArgs, targetMembers);
    result = ResultBuilder.buildResult(regionAlterResults);

    XmlEntity xmlEntity = findXmlEntity(regionAlterResults);
    if (xmlEntity != null) {
      persistClusterConfiguration(result,
          () -> getConfigurationPersistenceService()
              .addXmlEntity(xmlEntity, groups));
    }
    return result;
  }

}
