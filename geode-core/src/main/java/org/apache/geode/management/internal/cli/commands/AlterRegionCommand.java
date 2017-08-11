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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.RegionAlterFunction;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class AlterRegionCommand implements GfshCommand {
  @CliCommand(value = CliStrings.ALTER_REGION, help = CliStrings.ALTER_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  public Result alterRegion(
      @CliOption(key = CliStrings.ALTER_REGION__REGION, mandatory = true,
          help = CliStrings.ALTER_REGION__REGION__HELP) String regionPath,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.ALTER_REGION__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME,
          specifiedDefaultValue = "-1",
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME__HELP) Integer entryExpirationIdleTime,
      @CliOption(key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION,
          specifiedDefaultValue = "INVALIDATE",
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION__HELP) String entryExpirationIdleTimeAction,
      @CliOption(key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE,
          specifiedDefaultValue = "-1",
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE__HELP) Integer entryExpirationTTL,
      @CliOption(key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION,
          specifiedDefaultValue = "INVALIDATE",
          help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION__HELP) String entryExpirationTTLAction,
      @CliOption(key = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME,
          specifiedDefaultValue = "-1",
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME__HELP) Integer regionExpirationIdleTime,
      @CliOption(key = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION,
          specifiedDefaultValue = "INVALIDATE",
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION__HELP) String regionExpirationIdleTimeAction,
      @CliOption(key = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL, specifiedDefaultValue = "-1",
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL__HELP) Integer regionExpirationTTL,
      @CliOption(key = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION,
          specifiedDefaultValue = "INVALIDATE",
          help = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION__HELP) String regionExpirationTTLAction,
      @CliOption(key = CliStrings.ALTER_REGION__CACHELISTENER, specifiedDefaultValue = "",
          help = CliStrings.ALTER_REGION__CACHELISTENER__HELP) String[] cacheListeners,
      @CliOption(key = CliStrings.ALTER_REGION__CACHELOADER, specifiedDefaultValue = "",
          help = CliStrings.ALTER_REGION__CACHELOADER__HELP) String cacheLoader,
      @CliOption(key = CliStrings.ALTER_REGION__CACHEWRITER, specifiedDefaultValue = "",
          help = CliStrings.ALTER_REGION__CACHEWRITER__HELP) String cacheWriter,
      @CliOption(key = CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, specifiedDefaultValue = "",
          help = CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID__HELP) String[] asyncEventQueueIds,
      @CliOption(key = CliStrings.ALTER_REGION__GATEWAYSENDERID, specifiedDefaultValue = "",
          help = CliStrings.ALTER_REGION__GATEWAYSENDERID__HELP) String[] gatewaySenderIds,
      @CliOption(key = CliStrings.ALTER_REGION__CLONINGENABLED, specifiedDefaultValue = "false",
          help = CliStrings.ALTER_REGION__CLONINGENABLED__HELP) Boolean cloningEnabled,
      @CliOption(key = CliStrings.ALTER_REGION__EVICTIONMAX, specifiedDefaultValue = "0",
          help = CliStrings.ALTER_REGION__EVICTIONMAX__HELP) Integer evictionMax) {
    Result result;
    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();

    getSecurityService().authorizeRegionManage(regionPath);

    try {
      InternalCache cache = getCache();

      if (groups != null) {
        RegionCommandsUtils.validateGroups(cache, groups);
      }

      RegionFunctionArgs.ExpirationAttrs entryIdle = null;
      if (entryExpirationIdleTime != null || entryExpirationIdleTimeAction != null) {
        if (entryExpirationIdleTime != null && entryExpirationIdleTime == -1) {
          entryExpirationIdleTime = ExpirationAttributes.DEFAULT.getTimeout();
        }
        if (CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(entryExpirationIdleTimeAction)) {
          entryExpirationIdleTimeAction = ExpirationAttributes.DEFAULT.getAction().toString();
        }
        entryIdle = new RegionFunctionArgs.ExpirationAttrs(
            RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_IDLE, entryExpirationIdleTime,
            entryExpirationIdleTimeAction);
      }
      RegionFunctionArgs.ExpirationAttrs entryTTL = null;
      if (entryExpirationTTL != null || entryExpirationTTLAction != null) {
        if (entryExpirationTTL != null && entryExpirationTTL == -1) {
          entryExpirationTTL = ExpirationAttributes.DEFAULT.getTimeout();
        }
        if (CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(entryExpirationTTLAction)) {
          entryExpirationTTLAction = ExpirationAttributes.DEFAULT.getAction().toString();
        }
        entryTTL = new RegionFunctionArgs.ExpirationAttrs(
            RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_TTL, entryExpirationTTL,
            entryExpirationTTLAction);
      }
      RegionFunctionArgs.ExpirationAttrs regionIdle = null;
      if (regionExpirationIdleTime != null || regionExpirationIdleTimeAction != null) {
        if (regionExpirationIdleTime != null && regionExpirationIdleTime == -1) {
          regionExpirationIdleTime = ExpirationAttributes.DEFAULT.getTimeout();
        }
        if (CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(regionExpirationIdleTimeAction)) {
          regionExpirationIdleTimeAction = ExpirationAttributes.DEFAULT.getAction().toString();
        }
        regionIdle = new RegionFunctionArgs.ExpirationAttrs(
            RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_IDLE, regionExpirationIdleTime,
            regionExpirationIdleTimeAction);
      }
      RegionFunctionArgs.ExpirationAttrs regionTTL = null;
      if (regionExpirationTTL != null || regionExpirationTTLAction != null) {
        if (regionExpirationTTL != null && regionExpirationTTL == -1) {
          regionExpirationTTL = ExpirationAttributes.DEFAULT.getTimeout();
        }
        if (CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(regionExpirationTTLAction)) {
          regionExpirationTTLAction = ExpirationAttributes.DEFAULT.getAction().toString();
        }
        regionTTL = new RegionFunctionArgs.ExpirationAttrs(
            RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_TTL, regionExpirationTTL,
            regionExpirationTTLAction);
      }

      cacheLoader = convertDefaultValue(cacheLoader, StringUtils.EMPTY);
      cacheWriter = convertDefaultValue(cacheWriter, StringUtils.EMPTY);

      RegionFunctionArgs regionFunctionArgs;
      regionFunctionArgs = new RegionFunctionArgs(regionPath, null, null, false, null, null, null,
          entryIdle, entryTTL, regionIdle, regionTTL, null, null, null, null, cacheListeners,
          cacheLoader, cacheWriter, asyncEventQueueIds, gatewaySenderIds, null, cloningEnabled,
          null, null, null, null, null, null, null, null, evictionMax, null, null, null, null);

      Set<String> cacheListenersSet = regionFunctionArgs.getCacheListeners();
      if (cacheListenersSet != null && !cacheListenersSet.isEmpty()) {
        for (String cacheListener : cacheListenersSet) {
          if (!RegionCommandsUtils.isClassNameValid(cacheListener)) {
            throw new IllegalArgumentException(CliStrings.format(
                CliStrings.ALTER_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELISTENER_0_IS_INVALID,
                new Object[] {cacheListener}));
          }
        }
      }

      if (cacheLoader != null && !RegionCommandsUtils.isClassNameValid(cacheLoader)) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.ALTER_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELOADER_0_IS_INVALID,
            new Object[] {cacheLoader}));
      }

      if (cacheWriter != null && !RegionCommandsUtils.isClassNameValid(cacheWriter)) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.ALTER_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHEWRITER_0_IS_INVALID,
            new Object[] {cacheWriter}));
      }

      if (evictionMax != null && evictionMax < 0) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.ALTER_REGION__MSG__SPECIFY_POSITIVE_INT_FOR_EVICTIONMAX_0_IS_NOT_VALID,
            new Object[] {evictionMax}));
      }

      Set<DistributedMember> targetMembers = CliUtil.findMembers(groups, null);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> resultCollector =
          CliUtil.executeFunction(new RegionAlterFunction(), regionFunctionArgs, targetMembers);
      List<CliFunctionResult> regionAlterResults =
          (List<CliFunctionResult>) resultCollector.getResult();

      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      final String errorPrefix = "ERROR: ";
      for (CliFunctionResult regionAlterResult : regionAlterResults) {
        boolean success = regionAlterResult.isSuccessful();
        tabularResultData.accumulate("Member", regionAlterResult.getMemberIdOrName());
        if (success) {
          tabularResultData.accumulate("Status", regionAlterResult.getMessage());
          xmlEntity.set(regionAlterResult.getXmlEntity());
        } else {
          tabularResultData.accumulate("Status", errorPrefix + regionAlterResult.getMessage());
          tabularResultData.setStatus(Result.Status.ERROR);
        }
      }
      result = ResultBuilder.buildResult(tabularResultData);
    } catch (IllegalArgumentException | IllegalStateException e) {
      LogWrapper.getInstance().info(e.getMessage());
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (RuntimeException e) {
      LogWrapper.getInstance().info(e.getMessage(), e);
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), groups));
    }
    return result;
  }



}
