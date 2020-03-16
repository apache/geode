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

import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.logging.internal.log4j.LogLevel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.AlterRuntimeConfigFunction;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.security.ResourcePermission;

public class AlterRuntimeConfigCommand extends GfshCommand {
  private final AlterRuntimeConfigFunction alterRunTimeConfigFunction =
      new AlterRuntimeConfigFunction();
  private static final Logger logger = LogService.getLogger();

  @CliCommand(value = {CliStrings.ALTER_RUNTIME_CONFIG},
      help = CliStrings.ALTER_RUNTIME_CONFIG__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG},
      interceptor = "org.apache.geode.management.internal.cli.commands.AlterRuntimeConfigCommand$AlterRuntimeInterceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  @SuppressWarnings("deprecation")
  public ResultModel alterRuntimeConfig(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.ALTER_RUNTIME_CONFIG__MEMBER__HELP) String[] memberNameOrId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.ALTER_RUNTIME_CONFIG__MEMBER__HELP) String[] group,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT},
          help = CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT__HELP) Integer archiveDiskSpaceLimit,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT},
          help = CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT__HELP) Integer archiveFileSizeLimit,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT},
          help = CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT__HELP) Integer logDiskSpaceLimit,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT},
          help = CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT__HELP) Integer logFileSizeLimit,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL},
          optionContext = ConverterHint.LOG_LEVEL,
          help = CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL__HELP) String logLevel,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE},
          help = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE__HELP) String statisticArchiveFile,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE},
          help = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE__HELP) Integer statisticSampleRate,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED},
          help = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED__HELP) Boolean statisticSamplingEnabled,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ},
          specifiedDefaultValue = "false",
          help = CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ__HELP) Boolean setCopyOnRead,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE},
          help = CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE__HELP) Integer lockLease,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT},
          help = CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT__HELP) Integer lockTimeout,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL},
          help = CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL__HELP) Integer messageSyncInterval,
      @CliOption(key = {CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT},
          help = CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT__HELP) Integer searchTimeout) {

    Map<String, String> runTimeDistributionConfigAttributes = new HashMap<>();
    Map<String, String> rumTimeCacheAttributes = new HashMap<>();
    Set<DistributedMember> targetMembers = findMembers(group, memberNameOrId);

    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    if (archiveDiskSpaceLimit != null) {
      runTimeDistributionConfigAttributes.put(
          CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT,
          archiveDiskSpaceLimit.toString());
    }

    if (archiveFileSizeLimit != null) {
      runTimeDistributionConfigAttributes.put(
          CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT,
          archiveFileSizeLimit.toString());
    }

    if (logDiskSpaceLimit != null) {
      runTimeDistributionConfigAttributes.put(
          CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, logDiskSpaceLimit.toString());
    }

    if (logFileSizeLimit != null) {
      runTimeDistributionConfigAttributes.put(
          CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, logFileSizeLimit.toString());
    }

    if (logLevel != null && !logLevel.isEmpty()) {
      runTimeDistributionConfigAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL,
          logLevel);
    }

    if (statisticArchiveFile != null && !statisticArchiveFile.isEmpty()) {
      runTimeDistributionConfigAttributes
          .put(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, statisticArchiveFile);
    }

    if (statisticSampleRate != null) {
      runTimeDistributionConfigAttributes.put(
          CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, statisticSampleRate.toString());
    }

    if (statisticSamplingEnabled != null) {
      runTimeDistributionConfigAttributes.put(STATISTIC_SAMPLING_ENABLED,
          statisticSamplingEnabled.toString());
    }


    // Attributes that are set on the cache.
    if (setCopyOnRead != null) {
      rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ,
          setCopyOnRead.toString());
    }

    if (lockLease != null && lockLease > 0 && lockLease < Integer.MAX_VALUE) {
      rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE,
          lockLease.toString());
    }

    if (lockTimeout != null && lockTimeout > 0 && lockTimeout < Integer.MAX_VALUE) {
      rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT,
          lockTimeout.toString());
    }

    if (messageSyncInterval != null && messageSyncInterval > 0
        && messageSyncInterval < Integer.MAX_VALUE) {
      rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL,
          messageSyncInterval.toString());
    }

    if (searchTimeout != null && searchTimeout > 0 && searchTimeout < Integer.MAX_VALUE) {
      rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT,
          searchTimeout.toString());
    }

    if (runTimeDistributionConfigAttributes.isEmpty() && rumTimeCacheAttributes.isEmpty()) {
      return ResultModel.createError(CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE);
    }

    Map<String, String> allRunTimeAttributes = new HashMap<>();
    allRunTimeAttributes.putAll(runTimeDistributionConfigAttributes);
    allRunTimeAttributes.putAll(rumTimeCacheAttributes);

    ResultCollector<?, ?> rc =
        ManagementUtils
            .executeFunction(alterRunTimeConfigFunction, allRunTimeAttributes, targetMembers);
    List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());
    Set<String> successfulMembers = new TreeSet<>();
    Set<String> errorMessages = new TreeSet<>();

    for (CliFunctionResult result : results) {
      if (result.getThrowable() != null) {
        logger.info("Function failed: " + result.getThrowable());
        errorMessages.add(result.getThrowable().getMessage());
      } else {
        successfulMembers.add(result.getMemberIdOrName());
      }
    }
    final String lineSeparator = System.getProperty("line.separator");


    if (!successfulMembers.isEmpty()) {
      StringBuilder successMessageBuilder = new StringBuilder();

      successMessageBuilder.append(CliStrings.ALTER_RUNTIME_CONFIG__SUCCESS__MESSAGE);
      successMessageBuilder.append(lineSeparator);

      for (String member : successfulMembers) {
        successMessageBuilder.append(member);
        successMessageBuilder.append(lineSeparator);
      }

      Properties properties = new Properties();
      properties.putAll(runTimeDistributionConfigAttributes);

      ResultModel result = new ResultModel();
      InfoResultModel successInfo = result.addInfo("success");
      successInfo.addLine(successMessageBuilder.toString());
      // Set the Cache attributes to be modified
      final XmlEntity xmlEntity = XmlEntity.builder().withType(CacheXml.CACHE)
          .withAttributes(rumTimeCacheAttributes).build();
      InternalConfigurationPersistenceService cps = getConfigurationPersistenceService();
      if (cps == null) {
        successInfo.addLine(CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      } else {
        cps.modifyXmlAndProperties(properties, xmlEntity, group);
      }
      return result;
    } else {
      StringBuilder errorMessageBuilder = new StringBuilder();
      errorMessageBuilder.append("Following errors occurred while altering runtime config");
      errorMessageBuilder.append(lineSeparator);

      for (String errorMessage : errorMessages) {
        errorMessageBuilder.append(errorMessage);
        errorMessageBuilder.append(lineSeparator);
      }
      return ResultModel.createError(errorMessageBuilder.toString());
    }
  }

  public static class AlterRuntimeInterceptor extends AbstractCliAroundInterceptor {
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      // validate log level
      String logLevel = parseResult.getParamValueAsString("log-level");
      if (StringUtils.isNotBlank(logLevel) && (LogLevel.getLevel(logLevel) == null)) {
        return ResultModel.createError("Invalid log level: " + logLevel);
      }
      return ResultModel.createInfo("");
    }
  }
}
