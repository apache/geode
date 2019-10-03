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

import static org.apache.geode.management.internal.cli.commands.DataCommandsUtils.callFunctionForRegion;

import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class PutCommand extends GfshCommand {
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.PUT}, help = CliStrings.PUT__HELP)
  public ResultModel put(
      @CliOption(key = {CliStrings.PUT__KEY}, mandatory = true,
          help = CliStrings.PUT__KEY__HELP) String key,
      @CliOption(key = {CliStrings.PUT__VALUE}, mandatory = true,
          help = CliStrings.PUT__VALUE__HELP) String value,
      @CliOption(key = {CliStrings.PUT__REGIONNAME}, mandatory = true,
          help = CliStrings.PUT__REGIONNAME__HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath,
      @CliOption(key = {CliStrings.PUT__KEYCLASS},
          help = CliStrings.PUT__KEYCLASS__HELP) String keyClass,
      @CliOption(key = {CliStrings.PUT__VALUEKLASS},
          help = CliStrings.PUT__VALUEKLASS__HELP) String valueClass,
      @CliOption(key = {CliStrings.PUT__PUTIFABSENT}, help = CliStrings.PUT__PUTIFABSENT__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean putIfAbsent,
      @CliOption(key = {CliStrings.IFNOTEXISTS}, help = CliStrings.PUT__PUTIFNOTEXISTS__HELP,
          specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean putIfNotExists) {

    Cache cache = getCache();
    authorize(Resource.DATA, Operation.WRITE, regionPath);
    DataCommandResult dataResult;

    // For some reason we decided to delimit JSON strings with '(' and ')'
    key = DataCommandsUtils.makeBrokenJsonCompliant(key);
    value = DataCommandsUtils.makeBrokenJsonCompliant(value);

    LogService.getLogger().info("Cache instance for put: " + cache.toString());
    @SuppressWarnings("rawtypes")
    Region region = cache.getRegion(regionPath);
    DataCommandFunction putfn = new DataCommandFunction();
    if (region == null) {
      Set<DistributedMember> memberList = findAnyMembersForRegion(regionPath);
      if (CollectionUtils.isNotEmpty(memberList)) {
        DataCommandRequest request = new DataCommandRequest();
        request.setCommand(CliStrings.PUT);
        request.setValue(value);
        request.setKey(key);
        request.setKeyClass(keyClass);
        request.setRegionName(regionPath);
        request.setValueClass(valueClass);
        request.setPutIfAbsent(putIfNotExists || putIfAbsent);
        dataResult = callFunctionForRegion(request, putfn, memberList);
      } else {
        dataResult = DataCommandResult.createPutInfoResult(key, value, null,
            CliStrings.format(CliStrings.PUT__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS, regionPath),
            false);
      }
    } else {
      dataResult = putfn.put(key, value, putIfNotExists || putIfAbsent, keyClass, valueClass,
          regionPath, (InternalCache) cache);
    }
    dataResult.setKeyClass(keyClass);
    if (valueClass != null) {
      dataResult.setValueClass(valueClass);
    }
    return dataResult.toResultModel();
  }
}
