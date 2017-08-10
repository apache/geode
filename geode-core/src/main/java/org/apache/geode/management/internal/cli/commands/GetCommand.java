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
import static org.apache.geode.management.internal.cli.commands.DataCommandsUtils.getRegionAssociatedMembers;
import static org.apache.geode.management.internal.cli.commands.DataCommandsUtils.makePresentationResult;

import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.shiro.subject.Subject;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class GetCommand implements GfshCommand {
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.GET}, help = CliStrings.GET__HELP)
  public Result get(
      @CliOption(key = {CliStrings.GET__KEY}, mandatory = true,
          help = CliStrings.GET__KEY__HELP) String key,
      @CliOption(key = {CliStrings.GET__REGIONNAME}, mandatory = true,
          help = CliStrings.GET__REGIONNAME__HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath,
      @CliOption(key = {CliStrings.GET__KEYCLASS},
          help = CliStrings.GET__KEYCLASS__HELP) String keyClass,
      @CliOption(key = {CliStrings.GET__VALUEKLASS},
          help = CliStrings.GET__VALUEKLASS__HELP) String valueClass,
      @CliOption(key = CliStrings.GET__LOAD, unspecifiedDefaultValue = "true",
          specifiedDefaultValue = "true",
          help = CliStrings.GET__LOAD__HELP) Boolean loadOnCacheMiss) {

    InternalCache cache = getCache();
    cache.getSecurityService().authorizeRegionRead(regionPath, key);
    DataCommandResult dataResult;

    if (StringUtils.isEmpty(regionPath)) {
      return makePresentationResult(DataCommandResult.createGetResult(key, null, null,
          CliStrings.GET__MSG__REGIONNAME_EMPTY, false));
    }

    if (StringUtils.isEmpty(key)) {
      return makePresentationResult(DataCommandResult.createGetResult(key, null, null,
          CliStrings.GET__MSG__KEY_EMPTY, false));
    }

    @SuppressWarnings("rawtypes")
    Region region = cache.getRegion(regionPath);
    DataCommandFunction getfn = new DataCommandFunction();
    if (region == null) {
      Set<DistributedMember> memberList = getRegionAssociatedMembers(regionPath, getCache(), false);
      if (CollectionUtils.isNotEmpty(memberList)) {
        DataCommandRequest request = new DataCommandRequest();
        request.setCommand(CliStrings.GET);
        request.setKey(key);
        request.setKeyClass(keyClass);
        request.setRegionName(regionPath);
        request.setValueClass(valueClass);
        request.setLoadOnCacheMiss(loadOnCacheMiss);
        Subject subject = cache.getSecurityService().getSubject();
        if (subject != null) {
          request.setPrincipal(subject.getPrincipal());
        }
        dataResult = callFunctionForRegion(request, getfn, memberList);
      } else {
        dataResult = DataCommandResult.createGetInfoResult(key, null, null,
            CliStrings.format(CliStrings.GET__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS, regionPath),
            false);
      }
    } else {
      dataResult = getfn.get(null, key, keyClass, valueClass, regionPath, loadOnCacheMiss,
          cache.getSecurityService());
    }
    dataResult.setKeyClass(keyClass);
    if (valueClass != null) {
      dataResult.setValueClass(valueClass);
    }

    return makePresentationResult(dataResult);
  }
}
