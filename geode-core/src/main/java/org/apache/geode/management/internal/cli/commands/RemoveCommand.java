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

public class RemoveCommand implements GfshCommand {
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.REMOVE}, help = CliStrings.REMOVE__HELP)
  public Result remove(
      @CliOption(key = {CliStrings.REMOVE__KEY}, help = CliStrings.REMOVE__KEY__HELP,
          specifiedDefaultValue = "") String key,
      @CliOption(key = {CliStrings.REMOVE__REGION}, mandatory = true,
          help = CliStrings.REMOVE__REGION__HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath,
      @CliOption(key = CliStrings.REMOVE__ALL, help = CliStrings.REMOVE__ALL__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean removeAllKeys,
      @CliOption(key = {CliStrings.REMOVE__KEYCLASS},
          help = CliStrings.REMOVE__KEYCLASS__HELP) String keyClass) {
    InternalCache cache = getCache();
    DataCommandResult dataResult;

    if (StringUtils.isEmpty(regionPath)) {
      return makePresentationResult(DataCommandResult.createRemoveResult(key, null, null,
          CliStrings.REMOVE__MSG__REGIONNAME_EMPTY, false));
    }

    if (!removeAllKeys && (key == null)) {
      return makePresentationResult(DataCommandResult.createRemoveResult(null, null, null,
          CliStrings.REMOVE__MSG__KEY_EMPTY, false));
    }

    if (removeAllKeys) {
      cache.getSecurityService().authorizeRegionWrite(regionPath);
    } else {
      cache.getSecurityService().authorizeRegionWrite(regionPath, key);
    }

    @SuppressWarnings("rawtypes")
    Region region = cache.getRegion(regionPath);
    DataCommandFunction removefn = new DataCommandFunction();
    if (region == null) {
      Set<DistributedMember> memberList = getRegionAssociatedMembers(regionPath, getCache(), false);
      if (CollectionUtils.isNotEmpty(memberList)) {
        DataCommandRequest request = new DataCommandRequest();
        request.setCommand(CliStrings.REMOVE);
        request.setKey(key);
        request.setKeyClass(keyClass);
        request.setRemoveAllKeys(removeAllKeys ? "ALL" : null);
        request.setRegionName(regionPath);
        dataResult = callFunctionForRegion(request, removefn, memberList);
      } else {
        dataResult = DataCommandResult.createRemoveInfoResult(key, null, null,
            CliStrings.format(CliStrings.REMOVE__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS, regionPath),
            false);
      }

    } else {
      dataResult = removefn.remove(key, keyClass, regionPath, removeAllKeys ? "ALL" : null);
    }
    dataResult.setKeyClass(keyClass);

    return makePresentationResult(dataResult);
  }
}
