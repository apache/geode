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

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class LocateEntryCommand implements GfshCommand {
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.LOCATE_ENTRY}, help = CliStrings.LOCATE_ENTRY__HELP)
  public Result locateEntry(
      @CliOption(key = {CliStrings.LOCATE_ENTRY__KEY}, mandatory = true,
          help = CliStrings.LOCATE_ENTRY__KEY__HELP) String key,
      @CliOption(key = {CliStrings.LOCATE_ENTRY__REGIONNAME}, mandatory = true,
          help = CliStrings.LOCATE_ENTRY__REGIONNAME__HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath,
      @CliOption(key = {CliStrings.LOCATE_ENTRY__KEYCLASS},
          help = CliStrings.LOCATE_ENTRY__KEYCLASS__HELP) String keyClass,
      @CliOption(key = {CliStrings.LOCATE_ENTRY__VALUEKLASS},
          help = CliStrings.LOCATE_ENTRY__VALUEKLASS__HELP) String valueClass,
      @CliOption(key = {CliStrings.LOCATE_ENTRY__RECURSIVE},
          help = CliStrings.LOCATE_ENTRY__RECURSIVE__HELP,
          unspecifiedDefaultValue = "false") boolean recursive) {

    getSecurityService().authorizeRegionRead(regionPath, key);

    DataCommandResult dataResult;

    if (StringUtils.isEmpty(regionPath)) {
      return makePresentationResult(DataCommandResult.createLocateEntryResult(key, null, null,
          CliStrings.LOCATE_ENTRY__MSG__REGIONNAME_EMPTY, false));
    }

    if (StringUtils.isEmpty(key)) {
      return makePresentationResult(DataCommandResult.createLocateEntryResult(key, null, null,
          CliStrings.LOCATE_ENTRY__MSG__KEY_EMPTY, false));
    }

    DataCommandFunction locateEntry = new DataCommandFunction();
    Set<DistributedMember> memberList = getRegionAssociatedMembers(regionPath, getCache(), true);
    if (CollectionUtils.isNotEmpty(memberList)) {
      DataCommandRequest request = new DataCommandRequest();
      request.setCommand(CliStrings.LOCATE_ENTRY);
      request.setKey(key);
      request.setKeyClass(keyClass);
      request.setRegionName(regionPath);
      request.setValueClass(valueClass);
      request.setRecursive(recursive);
      dataResult = callFunctionForRegion(request, locateEntry, memberList);
    } else {
      dataResult = DataCommandResult.createLocateEntryInfoResult(key, null, null, CliStrings.format(
          CliStrings.LOCATE_ENTRY__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS, regionPath), false);
    }
    dataResult.setKeyClass(keyClass);
    if (valueClass != null) {
      dataResult.setValueClass(valueClass);
    }

    return makePresentationResult(dataResult);
  }
}
