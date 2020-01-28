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
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class RemoveCommand extends GfshCommand {
  public static final String REGION_NOT_FOUND = "Region <%s> not found in any of the members";

  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.REMOVE}, help = CliStrings.REMOVE__HELP)
  public ResultModel remove(
      @CliOption(key = {CliStrings.REMOVE__REGION}, mandatory = true,
          help = CliStrings.REMOVE__REGION__HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath,
      @CliOption(key = {CliStrings.REMOVE__KEY}, help = CliStrings.REMOVE__KEY__HELP,
          specifiedDefaultValue = "") String key,
      @CliOption(key = CliStrings.REMOVE__ALL, help = CliStrings.REMOVE__ALL__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean removeAllKeys,
      @CliOption(key = {CliStrings.REMOVE__KEYCLASS},
          help = CliStrings.REMOVE__KEYCLASS__HELP) String keyClass) {
    Cache cache = getCache();

    if (!removeAllKeys && (key == null)) {
      return new ResultModel().createError(CliStrings.REMOVE__MSG__KEY_EMPTY);
    }

    if (removeAllKeys) {
      authorize(Resource.DATA, Operation.WRITE, regionPath);
    } else {
      authorize(Resource.DATA, Operation.WRITE, regionPath, key);
    }

    key = DataCommandsUtils.makeBrokenJsonCompliant(key);

    Region region = cache.getRegion(regionPath);
    DataCommandFunction removefn = new DataCommandFunction();
    DataCommandResult dataResult;
    if (region == null) {
      Set<DistributedMember> memberList = findAnyMembersForRegion(regionPath);

      if (CollectionUtils.isEmpty(memberList)) {
        return new ResultModel().createError(String.format(REGION_NOT_FOUND, regionPath));
      }

      DataCommandRequest request = new DataCommandRequest();
      request.setCommand(CliStrings.REMOVE);
      request.setKey(key);
      request.setKeyClass(keyClass);
      request.setRemoveAllKeys(removeAllKeys ? "ALL" : null);
      request.setRegionName(regionPath);
      dataResult = callFunctionForRegion(request, removefn, memberList);
    } else {
      dataResult = removefn.remove(key, keyClass, regionPath, removeAllKeys ? "ALL" : null,
          (InternalCache) cache);
    }

    dataResult.setKeyClass(keyClass);
    ResultModel result;

    if (removeAllKeys) {
      result = dataResult.toResultModel(CliStrings.REMOVE__MSG__CLEARALL_DEPRECATION_WARNING);
    } else {
      result = dataResult.toResultModel();
    }

    return result;
  }
}
