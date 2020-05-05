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

public class ClearCommand extends GfshCommand {
  public static final String REGION_NOT_FOUND = "Region <%s> not found in any of the members";

  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.CLEAR_REGION}, help = CliStrings.CLEAR_REGION_HELP)
  public ResultModel clear(
      @CliOption(key = {CliStrings.CLEAR_REGION_REGION_NAME}, mandatory = true,
          help = CliStrings.CLEAR_REGION_REGION_NAME_HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath) {

    Cache cache = getCache();

    authorize(Resource.DATA, Operation.WRITE, regionPath);


    Region region = cache.getRegion(regionPath);
    DataCommandFunction clearfn = createCommandFunction();
    DataCommandResult dataResult;
    if (region == null) {
      Set<DistributedMember> memberList = findAnyMembersForRegion(regionPath);

      if (CollectionUtils.isEmpty(memberList)) {
        return ResultModel.createError(String.format(REGION_NOT_FOUND, regionPath));
      }

      DataCommandRequest request = new DataCommandRequest();
      request.setCommand(CliStrings.REMOVE);
      request.setRemoveAllKeys("ALL");
      request.setRegionName(regionPath);
      dataResult = callFunctionForRegion(request, clearfn, memberList);
    } else {
      dataResult = clearfn.remove(null, null, regionPath, "ALL",
          (InternalCache) cache);
    }

    dataResult.setKeyClass(null);

    return dataResult.toResultModel();
  }

  DataCommandResult callFunctionForRegion(DataCommandRequest request, DataCommandFunction clearfn,
      Set<DistributedMember> memberList) {
    return DataCommandsUtils.callFunctionForRegion(request, clearfn, memberList);
  }

  DataCommandFunction createCommandFunction() {
    return new DataCommandFunction();
  }
}
