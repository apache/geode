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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.RegionDestroyFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyRegionCommand implements GfshCommand {
  @CliCommand(value = {CliStrings.DESTROY_REGION}, help = CliStrings.DESTROY_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public Result destroyRegion(
      @CliOption(key = CliStrings.DESTROY_REGION__REGION, optionContext = ConverterHint.REGION_PATH,
          mandatory = true, help = CliStrings.DESTROY_REGION__REGION__HELP) String regionPath,
      @CliOption(key = CliStrings.IFEXISTS, help = CliStrings.IFEXISTS_HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifExists) {

    // regionPath should already be converted to have "/" in front of it.
    Result result;
    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();

    Set<DistributedMember> regionMembersList = findMembersForRegion(getCache(), regionPath);

    if (regionMembersList.size() == 0) {
      if (ifExists) {
        return ResultBuilder.createInfoResult("");
      } else {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.DESTROY_REGION__MSG__COULD_NOT_FIND_REGIONPATH_0_IN_GEODE,
                regionPath, "jmx-manager-update-rate milliseconds"));
      }
    }

    CliFunctionResult destroyRegionResult;

    ResultCollector<?, ?> resultCollector =
        CliUtil.executeFunction(RegionDestroyFunction.INSTANCE, regionPath, regionMembersList);
    List<CliFunctionResult> resultsList = (List<CliFunctionResult>) resultCollector.getResult();
    String message =
        CliStrings.format(CliStrings.DESTROY_REGION__MSG__REGION_0_1_DESTROYED, regionPath, "");

    // Only if there is an error is this set to false
    boolean isRegionDestroyed = true;
    for (CliFunctionResult aResultsList : resultsList) {
      destroyRegionResult = aResultsList;
      if (destroyRegionResult.isSuccessful()) {
        xmlEntity.set(destroyRegionResult.getXmlEntity());
      } else if (destroyRegionResult.getThrowable() != null) {
        Throwable t = destroyRegionResult.getThrowable();
        LogWrapper.getInstance().info(t.getMessage(), t);
        message = CliStrings.format(
            CliStrings.DESTROY_REGION__MSG__ERROR_OCCURRED_WHILE_DESTROYING_0_REASON_1, regionPath,
            t.getMessage());
        isRegionDestroyed = false;
      } else {
        message = CliStrings.format(
            CliStrings.DESTROY_REGION__MSG__UNKNOWN_RESULT_WHILE_DESTROYING_REGION_0_REASON_1,
            regionPath, destroyRegionResult.getMessage());
        isRegionDestroyed = false;
      }
    }
    if (isRegionDestroyed) {
      result = ResultBuilder.createInfoResult(message);
    } else {
      result = ResultBuilder.createUserErrorResult(message);
    }

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().deleteXmlEntity(xmlEntity.get(), null));
    }

    return result;
  }
}
