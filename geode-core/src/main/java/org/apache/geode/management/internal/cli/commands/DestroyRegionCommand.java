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
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifExists)
      throws Throwable {

    // regionPath should already be converted to have "/" in front of it.
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

    ResultCollector<?, ?> resultCollector =
        executeFunction(RegionDestroyFunction.INSTANCE, regionPath, regionMembersList);
    List<CliFunctionResult> resultsList = (List<CliFunctionResult>) resultCollector.getResult();

    // destroy is called on each member, if any error happens in any one of the member, we should
    // deem the destroy not successful.
    String errorMessage = null;
    for (CliFunctionResult functionResult : resultsList) {
      if (functionResult.isSuccessful()) {
        xmlEntity.set(functionResult.getXmlEntity());
      } else {
        if (functionResult.getThrowable() != null) {
          throw functionResult.getThrowable();
        }
        if (functionResult.getMessage() != null) {
          errorMessage = functionResult.getMessage();
        } else {
          errorMessage = "Destroy failed on one member";
        }
        // if any error occurred, break out without looking further
        break;
      }
    }

    if (errorMessage != null) {
      return ResultBuilder.createGemFireErrorResult(errorMessage);
    }

    Result result =
        ResultBuilder.createInfoResult(String.format("\"$s\" destroyed successfully.", regionPath));
    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().deleteXmlEntity(xmlEntity.get(), null));
    }
    return result;
  }
}
