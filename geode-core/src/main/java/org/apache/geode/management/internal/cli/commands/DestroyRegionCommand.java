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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliFunctionResult;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.RegionDestroyFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyRegionCommand extends InternalGfshCommand {
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

    // this finds all the members that host this region. destroy will be called on each of these
    // members since the region might be a scope.LOCAL region
    Set<DistributedMember> regionMembersList = findMembersForRegion(regionPath);

    if (regionMembersList.size() == 0) {
      String message =
          CliStrings.format(CliStrings.DESTROY_REGION__MSG__COULD_NOT_FIND_REGIONPATH_0_IN_GEODE,
              regionPath, "jmx-manager-update-rate milliseconds");
      throw new EntityNotFoundException(message, ifExists);
    }

    // destroy is called on each member. If the region destroy is successful on one member, we
    // deem the destroy action successful, since if one member destroy successfully, the subsequent
    // destroy on a another member would probably throw RegionDestroyedException
    List<CliFunctionResult> resultsList =
        executeAndGetFunctionResult(RegionDestroyFunction.INSTANCE, regionPath, regionMembersList);


    CommandResult result = ResultBuilder.buildResult(resultsList);
    XmlEntity xmlEntity = findXmlEntity(resultsList);

    // if at least one member returns with successful deletion, we will need to update cc
    if (xmlEntity != null) {
      persistClusterConfiguration(result,
          () -> ((InternalConfigurationPersistenceService) getConfigurationPersistenceService())
              .deleteXmlEntity(xmlEntity, null));
    }
    return result;
  }

}
