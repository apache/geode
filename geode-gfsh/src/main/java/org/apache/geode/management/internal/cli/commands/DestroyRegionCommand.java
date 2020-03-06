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

import static org.apache.geode.lang.Identifiable.find;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.RegionDestroyFunction;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyRegionCommand extends GfshCommand {
  @CliCommand(value = {CliStrings.DESTROY_REGION}, help = CliStrings.DESTROY_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel destroyRegion(
      @CliOption(key = CliStrings.DESTROY_REGION__REGION, optionContext = ConverterHint.REGION_PATH,
          mandatory = true, help = CliStrings.DESTROY_REGION__REGION__HELP) String regionPath,
      @CliOption(key = CliStrings.IFEXISTS, help = CliStrings.IFEXISTS_HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifExists) {

    // this finds all the members that host this region. destroy will be called on each of these
    // members since the region might be a scope.LOCAL region
    Set<DistributedMember> regionMembersList = findMembersForRegion(regionPath);

    if (regionMembersList.size() == 0) {
      String message =
          CliStrings.format(CliStrings.DESTROY_REGION__MSG__COULD_NOT_FIND_REGIONPATH_0_IN_GEODE,
              regionPath, "jmx-manager-update-rate milliseconds");
      throw new EntityNotFoundException(message, ifExists);
    }

    try {
      checkForJDBCMapping(regionPath);
    } catch (IllegalStateException e) {
      return ResultModel.createError(e.getMessage());
    }

    // destroy is called on each member. If the region destroy is successful on one member, we
    // deem the destroy action successful, since if one member destroy successfully, the subsequent
    // destroy on a another member would probably throw RegionDestroyedException
    List<CliFunctionResult> resultsList =
        executeAndGetFunctionResult(RegionDestroyFunction.INSTANCE, regionPath, regionMembersList);


    ResultModel result = ResultModel.createMemberStatusResult(resultsList);
    XmlEntity xmlEntity = findXmlEntity(resultsList);

    // if at least one member returns with successful deletion, we will need to update cc
    InternalConfigurationPersistenceService configurationPersistenceService =
        getConfigurationPersistenceService();
    if (xmlEntity != null) {
      if (configurationPersistenceService == null) {
        result.addInfo().addLine(CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      } else {
        configurationPersistenceService.deleteXmlEntity(xmlEntity, null);
      }
    }
    return result;
  }

  @SuppressWarnings("deprecation")
  private XmlEntity findXmlEntity(List<CliFunctionResult> functionResults) {
    return functionResults.stream().filter(CliFunctionResult::isSuccessful)
        .map(CliFunctionResult::getXmlEntity).filter(Objects::nonNull).findFirst().orElse(null);
  }

  void checkForJDBCMapping(String regionPath) {
    String regionName = regionPath;
    if (regionPath.startsWith("/")) {
      regionName = regionPath.substring(1);
    }
    InternalConfigurationPersistenceService ccService = getConfigurationPersistenceService();
    if (ccService == null) {
      return;
    }

    Set<String> groupNames = new HashSet<>(ccService.getGroups());
    groupNames.add("cluster");
    for (String groupName : groupNames) {
      CacheConfig cacheConfig = ccService.getCacheConfig(groupName);
      if (cacheConfig != null) {
        RegionConfig regionConfig = find(cacheConfig.getRegions(), regionName);
        if (regionConfig != null) {
          Identifiable<?> element =
              find(regionConfig.getCustomRegionElements(), "jdbc-mapping");
          if (element != null) {
            throw new IllegalStateException("Cannot destroy region \"" + regionName
                + "\" because JDBC mapping exists. Use \"destroy jdbc-mapping\" first.");
          }
        }
      }
    }
  }
}
