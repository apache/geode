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

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliFunctionResult;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.GatewaySenderDestroyFunction;
import org.apache.geode.management.internal.cli.functions.GatewaySenderDestroyFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyGatewaySenderCommand extends InternalGfshCommand {
  private static final Logger logger = LogService.getLogger();

  @CliCommand(value = CliStrings.DESTROY_GATEWAYSENDER,
      help = CliStrings.DESTROY_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public Result destroyGatewaySender(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.DESTROY_GATEWAYSENDER__GROUP__HELP) String[] onGroups,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.DESTROY_GATEWAYSENDER__MEMBER__HELP) String[] onMember,
      @CliOption(key = CliStrings.DESTROY_GATEWAYSENDER__ID, mandatory = true,
          optionContext = ConverterHint.GATEWAY_SENDER_ID,
          help = CliStrings.DESTROY_GATEWAYSENDER__ID__HELP) String id,
      @CliOption(key = CliStrings.IFEXISTS, help = CliStrings.IFEXISTS_HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifExist) {

    GatewaySenderDestroyFunctionArgs gatewaySenderDestroyFunctionArgs =
        new GatewaySenderDestroyFunctionArgs(id, ifExist);

    Set<DistributedMember> members = getMembers(onGroups, onMember);

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        GatewaySenderDestroyFunction.INSTANCE, gatewaySenderDestroyFunctionArgs, members);

    CommandResult result = ResultBuilder.buildResult(functionResults);
    XmlEntity xmlEntity = findXmlEntity(functionResults);

    // no xml needs to be updated, simply return
    if (xmlEntity == null) {
      return result;
    }

    // has xml but unable to persist to cluster config, need to print warning message and return
    if (onMember != null || getConfigurationPersistenceService() == null) {
      result.setCommandPersisted(false);
      return result;
    }

    // update cluster config
    ((InternalConfigurationPersistenceService) getConfigurationPersistenceService())
        .deleteXmlEntity(xmlEntity, onGroups);
    return result;
  }
}
