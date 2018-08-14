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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.IFEXISTS;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.IFEXISTS_HELP;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliFunctionResult;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.DestroyAsyncEventQueueFunction;
import org.apache.geode.management.internal.cli.functions.DestroyAsyncEventQueueFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyAsyncEventQueueCommand extends InternalGfshCommand {
  public static final String DESTROY_ASYNC_EVENT_QUEUE = "destroy async-event-queue";
  public static final String DESTROY_ASYNC_EVENT_QUEUE__HELP = "destroy an Async Event Queue";
  public static final String DESTROY_ASYNC_EVENT_QUEUE__ID = "id";
  public static final String DESTROY_ASYNC_EVENT_QUEUE__ID__HELP =
      "ID of the queue to be destroyed.";
  public static final String DESTROY_ASYNC_EVENT_QUEUE__GROUP__HELP =
      "Group(s) of members on which to destroy the async event queue.";

  public static final String DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND =
      "Async event queue \"%s\" not found";
  public static final String DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED =
      "Async event queue \"%s\" destroyed";

  @CliCommand(value = DESTROY_ASYNC_EVENT_QUEUE, help = DESTROY_ASYNC_EVENT_QUEUE__HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result destroyAsyncEventQueue(
      @CliOption(key = DESTROY_ASYNC_EVENT_QUEUE__ID, mandatory = true,
          help = DESTROY_ASYNC_EVENT_QUEUE__ID__HELP) String aeqId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = DESTROY_ASYNC_EVENT_QUEUE__GROUP__HELP) String[] onGroups,
      @CliOption(key = IFEXISTS, help = IFEXISTS_HELP, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean ifExists)
      throws Throwable {
    DestroyAsyncEventQueueFunctionArgs asyncEventQueueDestoryFunctionArgs =
        new DestroyAsyncEventQueueFunctionArgs(aeqId, ifExists);

    Set<DistributedMember> members = getMembers(onGroups, null);
    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        new DestroyAsyncEventQueueFunction(), asyncEventQueueDestoryFunctionArgs, members);

    Result commandResult = ResultBuilder.buildResult(functionResults);
    XmlEntity xmlEntity = findXmlEntity(functionResults);
    if (xmlEntity != null) {
      persistClusterConfiguration(commandResult,
          () -> ((InternalConfigurationPersistenceService) getConfigurationPersistenceService())
              .deleteXmlEntity(xmlEntity, onGroups));
    }
    return commandResult;
  }
}
