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

import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ManageIndexDefinitionFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ClearDefinedIndexesCommand extends GfshCommand {
  @CliCommand(value = CliStrings.CLEAR_DEFINED_INDEXES, help = CliStrings.CLEAR_DEFINED__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public ResultModel clearDefinedIndexes() {
    IndexDefinition.indexDefinitions.clear();

    // clear the definition on the other locators as well
    Set<DistributedMember> allOtherLocators = findAllOtherLocators();
    if (allOtherLocators.size() > 0) {
      executeAndGetFunctionResult(new ManageIndexDefinitionFunction(), null, allOtherLocators);
    }

    return ResultModel.createInfo(CliStrings.CLEAR_DEFINED_INDEX__SUCCESS__MSG);
  }
}
