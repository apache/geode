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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.functions.FetchSharedConfigurationStatusFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.configuration.domain.SharedConfigurationStatus;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class StatusClusterConfigServiceCommand extends GfshCommand {
  @CliCommand(value = CliStrings.STATUS_SHARED_CONFIG, help = CliStrings.STATUS_SHARED_CONFIG_HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_LOCATOR)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public ResultModel statusSharedConfiguration() {
    final InternalCache cache = (InternalCache) getCache();
    final Set<DistributedMember> locators = new HashSet<>(
        cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration().keySet());

    if (locators.isEmpty()) {
      return ResultModel.createInfo(CliStrings.NO_LOCATORS_WITH_SHARED_CONFIG);
    }

    ResultModel resultModel = new ResultModel();
    TabularResultModel tabularResultModel =
        resultModel.addTable("Status of shared configuration on locators");
    if (!populateSharedConfigurationStatus(locators, tabularResultModel)) {
      resultModel.setStatus(Status.ERROR);
    }

    return resultModel;
  }

  private boolean populateSharedConfigurationStatus(Set<DistributedMember> locators,
      TabularResultModel tabularResultModel) {
    boolean isSharedConfigRunning = false;
    ResultCollector<?, ?> rc =
        ManagementUtils.executeFunction(new FetchSharedConfigurationStatusFunction(), null,
            locators);
    @SuppressWarnings("unchecked")
    List<CliFunctionResult> results = (List<CliFunctionResult>) rc.getResult();

    for (CliFunctionResult result : results) {
      tabularResultModel.accumulate(CliStrings.STATUS_SHARED_CONFIG_NAME_HEADER,
          result.getMemberIdOrName());
      String status = (String) result.getResultObject();
      tabularResultModel.accumulate(CliStrings.STATUS_SHARED_CONFIG_STATUS, status);
      if (SharedConfigurationStatus.RUNNING.name().equals(status)) {
        isSharedConfigRunning = true;
      }
    }

    return isSharedConfigRunning;
  }
}
