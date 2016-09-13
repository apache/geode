/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.commands;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.FetchSharedConfigurationStatusFunction;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.configuration.domain.SharedConfigurationStatus;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;


public class StatusCommands extends AbstractCommandsSupport implements CommandMarker {
  static final FetchSharedConfigurationStatusFunction fetchSharedConfigStatusFunction = new FetchSharedConfigurationStatusFunction(); 

  @SuppressWarnings("unchecked")
  @CliCommand (value = CliStrings.STATUS_SHARED_CONFIG, help = CliStrings.STATUS_SHARED_CONFIG_HELP)
  @CliMetaData (relatedTopic = CliStrings.TOPIC_GEODE_LOCATOR)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result statusSharedConfiguration() {
    final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    final Set<DistributedMember> locators = new HashSet<DistributedMember>(cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration().keySet());
    if (locators.isEmpty()) {
      return ResultBuilder.createInfoResult(CliStrings.NO_LOCATORS_WITH_SHARED_CONFIG);
    } else {
      return ResultBuilder.buildResult(getSharedConfigurationStatus(locators));
    }
  }
  
  private TabularResultData getSharedConfigurationStatus(Set<DistributedMember> locators) {
    boolean isSharedConfigRunning = false;
    ResultCollector<?,?> rc = CliUtil.executeFunction(fetchSharedConfigStatusFunction, null, locators);
    List<CliFunctionResult> results = (List<CliFunctionResult>)rc.getResult();
    TabularResultData table = ResultBuilder.createTabularResultData();
    table.setHeader("Status of shared configuration on locators");

    for (CliFunctionResult result : results) {
      table.accumulate(CliStrings.STATUS_SHARED_CONFIG_NAME_HEADER,  result.getMemberIdOrName());
      String status = (String) result.getSerializables()[0];
      table.accumulate(CliStrings.STATUS_SHARED_CONFIG_STATUS, status);
      if (SharedConfigurationStatus.RUNNING.name().equals(status)) {
        isSharedConfigRunning = true;
      }
    }

    if (!isSharedConfigRunning) {
      table.setStatus(Status.ERROR);
    }
    return table;
  }
  
  @CliAvailabilityIndicator({ CliStrings.STATUS_SHARED_CONFIG})
  public final boolean isConnected() {
    if (!CliUtil.isGfshVM()) {
      return true;
    }
    return (getGfsh() != null && getGfsh().isConnectedAndReady());
  }
}
