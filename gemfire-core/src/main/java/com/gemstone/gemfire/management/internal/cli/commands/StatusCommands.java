/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.commands;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;

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

/********
 * 
 * @author bansods
 *
 */

public class StatusCommands extends AbstractCommandsSupport implements CommandMarker {
  static final FetchSharedConfigurationStatusFunction fetchSharedConfigStatusFunction = new FetchSharedConfigurationStatusFunction(); 

  @SuppressWarnings("unchecked")
  @CliCommand (value = CliStrings.STATUS_SHARED_CONFIG, help = CliStrings.STATUS_SHARED_CONFIG_HELP)
  @CliMetaData (relatedTopic = CliStrings.TOPIC_GEMFIRE_LOCATOR)
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
