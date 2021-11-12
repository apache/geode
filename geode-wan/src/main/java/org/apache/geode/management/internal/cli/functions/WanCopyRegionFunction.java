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
package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__ALREADY__RUNNING__COMMAND;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__CANCELED__BEFORE__HAVING__COPIED;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__EXECUTIONS__CANCELED;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__EXECUTION__CANCELED;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__EXECUTION__FAILED;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__NO__RUNNING__COMMAND;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__REGION__NOT__FOUND;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__REGION__NOT__USING_SENDER;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__SENDER__NOT__FOUND;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__SENDER__NOT__RUNNING;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY;

import java.io.Serializable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.internal.WanCopyRegionFunctionService;
import org.apache.geode.cache.wan.internal.WanCopyRegionFunctionServiceAlreadyRunningException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 * Class for copying via WAN the contents of a region
 * It must be executed in all members of the Geode cluster that host the region
 * to be copied. (called with onServers() or withMembers() passing the list
 * of all members hosting the region).
 * It also offers the possibility to cancel an ongoing execution of this function.
 * The copying itself is executed in a new thread with a known name
 * (parameterized with the regionName and senderId) in order to allow
 * to cancel ongoing invocations by interrupting that thread.
 *
 * It accepts the following arguments in an array of objects
 * 0: regionName (String)
 * 1: senderId (String)
 * 2: isCancel (Boolean): If true, it indicates that an ongoing execution of this
 * function for the given region and senderId must be stopped. Otherwise,
 * it indicates that the region must be copied.
 * 3: maxRate (Long) maximum copy rate in entries per second. In the case of
 * parallel gateway senders, the maxRate is per server hosting the region.
 * 4: batchSize (Integer): the size of the batches. Region entries are copied in batches of the
 * passed size. After each batch is sent, the function checks if the command
 * must be canceled and also sleeps for some time if necessary to adjust the
 * copy rate to the one passed as argument.
 */
public class WanCopyRegionFunction extends CliFunction<Object[]> implements Declarable {
  private static final long serialVersionUID = 1L;

  public static final String ID = WanCopyRegionFunction.class.getName();

  private final WanCopyRegionFunctionServiceProvider serviceProvider;

  public WanCopyRegionFunction() {
    this(new WanCopyRegionFunctionServiceProviderImpl());
  }

  @VisibleForTesting
  WanCopyRegionFunction(WanCopyRegionFunctionServiceProvider serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    final Object[] args = context.getArguments();
    if (args.length < 5) {
      throw new IllegalStateException(
          "Arguments length does not match required length.");
    }
    String regionName = (String) args[0];
    final String senderId = (String) args[1];
    final boolean isCancel = (Boolean) args[2];
    long maxRate = (Long) args[3];
    int batchSize = (Integer) args[4];

    if (regionName.startsWith(SEPARATOR)) {
      regionName = regionName.substring(1);
    }
    if (regionName.equals("*") && senderId.equals("*") && isCancel) {
      return cancelAllWanCopyRegion(context);
    }

    if (isCancel) {
      return cancelWanCopyRegion(context, regionName, senderId);
    }
    final Cache cache = context.getCache();

    final Region<?, ?> region = cache.getRegion(regionName);
    if (region == null) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(WAN_COPY_REGION__MSG__REGION__NOT__FOUND, regionName));
    }
    GatewaySender sender = cache.getGatewaySender(senderId);
    if (sender == null) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(WAN_COPY_REGION__MSG__SENDER__NOT__FOUND, senderId));
    }
    if (!region.getAttributes().getGatewaySenderIds().contains(sender.getId())) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(WAN_COPY_REGION__MSG__REGION__NOT__USING_SENDER, regionName,
              senderId));
    }
    if (!sender.isRunning()) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(WAN_COPY_REGION__MSG__SENDER__NOT__RUNNING, senderId));
    }
    if (!sender.isParallel() && !((InternalGatewaySender) sender).isPrimary()) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
          CliStrings.format(WAN_COPY_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY,
              senderId));
    }

    return executeFunctionInService(context, region, sender, maxRate, batchSize);
  }

  private CliFunctionResult executeFunctionInService(FunctionContext<Object[]> context,
      Region<?, ?> region, GatewaySender sender, long maxRate, int batchSize) {
    try {
      return serviceProvider.get((InternalCache) context.getCache()).execute(
          () -> new WanCopyRegionFunctionDelegate().wanCopyRegion(
              (InternalCache) context.getCache(), context.getMemberName(), region, sender, maxRate,
              batchSize),
          region.getName(),
          sender.getId());
    } catch (InterruptedException | CancellationException e) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          WAN_COPY_REGION__MSG__CANCELED__BEFORE__HAVING__COPIED);
    } catch (ExecutionException e) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(WAN_COPY_REGION__MSG__EXECUTION__FAILED, e.getMessage()));
    } catch (WanCopyRegionFunctionServiceAlreadyRunningException e) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(WAN_COPY_REGION__MSG__ALREADY__RUNNING__COMMAND, region.getName(),
              sender.getId()));
    }
  }

  private CliFunctionResult cancelWanCopyRegion(FunctionContext<Object[]> context,
      String regionName, String senderId) {
    boolean canceled =
        serviceProvider.get((InternalCache) context.getCache()).cancel(regionName, senderId);
    if (!canceled) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(WAN_COPY_REGION__MSG__NO__RUNNING__COMMAND,
              regionName, senderId));
    }
    return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
        WAN_COPY_REGION__MSG__EXECUTION__CANCELED);
  }

  private CliFunctionResult cancelAllWanCopyRegion(FunctionContext<Object[]> context) {
    String executionsString = serviceProvider.get((InternalCache) context.getCache())
        .cancelAll();
    return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
        CliStrings.format(WAN_COPY_REGION__MSG__EXECUTIONS__CANCELED, executionsString));
  }

  @FunctionalInterface
  interface WanCopyRegionFunctionServiceProvider extends Serializable {
    WanCopyRegionFunctionService get(InternalCache cache);
  }

  static class WanCopyRegionFunctionServiceProviderImpl
      implements WanCopyRegionFunctionServiceProvider {
    @Override
    public WanCopyRegionFunctionService get(InternalCache cache) {
      return cache.getService(WanCopyRegionFunctionService.class);
    }
  }
}
