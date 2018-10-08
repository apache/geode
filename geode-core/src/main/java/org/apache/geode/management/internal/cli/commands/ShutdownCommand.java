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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.ShutDownFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ShutdownCommand extends InternalGfshCommand {
  private static final String DEFAULT_TIME_OUT = "10";
  private static final Logger logger = LogService.getLogger();

  @CliCommand(value = CliStrings.SHUTDOWN, help = CliStrings.SHUTDOWN__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_LIFECYCLE},
      interceptor = "org.apache.geode.management.internal.cli.commands.ShutdownCommand$ShutdownCommandInterceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result shutdown(
      @CliOption(key = CliStrings.SHUTDOWN__TIMEOUT, unspecifiedDefaultValue = DEFAULT_TIME_OUT,
          help = CliStrings.SHUTDOWN__TIMEOUT__HELP) int userSpecifiedTimeout,
      @CliOption(key = CliStrings.INCLUDE_LOCATORS, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.INCLUDE_LOCATORS_HELP) boolean shutdownLocators) {
    try {
      if (userSpecifiedTimeout < Integer.parseInt(DEFAULT_TIME_OUT)) {
        return ResultBuilder.createInfoResult(CliStrings.SHUTDOWN__MSG__IMPROPER_TIMEOUT);
      }

      // convert to milliseconds
      long timeout = userSpecifiedTimeout * 1000;
      InternalCache cache = (InternalCache) getCache();
      int numDataNodes = getAllNormalMembers().size();
      Set<DistributedMember> locators = getAllMembers();
      Set<DistributedMember> dataNodes = getAllNormalMembers();
      locators.removeAll(dataNodes);

      if (!shutdownLocators && numDataNodes == 0) {
        return ResultBuilder.createInfoResult(CliStrings.SHUTDOWN__MSG__NO_DATA_NODE_FOUND);
      }

      String managerName = cache.getJmxManagerAdvisor().getDistributionManager().getId().getId();

      final DistributedMember manager = getMember(managerName);

      dataNodes.remove(manager);

      // shut down all data members excluding this manager if manager is a data node
      long timeElapsed = shutDownNodeWithTimeOut(timeout, dataNodes);
      timeout = timeout - timeElapsed;

      // shut down locators one by one
      if (shutdownLocators) {
        if (manager == null) {
          return ResultBuilder.createUserErrorResult(CliStrings.SHUTDOWN__MSG__MANAGER_NOT_FOUND);
        }

        // remove current locator as that would get shutdown last
        if (locators.contains(manager)) {
          locators.remove(manager);
        }

        for (DistributedMember locator : locators) {
          Set<DistributedMember> lsSet = new HashSet<>();
          lsSet.add(locator);
          long elapsedTime = shutDownNodeWithTimeOut(timeout, lsSet);
          timeout = timeout - elapsedTime;
        }
      }

      if (locators.contains(manager) && !shutdownLocators) { // This means manager is a locator and
        // shutdownLocators is false. Hence we
        // should not stop the manager
        return ResultBuilder.createInfoResult("Shutdown is triggered");
      }
      // now shut down this manager
      Set<DistributedMember> mgrSet = new HashSet<>();
      mgrSet.add(manager);
      // No need to check further timeout as this is the last node we will be
      // shutting down
      shutDownNodeWithTimeOut(timeout, mgrSet);

    } catch (TimeoutException tex) {
      return ResultBuilder.createInfoResult(CliStrings.SHUTDOWN_TIMEDOUT);
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      return ResultBuilder.createUserErrorResult(ex.getMessage());
    }
    // @TODO. List all the nodes which could be successfully shutdown
    return ResultBuilder.createInfoResult("Shutdown is triggered");
  }

  /**
   * @param timeout user specified timeout
   * @param nodesToBeStopped list of nodes to be stopped
   * @return Elapsed time to shutdown the given nodes;
   */
  private long shutDownNodeWithTimeOut(long timeout, Set<DistributedMember> nodesToBeStopped)
      throws TimeoutException, InterruptedException, ExecutionException {

    long shutDownTimeStart = System.currentTimeMillis();
    shutdownNode(timeout, nodesToBeStopped);

    long shutDownTimeEnd = System.currentTimeMillis();

    long timeElapsed = shutDownTimeEnd - shutDownTimeStart;

    if (timeElapsed > timeout || Boolean.getBoolean("ThrowTimeoutException")) {
      // The second check for ThrowTimeoutException is a test hook
      throw new TimeoutException();
    }
    return timeElapsed;
  }

  private void shutdownNode(final long timeout, final Set<DistributedMember> includeMembers)
      throws TimeoutException, InterruptedException, ExecutionException {
    ExecutorService exec = LoggingExecutors.newSingleThreadExecutor("ShutdownCommand", false);
    try {
      final Function shutDownFunction = new ShutDownFunction();
      logger.info("Gfsh executing shutdown on members " + includeMembers);
      Callable<String> shutdownNodes = () -> {
        try {
          Execution execution = FunctionService.onMembers(includeMembers);
          execution.execute(shutDownFunction);
        } catch (FunctionException functionEx) {
          // Expected Exception as the function is shutting down the target members and the result
          // collector will get member departed exception
        }
        return "SUCCESS";
      };
      Future<String> result = exec.submit(shutdownNodes);
      result.get(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      logger.error("TimeoutException in shutting down members." + includeMembers);
      throw te;
    } catch (InterruptedException e) {
      logger.error("InterruptedException in shutting down members." + includeMembers);
      throw e;
    } catch (ExecutionException e) {
      logger.error("ExecutionException in shutting down members." + includeMembers);
      throw e;
    } finally {
      exec.shutdownNow();
    }
  }

  public static class ShutdownCommandInterceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {

      // This hook is for testing purpose only.
      if (Boolean.getBoolean(CliStrings.IGNORE_INTERCEPTORS)) {
        return ResultBuilder.createInfoResult(CliStrings.SHUTDOWN__MSG__SHUTDOWN_ENTIRE_DS);
      }

      Response response = readYesNo(CliStrings.SHUTDOWN__MSG__WARN_USER, Response.YES);
      if (response == Response.NO) {
        return ResultBuilder
            .createShellClientAbortOperationResult(CliStrings.SHUTDOWN__MSG__ABORTING_SHUTDOWN);
      } else {
        return ResultBuilder.createInfoResult(CliStrings.SHUTDOWN__MSG__SHUTDOWN_ENTIRE_DS);
      }
    }
  }
}
