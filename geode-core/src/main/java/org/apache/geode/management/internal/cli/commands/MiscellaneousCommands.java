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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;

import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.deadlock.DeadlockDetector;
import org.apache.geode.distributed.internal.deadlock.Dependency;
import org.apache.geode.distributed.internal.deadlock.DependencyGraph;
import org.apache.geode.distributed.internal.deadlock.GemFireDeadlockDetector;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogLevel;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.JVMMetrics;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.CliUtil.DeflaterInflaterData;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.domain.StackTracesPerMember;
import org.apache.geode.management.internal.cli.functions.ChangeLogLevelFunction;
import org.apache.geode.management.internal.cli.functions.GarbageCollectionFunction;
import org.apache.geode.management.internal.cli.functions.GetStackTracesFunction;
import org.apache.geode.management.internal.cli.functions.NetstatFunction;
import org.apache.geode.management.internal.cli.functions.NetstatFunction.NetstatFunctionArgument;
import org.apache.geode.management.internal.cli.functions.NetstatFunction.NetstatFunctionResult;
import org.apache.geode.management.internal.cli.functions.ShutDownFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.CompositeResultData.SectionResultData;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.result.ResultDataException;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * @since GemFire 7.0
 */
public class MiscellaneousCommands implements GfshCommand {

  public static final String NETSTAT_FILE_REQUIRED_EXTENSION = ".txt";
  public final static String DEFAULT_TIME_OUT = "10";
  private final static Logger logger = LogService.getLogger();

  private final GetStackTracesFunction getStackTracesFunction = new GetStackTracesFunction();

  public void shutdownNode(final long timeout, final Set<DistributedMember> includeMembers)
      throws TimeoutException, InterruptedException, ExecutionException {
    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      final Function shutDownFunction = new ShutDownFunction();

      logger.info("Gfsh executing shutdown on members " + includeMembers);

      Callable<String> shutdownNodes = new Callable<String>() {

        @Override
        public String call() {
          try {
            Execution execution = FunctionService.onMembers(includeMembers);
            execution.execute(shutDownFunction);
          } catch (FunctionException functionEx) {
            // Expected Exception as the function is shutting down the target members and the result
            // collector will get member departed exception
          }
          return "SUCCESS";
        }
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

  @CliCommand(value = CliStrings.SHUTDOWN, help = CliStrings.SHUTDOWN__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_LIFECYCLE},
      interceptor = "org.apache.geode.management.internal.cli.commands.MiscellaneousCommands$Interceptor")
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public Result shutdown(
      @CliOption(key = CliStrings.SHUTDOWN__TIMEOUT, unspecifiedDefaultValue = DEFAULT_TIME_OUT,
          help = CliStrings.SHUTDOWN__TIMEOUT__HELP) int userSpecifiedTimeout,
      @CliOption(key = CliStrings.INCLUDE_LOCATORS, unspecifiedDefaultValue = "false",
          help = CliStrings.INCLUDE_LOCATORS_HELP) boolean shutdownLocators) {
    try {

      if (userSpecifiedTimeout < Integer.parseInt(DEFAULT_TIME_OUT)) {
        return ResultBuilder.createInfoResult(CliStrings.SHUTDOWN__MSG__IMPROPER_TIMEOUT);
      }

      // convert to milliseconds
      long timeout = userSpecifiedTimeout * 1000;

      InternalCache cache = getCache();

      int numDataNodes = CliUtil.getAllNormalMembers(cache).size();

      Set<DistributedMember> locators = CliUtil.getAllMembers(cache);

      Set<DistributedMember> dataNodes = CliUtil.getAllNormalMembers(cache);

      locators.removeAll(dataNodes);

      if (!shutdownLocators && numDataNodes == 0) {
        return ResultBuilder.createInfoResult(CliStrings.SHUTDOWN__MSG__NO_DATA_NODE_FOUND);
      }

      String managerName = cache.getJmxManagerAdvisor().getDistributionManager().getId().getId();

      final DistributedMember manager = CliUtil.getDistributedMemberByNameOrId(managerName);

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
      ex.printStackTrace();
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

  /**
   * Interceptor used by gfsh to intercept execution of shutdownall.
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
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

  @CliCommand(value = CliStrings.GC, help = CliStrings.GC__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public Result gc(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.GC__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.GC__MEMBER__HELP) String memberId) {
    InternalCache cache = getCache();
    Result result;
    CompositeResultData gcResultTable = ResultBuilder.createCompositeResultData();
    TabularResultData resultTable = gcResultTable.addSection().addTable("Table1");
    String headerText = "GC Summary";
    resultTable.setHeader(headerText);
    Set<DistributedMember> dsMembers = new HashSet<>();
    if (memberId != null && memberId.length() > 0) {
      DistributedMember member = CliUtil.getDistributedMemberByNameOrId(memberId);
      if (member == null) {
        return ResultBuilder
            .createGemFireErrorResult(memberId + CliStrings.GC__MSG__MEMBER_NOT_FOUND);
      }
      dsMembers.add(member);
      result = executeAndBuildResult(resultTable, dsMembers);
    } else if (groups != null && groups.length > 0) {
      for (String group : groups) {
        dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(group));
      }
      result = executeAndBuildResult(resultTable, dsMembers);

    } else {
      // gc on entire cluster
      // exclude locators
      dsMembers = CliUtil.getAllNormalMembers(cache);
      result = executeAndBuildResult(resultTable, dsMembers);

    }
    return result;
  }

  Result executeAndBuildResult(TabularResultData resultTable, Set<DistributedMember> dsMembers) {
    try {
      List<?> resultList;
      Function garbageCollectionFunction = new GarbageCollectionFunction();
      resultList =
          (List<?>) CliUtil.executeFunction(garbageCollectionFunction, null, dsMembers).getResult();

      for (Object object : resultList) {
        if (object instanceof Exception) {
          LogWrapper.getInstance().fine("Exception in GC " + ((Throwable) object).getMessage(),
              ((Throwable) object));
          continue;
        } else if (object instanceof Throwable) {
          LogWrapper.getInstance().fine("Exception in GC " + ((Throwable) object).getMessage(),
              ((Throwable) object));
          continue;
        }

        if (object != null) {
          if (object instanceof String) {
            // unexpected exception string - cache may be closed or something
            return ResultBuilder.createUserErrorResult((String) object);
          } else {
            Map<String, String> resultMap = (Map<String, String>) object;
            toTabularResultData(resultTable, resultMap.get("MemberId"),
                resultMap.get("HeapSizeBeforeGC"), resultMap.get("HeapSizeAfterGC"),
                resultMap.get("TimeSpentInGC"));
          }
        } else {
          LogWrapper.getInstance().fine("ResultMap was null ");
        }
      }
    } catch (Exception e) {
      String stack = CliUtil.stackTraceAsString(e);
      LogWrapper.getInstance().info("GC exception is " + stack);
      return ResultBuilder.createGemFireErrorResult(e.getMessage() + ": " + stack);
    }
    return ResultBuilder.buildResult(resultTable);
  }

  protected void toTabularResultData(TabularResultData table, String memberId,
      String heapSizeBefore, String heapSizeAfter, String timeTaken) {
    table.accumulate(CliStrings.GC__MSG__MEMBER_NAME, memberId);
    table.accumulate(CliStrings.GC__MSG__HEAP_SIZE_BEFORE_GC, heapSizeBefore);
    table.accumulate(CliStrings.GC__MSG__HEAP_SIZE_AFTER_GC, heapSizeAfter);
    table.accumulate(CliStrings.GC__MSG__TOTAL_TIME_IN_GC, timeTaken);
  }

  @CliCommand(value = CliStrings.NETSTAT, help = CliStrings.NETSTAT__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  // TODO : Verify the auto-completion for multiple values.
  public Result netstat(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.NETSTAT__MEMBER__HELP) String[] members,
      @CliOption(key = CliStrings.GROUP, optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.NETSTAT__GROUP__HELP) String group,
      @CliOption(key = CliStrings.NETSTAT__FILE,
          help = CliStrings.NETSTAT__FILE__HELP) String saveAs,
      @CliOption(key = CliStrings.NETSTAT__WITHLSOF, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.NETSTAT__WITHLSOF__HELP) boolean withlsof) {
    Result result;

    Map<String, DistributedMember> hostMemberMap = new HashMap<>();
    Map<String, List<String>> hostMemberListMap = new HashMap<>();

    try {
      if (members != null && members.length > 0 && group != null) {
        throw new IllegalArgumentException(
            CliStrings.NETSTAT__MSG__ONLY_ONE_OF_MEMBER_OR_GROUP_SHOULD_BE_SPECIFIED);
      }
      StringBuilder resultInfo = new StringBuilder();

      // Execute for remote members whose id or name matches
      InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();

      if (members != null) {
        Set<String> notFoundMembers = new HashSet<>();
        for (String memberIdOrName : members) {
          Set<DistributedMember> membersToExecuteOn = CliUtil.getAllMembers(system);
          boolean memberFound = false;
          for (DistributedMember distributedMember : membersToExecuteOn) {
            String memberName = distributedMember.getName();
            String memberId = distributedMember.getId();
            if (memberName.equals(memberIdOrName) || memberId.equals(memberIdOrName)) {
              buildMaps(hostMemberMap, hostMemberListMap, memberIdOrName, distributedMember);

              memberFound = true;
              break;
            }
          }
          if (!memberFound) {
            notFoundMembers.add(memberIdOrName);
          }
        }
        // if there are not found members, it's probably unknown member or member has departed
        if (!notFoundMembers.isEmpty()) {
          throw new IllegalArgumentException(
              CliStrings.format(CliStrings.NETSTAT__MSG__COULD_NOT_FIND_MEMBERS_0,
                  new Object[] {CliUtil.collectionToString(notFoundMembers, -1)}));
        }
      } else {
        Set<DistributedMember> membersToExecuteOn;
        if (group != null) {
          membersToExecuteOn = system.getGroupMembers(group);
        } else {
          // consider all members
          membersToExecuteOn = CliUtil.getAllMembers(system);
        }

        for (DistributedMember distributedMember : membersToExecuteOn) {
          String memberName = distributedMember.getName();
          String memberId = distributedMember.getId();
          String memberIdOrName =
              memberName != null && !memberName.isEmpty() ? memberName : memberId;

          buildMaps(hostMemberMap, hostMemberListMap, memberIdOrName, distributedMember);
        }
      }

      String lineSeparatorToUse;
      lineSeparatorToUse = CommandExecutionContext.getShellLineSeparator();
      if (lineSeparatorToUse == null) {
        lineSeparatorToUse = GfshParser.LINE_SEPARATOR;
      }
      NetstatFunctionArgument nfa = new NetstatFunctionArgument(lineSeparatorToUse, withlsof);

      if (!hostMemberMap.isEmpty()) {
        Set<DistributedMember> membersToExecuteOn = new HashSet<>(hostMemberMap.values());
        ResultCollector<?, ?> netstatResult =
            CliUtil.executeFunction(NetstatFunction.INSTANCE, nfa, membersToExecuteOn);
        List<?> resultList = (List<?>) netstatResult.getResult();
        for (Object aResultList : resultList) {
          NetstatFunctionResult netstatFunctionResult = (NetstatFunctionResult) aResultList;
          DeflaterInflaterData deflaterInflaterData = netstatFunctionResult.getCompressedBytes();
          try {
            String remoteHost = netstatFunctionResult.getHost();
            List<String> membersList = hostMemberListMap.get(remoteHost);
            resultInfo.append(MessageFormat.format(netstatFunctionResult.getHeaderInfo(),
                CliUtil.collectionToString(membersList, 120)));
            DeflaterInflaterData uncompressedBytes = CliUtil.uncompressBytes(
                deflaterInflaterData.getData(), deflaterInflaterData.getDataLength());
            resultInfo.append(new String(uncompressedBytes.getData()));
          } catch (DataFormatException e) {
            resultInfo.append("Error in some data. Reason : " + e.getMessage());
          }
        }
      }

      InfoResultData resultData = ResultBuilder.createInfoResultData();
      if (saveAs != null && !saveAs.isEmpty()) {
        String saveToFile = saveAs;
        if (!saveAs.endsWith(NETSTAT_FILE_REQUIRED_EXTENSION)) {
          saveToFile = saveAs + NETSTAT_FILE_REQUIRED_EXTENSION;
        }
        resultData.addAsFile(saveToFile, resultInfo.toString(),
            CliStrings.NETSTAT__MSG__SAVED_OUTPUT_IN_0, false); // Note: substitution for {0} will
        // happen on client side.
      } else {
        resultData.addLine(resultInfo.toString());
      }
      result = ResultBuilder.buildResult(resultData);
    } catch (IllegalArgumentException e) {
      LogWrapper.getInstance()
          .info(CliStrings.format(
              CliStrings.NETSTAT__MSG__ERROR_OCCURRED_WHILE_EXECUTING_NETSTAT_ON_0,
              new Object[] {Arrays.toString(members)}));
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (RuntimeException e) {
      LogWrapper.getInstance()
          .info(CliStrings.format(
              CliStrings.NETSTAT__MSG__ERROR_OCCURRED_WHILE_EXECUTING_NETSTAT_ON_0,
              new Object[] {Arrays.toString(members)}), e);
      result = ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.NETSTAT__MSG__ERROR_OCCURRED_WHILE_EXECUTING_NETSTAT_ON_0,
              new Object[] {Arrays.toString(members)}));
    } finally {
      hostMemberMap.clear();
      hostMemberListMap.clear();
    }
    return result;
  }

  private void buildMaps(Map<String, DistributedMember> hostMemberMap,
      Map<String, List<String>> hostMemberListMap, String memberIdOrName,
      DistributedMember distributedMember) {
    String host = distributedMember.getHost();

    // Maintain one member for a host - function execution purpose - once only for a host
    if (!hostMemberMap.containsKey(host)) {
      hostMemberMap.put(host, distributedMember);
    }

    // Maintain all members for a host - display purpose
    List<String> list;
    if (!hostMemberListMap.containsKey(host)) {
      list = new ArrayList<>();
      hostMemberListMap.put(host, list);
    } else {
      list = hostMemberListMap.get(host);
    }
    list.add(memberIdOrName);
  }

  @CliCommand(value = CliStrings.SHOW_DEADLOCK, help = CliStrings.SHOW_DEADLOCK__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result showDeadlock(@CliOption(key = CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE,
      help = CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE__HELP,
      mandatory = true) String filename) {

    Result result;
    try {
      if (!filename.endsWith(".txt")) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, ".txt"));
      }
      InternalCache cache = getCache();

      Set<DistributedMember> allMembers = CliUtil.getAllMembers(cache);
      GemFireDeadlockDetector gfeDeadLockDetector = new GemFireDeadlockDetector(allMembers);
      DependencyGraph dependencyGraph = gfeDeadLockDetector.find();
      Collection<Dependency> deadlock = dependencyGraph.findCycle();
      DependencyGraph deepest = null;
      if (deadlock == null) {
        deepest = dependencyGraph.findLongestCallChain();
        if (deepest != null) {
          deadlock = deepest.getEdges();
        }
      }
      Set<Dependency> dependencies = (Set<Dependency>) dependencyGraph.getEdges();

      InfoResultData resultData = ResultBuilder.createInfoResultData();

      if (deadlock != null) {
        if (deepest != null) {
          resultData.addLine(CliStrings.SHOW_DEADLOCK__DEEPEST_FOUND);
        } else {
          resultData.addLine(CliStrings.SHOW_DEADLOCK__DEADLOCK__DETECTED);
        }
        resultData.addLine(DeadlockDetector.prettyFormat(deadlock));
      } else {
        resultData.addLine(CliStrings.SHOW_DEADLOCK__NO__DEADLOCK);
      }
      resultData.addAsFile(filename, DeadlockDetector.prettyFormat(dependencies),
          MessageFormat.format(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__REVIEW, filename), false);
      result = ResultBuilder.buildResult(resultData);

    } catch (Exception e) {
      result = ResultBuilder
          .createGemFireErrorResult(CliStrings.SHOW_DEADLOCK__ERROR + " : " + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = CliStrings.SHOW_LOG, help = CliStrings.SHOW_LOG_HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result showLog(
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.SHOW_LOG_MEMBER_HELP, mandatory = true) String memberNameOrId,
      @CliOption(key = CliStrings.SHOW_LOG_LINE_NUM, unspecifiedDefaultValue = "0",
          help = CliStrings.SHOW_LOG_LINE_NUM_HELP) int numberOfLines) {
    Result result;
    try {
      InternalCache cache = getCache();
      SystemManagementService service =
          (SystemManagementService) ManagementService.getExistingManagementService(cache);
      MemberMXBean bean;
      DistributedMember memberToBeInvoked = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);
      if (memberToBeInvoked != null) {
        String memberId = memberToBeInvoked.getId();

        if (cache.getDistributedSystem().getDistributedMember().getId().equals(memberId)) {
          bean = service.getMemberMXBean();
        } else {
          ObjectName objectName = service.getMemberMBeanName(memberToBeInvoked);
          bean = service.getMBeanProxy(objectName, MemberMXBean.class);
        }

        if (numberOfLines > ManagementConstants.MAX_SHOW_LOG_LINES) {
          numberOfLines = ManagementConstants.MAX_SHOW_LOG_LINES;
        }
        if (numberOfLines == 0 || numberOfLines < 0) {
          numberOfLines = ManagementConstants.DEFAULT_SHOW_LOG_LINES;
        }
        InfoResultData resultData = ResultBuilder.createInfoResultData();
        if (bean != null) {
          String log = bean.showLog(numberOfLines);
          if (log != null) {
            resultData.addLine(log);
          } else {
            resultData.addLine(CliStrings.SHOW_LOG_NO_LOG);
          }
        } else {
          ErrorResultData errorResultData =
              ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                  .addLine(memberNameOrId + CliStrings.SHOW_LOG_MSG_MEMBER_NOT_FOUND);
          return (ResultBuilder.buildResult(errorResultData));
        }

        result = ResultBuilder.buildResult(resultData);
      } else {
        ErrorResultData errorResultData =
            ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                .addLine(memberNameOrId + CliStrings.SHOW_LOG_MSG_MEMBER_NOT_FOUND);
        return (ResultBuilder.buildResult(errorResultData));
      }

    } catch (Exception e) {
      result = ResultBuilder
          .createGemFireErrorResult(CliStrings.SHOW_LOG_ERROR + CliUtil.stackTraceAsString(e));
    }
    return result;
  }

  /**
   * Current implementation supports writing it to a file and returning the location of the file
   */
  @CliCommand(value = CliStrings.EXPORT_STACKTRACE, help = CliStrings.EXPORT_STACKTRACE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result exportStackTrace(@CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
      optionContext = ConverterHint.ALL_MEMBER_IDNAME,
      help = CliStrings.EXPORT_STACKTRACE__HELP) String[] memberNameOrId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.ALL_MEMBER_IDNAME, help = CliStrings.GROUP) String[] group,

      @CliOption(key = CliStrings.EXPORT_STACKTRACE__FILE,
          help = CliStrings.EXPORT_STACKTRACE__FILE__HELP) String fileName,

      @CliOption(key = CliStrings.EXPORT_STACKTRACE__FAIL__IF__FILE__PRESENT,
          unspecifiedDefaultValue = "false",
          help = CliStrings.EXPORT_STACKTRACE__FAIL__IF__FILE__PRESENT__HELP) boolean failIfFilePresent) {

    Result result;
    StringBuffer filePrefix = new StringBuffer("stacktrace");

    if (fileName == null) {
      fileName = filePrefix.append("_").append(System.currentTimeMillis()).toString();
    }
    final File outFile = new File(fileName);
    try {
      if (outFile.exists() && failIfFilePresent) {
        return ResultBuilder.createShellClientErrorResult(CliStrings.format(
            CliStrings.EXPORT_STACKTRACE__ERROR__FILE__PRESENT, outFile.getCanonicalPath()));
      }


      InternalCache cache = getCache();
      InternalDistributedSystem ads = cache.getInternalDistributedSystem();

      InfoResultData resultData = ResultBuilder.createInfoResultData();

      Map<String, byte[]> dumps = new HashMap<>();
      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrId);
      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(getStackTracesFunction, null, targetMembers);
      ArrayList<Object> resultList = (ArrayList<Object>) rc.getResult();

      for (Object resultObj : resultList) {
        if (resultObj instanceof StackTracesPerMember) {
          StackTracesPerMember stackTracePerMember = (StackTracesPerMember) resultObj;
          dumps.put(stackTracePerMember.getMemberNameOrId(), stackTracePerMember.getStackTraces());
        }
      }

      String filePath = writeStacksToFile(dumps, fileName);
      resultData.addLine(CliStrings.format(CliStrings.EXPORT_STACKTRACE__SUCCESS, filePath));
      resultData.addLine(CliStrings.EXPORT_STACKTRACE__HOST + ads.getDistributedMember().getHost());

      result = ResultBuilder.buildResult(resultData);
    } catch (IOException ex) {
      result = ResultBuilder
          .createGemFireErrorResult(CliStrings.EXPORT_STACKTRACE__ERROR + ex.getMessage());
    }
    return result;
  }

  /**
   * Interceptor used by gfsh to intercept execution of shutdownall.
   */
  public static class ExportStackTraceInterceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {

      Map<String, String> paramValueMap = parseResult.getParamValueStrings();
      String fileName = paramValueMap.get(CliStrings.EXPORT_STACKTRACE__FILE);

      Response response = readYesNo(
          CliStrings.format(CliStrings.EXPORT_STACKTRACE_WARN_USER, fileName), Response.YES);
      if (response == Response.NO) {
        return ResultBuilder
            .createShellClientAbortOperationResult(CliStrings.EXPORT_STACKTRACE_MSG_ABORTING);
      } else {
        // we don't to show any info result
        return ResultBuilder.createInfoResult("");
      }
    }
  }

  /***
   * Writes the Stack traces member-wise to a text file
   *
   * @param dumps - Map containing key : member , value : zipped stack traces
   * @param fileName - Name of the file to which the stack-traces are written to
   * @return Canonical path of the file which contains the stack-traces
   * @throws IOException
   */
  private String writeStacksToFile(Map<String, byte[]> dumps, String fileName) throws IOException {
    String filePath;
    PrintWriter ps;
    File outputFile;

    outputFile = new File(fileName);
    try (OutputStream os = new FileOutputStream(outputFile)) {
      ps = new PrintWriter(os);

      for (Entry<String, byte[]> entry : dumps.entrySet()) {
        ps.append("*** Stack-trace for member " + entry.getKey() + " ***");
        ps.flush();
        GZIPInputStream zipIn = new GZIPInputStream(new ByteArrayInputStream(entry.getValue()));
        BufferedInputStream bin = new BufferedInputStream(zipIn);
        byte[] buffer = new byte[10000];
        int count;
        while ((count = bin.read(buffer)) != -1) {
          os.write(buffer, 0, count);
        }
        ps.append('\n');
      }
      ps.flush();
      filePath = outputFile.getCanonicalPath();
    }

    return filePath;
  }

  @CliCommand(value = CliStrings.SHOW_METRICS, help = CliStrings.SHOW_METRICS__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_STATISTICS})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result showMetrics(
      @CliOption(key = {CliStrings.MEMBER}, optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.SHOW_METRICS__MEMBER__HELP) String memberNameOrId,
      @CliOption(key = {CliStrings.SHOW_METRICS__REGION}, optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.SHOW_METRICS__REGION__HELP) String regionName,
      @CliOption(key = {CliStrings.SHOW_METRICS__FILE},
          help = CliStrings.SHOW_METRICS__FILE__HELP) String export_to_report_to,
      @CliOption(key = {CliStrings.SHOW_METRICS__CACHESERVER__PORT},
          help = CliStrings.SHOW_METRICS__CACHESERVER__PORT__HELP) String cacheServerPortString,
      @CliOption(key = {CliStrings.SHOW_METRICS__CATEGORY},
          help = CliStrings.SHOW_METRICS__CATEGORY__HELP) String[] categories) {

    Result result;
    try {

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        if (!export_to_report_to.endsWith(".csv")) {
          return ResultBuilder
              .createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, ".csv"));
        }
      }
      if (regionName != null && !regionName.isEmpty()) {

        if (!org.apache.geode.internal.lang.StringUtils.isBlank(cacheServerPortString)) {
          return ResultBuilder
              .createUserErrorResult(CliStrings.SHOW_METRICS__CANNOT__USE__CACHESERVERPORT);
        }

        // MBean names contain the forward slash
        if (!regionName.startsWith("/")) {
          regionName = "/" + regionName;
        }

        if (memberNameOrId == null || memberNameOrId.isEmpty()) {
          result = ResultBuilder.buildResult(
              getDistributedRegionMetrics(regionName, export_to_report_to, categories));
        } else {
          DistributedMember member = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);

          if (member != null) {
            result = ResultBuilder.buildResult(
                getRegionMetricsFromMember(regionName, member, export_to_report_to, categories));
          } else {
            ErrorResultData erd = ResultBuilder.createErrorResultData();
            erd.addLine(
                CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberNameOrId));
            result = ResultBuilder.buildResult(erd);
          }
        }
      } else if (memberNameOrId != null && !memberNameOrId.isEmpty()) {

        DistributedMember member = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);

        if (member != null) {
          int cacheServerPort = -1;
          if (cacheServerPortString != null && !cacheServerPortString.isEmpty()) {
            try {
              cacheServerPort = Integer.parseInt(cacheServerPortString);
            } catch (NumberFormatException nfe) {
              return ResultBuilder.createUserErrorResult("Invalid port");
            }
          }
          result = ResultBuilder.buildResult(
              getMemberMetrics(member, export_to_report_to, categories, cacheServerPort));
        } else {
          ErrorResultData erd = ResultBuilder.createErrorResultData();
          erd.addLine(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberNameOrId));
          result = ResultBuilder.buildResult(erd);
        }
      } else {
        if (!org.apache.geode.internal.lang.StringUtils.isBlank(cacheServerPortString)) {
          return ResultBuilder
              .createUserErrorResult(CliStrings.SHOW_METRICS__CANNOT__USE__CACHESERVERPORT);
        }
        result = ResultBuilder.buildResult(getSystemWideMetrics(export_to_report_to, categories));
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      return ResultBuilder.createGemFireErrorResult(CliUtil.stackTraceAsString(e));
    }
    return result;
  }

  /**
   * Gets the system wide metrics
   *
   * @return ResultData with required System wide statistics or ErrorResultData if DS MBean is not
   *         found to gather metrics
   */
  private ResultData getSystemWideMetrics(String export_to_report_to, String[] categoriesArr) {
    final InternalCache cache = getCache();
    final ManagementService managementService = ManagementService.getManagementService(cache);
    DistributedSystemMXBean dsMxBean = managementService.getDistributedSystemMXBean();
    StringBuilder csvBuilder = null;
    if (dsMxBean != null) {

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        csvBuilder = new StringBuilder();
        csvBuilder.append("Category");
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__METRIC__HEADER);
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__VALUE__HEADER);
        csvBuilder.append('\n');
      }

      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      SectionResultData section = crd.addSection();
      TabularResultData metricsTable = section.addTable();
      Map<String, Boolean> categoriesMap = getSystemMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        // Checking if the categories specified by the user are valid or not

        if (userCategories.isEmpty()) {
          for (String category : checkSet) {
            categoriesMap.put(category, false);
          }
          for (String category : categories) {
            categoriesMap.put(category.toLowerCase(), true);
          }
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append("Invalid Categories\n");

          for (String category : userCategories) {
            sb.append(category);
            sb.append('\n');
          }
          return ResultBuilder.createErrorResultData().addLine(sb.toString());
        }
      }
      metricsTable.setHeader("Cluster-wide Metrics");

      if (categoriesMap.get("cluster")) {
        writeToTableAndCsv(metricsTable, "cluster", "totalHeapSize", dsMxBean.getTotalHeapSize(),
            csvBuilder);
      }

      if (categoriesMap.get("cache")) {
        writeToTableAndCsv(metricsTable, "cache", "totalRegionEntryCount",
            dsMxBean.getTotalRegionEntryCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalRegionCount", dsMxBean.getTotalRegionCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalMissCount", dsMxBean.getTotalMissCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalHitCount", dsMxBean.getTotalHitCount(),
            csvBuilder);
      }

      if (categoriesMap.get("diskstore")) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalDiskUsage",
            dsMxBean.getTotalDiskUsage(), csvBuilder); // deadcoded to workaround bug 46397
        writeToTableAndCsv(metricsTable, ""/* 46608 */, "diskReadsRate",
            dsMxBean.getDiskReadsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", dsMxBean.getDiskWritesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "flushTimeAvgLatency",
            dsMxBean.getDiskFlushAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBackupInProgress",
            dsMxBean.getTotalBackupInProgress(), csvBuilder);
      }

      if (categoriesMap.get("query")) {
        writeToTableAndCsv(metricsTable, "query", "activeCQCount", dsMxBean.getActiveCQCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "queryRequestRate", dsMxBean.getQueryRequestRate(),
            csvBuilder);
      }
      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder.toString(),
            "Cluster wide metrics exported to {0}.", false);
      }

      return crd;
    } else {
      String errorMessage =
          CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Distributed System MBean not found");
      return ResultBuilder.createErrorResultData().addLine(errorMessage);
    }
  }

  /**
   * Gets the Cluster wide metrics for a given member
   *
   * @return ResultData with required Member statistics or ErrorResultData if MemberMbean is not
   *         found to gather metrics
   * @throws ResultDataException if building result fails
   */
  private ResultData getMemberMetrics(DistributedMember distributedMember,
      String export_to_report_to, String[] categoriesArr, int cacheServerPort)
      throws ResultDataException {
    final InternalCache cache = getCache();
    final SystemManagementService managementService =
        (SystemManagementService) ManagementService.getManagementService(cache);

    ObjectName memberMBeanName = managementService.getMemberMBeanName(distributedMember);
    MemberMXBean memberMxBean =
        managementService.getMBeanInstance(memberMBeanName, MemberMXBean.class);
    ObjectName csMxBeanName;
    CacheServerMXBean csMxBean = null;

    if (memberMxBean != null) {

      if (cacheServerPort != -1) {
        csMxBeanName =
            managementService.getCacheServerMBeanName(cacheServerPort, distributedMember);
        csMxBean = managementService.getMBeanInstance(csMxBeanName, CacheServerMXBean.class);

        if (csMxBean == null) {
          ErrorResultData erd = ResultBuilder.createErrorResultData();
          erd.addLine(CliStrings.format(CliStrings.SHOW_METRICS__CACHE__SERVER__NOT__FOUND,
              cacheServerPort, MBeanJMXAdapter.getMemberNameOrId(distributedMember)));
          return erd;
        }
      }

      JVMMetrics jvmMetrics = memberMxBean.showJVMMetrics();

      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      SectionResultData section = crd.addSection();
      TabularResultData metricsTable = section.addTable();
      metricsTable.setHeader("Member Metrics");
      StringBuilder csvBuilder = null;

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        csvBuilder = new StringBuilder();
        csvBuilder.append("Category");
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__METRIC__HEADER);
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__VALUE__HEADER);
        csvBuilder.append('\n');
      }

      Map<String, Boolean> categoriesMap = getMemberMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        // Checking if the categories specified by the user are valid or not
        if (userCategories.isEmpty()) {
          for (String category : checkSet) {
            categoriesMap.put(category, false);
          }
          for (String category : categories) {
            categoriesMap.put(category.toLowerCase(), true);
          }
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append("Invalid Categories\n");

          for (String category : userCategories) {
            sb.append(category);
            sb.append('\n');
          }
          return ResultBuilder.createErrorResultData().addLine(sb.toString());
        }
      }

      /*
       * Member Metrics
       */
      // member, jvm, region, serialization, communication, function, transaction, diskstore, lock,
      // eviction, distribution
      if (categoriesMap.get("member")) {
        writeToTableAndCsv(metricsTable, "member", "upTime", memberMxBean.getMemberUpTime(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cpuUsage", memberMxBean.getCpuUsage(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "currentHeapSize", memberMxBean.getCurrentHeapSize(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "maximumHeapSize", memberMxBean.getMaximumHeapSize(),
            csvBuilder);
      }
      /*
       * JVM Metrics
       */
      if (categoriesMap.get("jvm")) {
        writeToTableAndCsv(metricsTable, "jvm ", "jvmThreads ", jvmMetrics.getTotalThreads(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "fileDescriptorLimit",
            memberMxBean.getFileDescriptorLimit(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalFileDescriptorOpen",
            memberMxBean.getTotalFileDescriptorOpen(), csvBuilder);
      }
      /*
       * Member wide region metrics
       */
      if (categoriesMap.get("region")) {
        writeToTableAndCsv(metricsTable, "region ", "totalRegionCount ",
            memberMxBean.getTotalRegionCount(), csvBuilder);
        String[] regionNames = memberMxBean.listRegions();
        if (regionNames != null) {
          for (int i = 0; i < regionNames.length; i++) {
            if (i == 0) {
              writeToTableAndCsv(metricsTable, "", "listOfRegions", regionNames[i].substring(1),
                  csvBuilder);
            } else {
              writeToTableAndCsv(metricsTable, "", "", regionNames[i].substring(1), csvBuilder);
            }
          }
        }

        String[] rootRegionNames = memberMxBean.getRootRegionNames();
        if (rootRegionNames != null) {
          for (int i = 0; i < rootRegionNames.length; i++) {
            if (i == 0) {
              writeToTableAndCsv(metricsTable, "", "rootRegions", rootRegionNames[i], csvBuilder);
            } else {
              writeToTableAndCsv(metricsTable, "", "", rootRegionNames[i], csvBuilder);
            }
          }
        }
        writeToTableAndCsv(metricsTable, "", "totalRegionEntryCount",
            memberMxBean.getTotalRegionEntryCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBucketCount", memberMxBean.getTotalBucketCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalPrimaryBucketCount",
            memberMxBean.getTotalPrimaryBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsAvgLatency", memberMxBean.getGetsAvgLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsAvgLatency", memberMxBean.getPutsAvgLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "createsRate", memberMxBean.getCreatesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "destroyRate", memberMxBean.getDestroysRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putAllAvgLatency", memberMxBean.getPutAllAvgLatency(),
            csvBuilder);
        // Not available from stats. After Stats re-org it will be available
        // writeToTableAndCsv(metricsTable, "", "getAllAvgLatency",
        // memberMxBean.getGetAllAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalMissCount", memberMxBean.getTotalMissCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalHitCount", memberMxBean.getTotalHitCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsRate", memberMxBean.getGetsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsRate", memberMxBean.getPutsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheWriterCallsAvgLatency",
            memberMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency",
            memberMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalLoadsCompleted",
            memberMxBean.getTotalLoadsCompleted(), csvBuilder);
      }

      /*
       * SERIALIZATION
       */
      if (categoriesMap.get("serialization")) {
        writeToTableAndCsv(metricsTable, "serialization", "serializationRate",
            memberMxBean.getSerializationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "serializationLatency",
            memberMxBean.getSerializationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "deserializationRate",
            memberMxBean.getDeserializationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "deserializationLatency",
            memberMxBean.getDeserializationLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "deserializationAvgLatency",
            memberMxBean.getDeserializationAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "PDXDeserializationAvgLatency",
            memberMxBean.getPDXDeserializationAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "PDXDeserializationRate",
            memberMxBean.getPDXDeserializationRate(), csvBuilder);
      }

      /*
       * Communication Metrics
       */
      if (categoriesMap.get("communication")) {
        writeToTableAndCsv(metricsTable, "communication", "bytesSentRate",
            memberMxBean.getBytesSentRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "bytesReceivedRate",
            memberMxBean.getBytesReceivedRate(), csvBuilder);
        String[] connectedGatewayReceivers = memberMxBean.listConnectedGatewayReceivers();
        writeToTableAndCsv(metricsTable, "", "connectedGatewayReceivers", connectedGatewayReceivers,
            csvBuilder);

        String[] connectedGatewaySenders = memberMxBean.listConnectedGatewaySenders();
        writeToTableAndCsv(metricsTable, "", "connectedGatewaySenders", connectedGatewaySenders,
            csvBuilder);
      }

      /*
       * Member wide function metrics
       */
      if (categoriesMap.get("function")) {
        writeToTableAndCsv(metricsTable, "function", "numRunningFunctions",
            memberMxBean.getNumRunningFunctions(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "functionExecutionRate",
            memberMxBean.getFunctionExecutionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "numRunningFunctionsHavingResults",
            memberMxBean.getNumRunningFunctionsHavingResults(), csvBuilder);
      }

      /*
       * totalTransactionsCount currentTransactionalThreadIds transactionCommitsAvgLatency
       * transactionCommittedTotalCount transactionRolledBackTotalCount transactionCommitsRate
       */
      if (categoriesMap.get("transaction")) {
        writeToTableAndCsv(metricsTable, "transaction", "totalTransactionsCount",
            memberMxBean.getTotalTransactionsCount(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "transactionCommitsAvgLatency",
            memberMxBean.getTransactionCommitsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "transactionCommittedTotalCount",
            memberMxBean.getTransactionCommittedTotalCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "transactionRolledBackTotalCount",
            memberMxBean.getTransactionRolledBackTotalCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "transactionCommitsRate",
            memberMxBean.getTransactionCommitsRate(), csvBuilder);
      }
      /*
       * Member wide disk metrics
       */
      if (categoriesMap.get("diskstore")) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalDiskUsage",
            memberMxBean.getTotalDiskUsage(), csvBuilder); // deadcoded to workaround bug 46397
        writeToTableAndCsv(metricsTable, ""/* 46608 */, "diskReadsRate",
            memberMxBean.getDiskReadsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", memberMxBean.getDiskWritesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "flushTimeAvgLatency",
            memberMxBean.getDiskFlushAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalQueueSize",
            memberMxBean.getTotalDiskTasksWaiting(), csvBuilder); // deadcoded to workaround bug
        // 46397
        writeToTableAndCsv(metricsTable, "", "totalBackupInProgress",
            memberMxBean.getTotalBackupInProgress(), csvBuilder);
      }
      /*
       * Member wide Lock
       */
      if (categoriesMap.get("lock")) {
        writeToTableAndCsv(metricsTable, "lock", "lockWaitsInProgress",
            memberMxBean.getLockWaitsInProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalLockWaitTime",
            memberMxBean.getTotalLockWaitTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalNumberOfLockService",
            memberMxBean.getTotalNumberOfLockService(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "requestQueues", memberMxBean.getLockRequestQueues(),
            csvBuilder);
      }
      /*
       * Eviction
       */
      if (categoriesMap.get("eviction")) {
        writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate",
            memberMxBean.getLruEvictionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lruDestroyRate", memberMxBean.getLruDestroyRate(),
            csvBuilder);
      }
      /*
       * Distribution
       */
      if (categoriesMap.get("distribution")) {
        writeToTableAndCsv(metricsTable, "distribution", "getInitialImagesInProgress",
            memberMxBean.getInitialImagesInProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getInitialImageTime",
            memberMxBean.getInitialImageTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getInitialImageKeysReceived",
            memberMxBean.getInitialImageKeysReceived(), csvBuilder);
      }

      /*
       * OffHeap
       */
      if (categoriesMap.get("offheap")) {
        writeToTableAndCsv(metricsTable, "offheap", "maxMemory", memberMxBean.getOffHeapMaxMemory(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "freeMemory", memberMxBean.getOffHeapFreeMemory(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "usedMemory", memberMxBean.getOffHeapUsedMemory(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "objects", memberMxBean.getOffHeapObjects(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "fragmentation",
            memberMxBean.getOffHeapFragmentation(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "compactionTime",
            memberMxBean.getOffHeapCompactionTime(), csvBuilder);
      }

      /*
       * CacheServer stats
       */
      if (csMxBean != null) {
        writeToTableAndCsv(metricsTable, "cache-server", "clientConnectionCount",
            csMxBean.getClientConnectionCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hostnameForClients", csMxBean.getHostNameForClients(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getRequestAvgLatency",
            csMxBean.getGetRequestAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRequestAvgLatency",
            csMxBean.getPutRequestAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalConnectionsTimedOut",
            csMxBean.getTotalConnectionsTimedOut(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "threadQueueSize", csMxBean.getPutRequestAvgLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "connectionThreads", csMxBean.getConnectionThreads(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "connectionLoad", csMxBean.getConnectionLoad(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "loadPerConnection", csMxBean.getLoadPerConnection(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "queueLoad", csMxBean.getQueueLoad(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "loadPerQueue", csMxBean.getLoadPerQueue(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getRequestRate", csMxBean.getGetRequestRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRequestRate", csMxBean.getPutRequestRate(),
            csvBuilder);

        /*
         * Notification
         */
        writeToTableAndCsv(metricsTable, "notification", "numClientNotificationRequests",
            csMxBean.getNumClientNotificationRequests(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "clientNotificationRate",
            csMxBean.getClientNotificationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "clientNotificationAvgLatency",
            csMxBean.getClientNotificationAvgLatency(), csvBuilder);

        /*
         * Query
         */
        writeToTableAndCsv(metricsTable, "query", "activeCQCount", csMxBean.getActiveCQCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "query", "queryRequestRate",
            csMxBean.getQueryRequestRate(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "indexCount", csMxBean.getIndexCount(), csvBuilder);

        String[] indexList = csMxBean.getIndexList();
        writeToTableAndCsv(metricsTable, "", "index list", indexList, csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalIndexMaintenanceTime",
            csMxBean.getTotalIndexMaintenanceTime(), csvBuilder);
      }

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder.toString(), "Member metrics exported to {0}.",
            false);
      }
      return crd;

    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Member MBean for "
          + MBeanJMXAdapter.getMemberNameOrId(distributedMember) + " not found");
      return ResultBuilder.createErrorResultData().addLine(errorMessage);
    }
  }

  /**
   * Gets the Cluster-wide metrics for a region
   *
   * @return ResultData containing the table
   * @throws ResultDataException if building result fails
   */
  private ResultData getDistributedRegionMetrics(String regionName, String export_to_report_to,
      String[] categoriesArr) throws ResultDataException {

    final InternalCache cache = getCache();
    final ManagementService managementService = ManagementService.getManagementService(cache);

    DistributedRegionMXBean regionMxBean = managementService.getDistributedRegionMXBean(regionName);

    if (regionMxBean != null) {
      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      SectionResultData section = crd.addSection();
      TabularResultData metricsTable = section.addTable();
      metricsTable.setHeader("Cluster-wide Region Metrics");
      StringBuilder csvBuilder = null;

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        csvBuilder = new StringBuilder();
        csvBuilder.append("Category");
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__METRIC__HEADER);
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__VALUE__HEADER);
        csvBuilder.append('\n');
      }

      Map<String, Boolean> categoriesMap = getSystemRegionMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        // Checking if the categories specified by the user are valid or not
        if (userCategories.isEmpty()) {
          for (String category : checkSet) {
            categoriesMap.put(category, false);
          }
          for (String category : categories) {
            categoriesMap.put(category.toLowerCase(), true);
          }
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append("Invalid Categories\n");

          for (String category : userCategories) {
            sb.append(category);
            sb.append('\n');
          }
          return ResultBuilder.createErrorResultData().addLine(sb.toString());
        }
      }
      /*
       * General System metrics
       */
      // cluster, region, partition , diskstore, callback, eviction
      if (categoriesMap.get("cluster")) {
        writeToTableAndCsv(metricsTable, "cluster", "member count", regionMxBean.getMemberCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "region entry count",
            regionMxBean.getSystemRegionEntryCount(), csvBuilder);
      }

      if (categoriesMap.get("region")) {
        writeToTableAndCsv(metricsTable, "region", "lastModifiedTime",
            regionMxBean.getLastModifiedTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lastAccessedTime", regionMxBean.getLastAccessedTime(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "missCount", regionMxBean.getMissCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitCount", regionMxBean.getHitCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitRatio", regionMxBean.getHitRatio(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsRate", regionMxBean.getGetsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsRate", regionMxBean.getPutsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "createsRate", regionMxBean.getCreatesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "destroyRate", regionMxBean.getDestroyRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putAllRate", regionMxBean.getPutAllRate(),
            csvBuilder);
      }

      if (categoriesMap.get("partition")) {
        writeToTableAndCsv(metricsTable, "partition", "putLocalRate",
            regionMxBean.getPutLocalRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteRate", regionMxBean.getPutRemoteRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteLatency", regionMxBean.getPutRemoteLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteAvgLatency",
            regionMxBean.getPutRemoteAvgLatency(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "bucketCount", regionMxBean.getBucketCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "primaryBucketCount",
            regionMxBean.getPrimaryBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "numBucketsWithoutRedundancy",
            regionMxBean.getNumBucketsWithoutRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBucketSize", regionMxBean.getTotalBucketSize(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "averageBucketSize", regionMxBean.getAvgBucketSize(),
            csvBuilder);
      }
      /*
       * Disk store
       */
      if (categoriesMap.get("diskstore")) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalEntriesOnlyOnDisk",
            regionMxBean.getTotalEntriesOnlyOnDisk(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskReadsRate", regionMxBean.getDiskReadsRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", regionMxBean.getDiskWritesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalDiskWriteInProgress",
            regionMxBean.getTotalDiskWritesProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskTaskWaiting", regionMxBean.getDiskTaskWaiting(),
            csvBuilder);

      }
      /*
       * LISTENER
       */
      if (categoriesMap.get("callback")) {
        writeToTableAndCsv(metricsTable, "callback", "cacheWriterCallsAvgLatency",
            regionMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency",
            regionMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
      }

      /*
       * Eviction
       */
      if (categoriesMap.get("eviction")) {
        writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate",
            regionMxBean.getLruEvictionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lruDestroyRate", regionMxBean.getLruDestroyRate(),
            csvBuilder);
      }

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder.toString(),
            "Aggregate Region Metrics exported to {0}.", false);
      }

      return crd;
    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR,
          "Distributed Region MBean for " + regionName + " not found");
      erd.addLine(errorMessage);
      return erd;
    }
  }

  /**
   * Gets the metrics of region on a given member
   *
   * @return ResultData with required Region statistics or ErrorResultData if Region MBean is not
   *         found to gather metrics
   * @throws ResultDataException if building result fails
   */
  private ResultData getRegionMetricsFromMember(String regionName,
      DistributedMember distributedMember, String export_to_report_to, String[] categoriesArr)
      throws ResultDataException {

    final InternalCache cache = getCache();
    final SystemManagementService managementService =
        (SystemManagementService) ManagementService.getManagementService(cache);

    ObjectName regionMBeanName =
        managementService.getRegionMBeanName(distributedMember, regionName);
    RegionMXBean regionMxBean =
        managementService.getMBeanInstance(regionMBeanName, RegionMXBean.class);

    if (regionMxBean != null) {
      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      SectionResultData section = crd.addSection();
      TabularResultData metricsTable = section.addTable();
      metricsTable.setHeader("Metrics for region:" + regionName + " On Member "
          + MBeanJMXAdapter.getMemberNameOrId(distributedMember));
      StringBuilder csvBuilder = null;

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        csvBuilder = new StringBuilder();
        csvBuilder.append("Category");
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__METRIC__HEADER);
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__VALUE__HEADER);
        csvBuilder.append('\n');
      }

      /*
       * Region Metrics
       */
      Map<String, Boolean> categoriesMap = getRegionMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        // Checking if the categories specified by the user are valid or not
        if (userCategories.isEmpty()) {
          for (String category : checkSet) {
            categoriesMap.put(category, false);
          }
          for (String category : categories) {
            categoriesMap.put(category.toLowerCase(), true);
          }
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append("Invalid Categories\n");

          for (String category : userCategories) {
            sb.append(category);
            sb.append('\n');
          }
          return ResultBuilder.createErrorResultData().addLine(sb.toString());
        }
      }

      if (categoriesMap.get("region")) {
        writeToTableAndCsv(metricsTable, "region", "lastModifiedTime",
            regionMxBean.getLastModifiedTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lastAccessedTime", regionMxBean.getLastAccessedTime(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "missCount", regionMxBean.getMissCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitCount", regionMxBean.getHitCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitRatio", regionMxBean.getHitRatio(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsRate", regionMxBean.getGetsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsRate", regionMxBean.getPutsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "createsRate", regionMxBean.getCreatesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "destroyRate", regionMxBean.getDestroyRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putAllRate", regionMxBean.getPutAllRate(),
            csvBuilder);
      }

      if (categoriesMap.get("partition")) {
        writeToTableAndCsv(metricsTable, "partition", "putLocalRate",
            regionMxBean.getPutLocalRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteRate", regionMxBean.getPutRemoteRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteLatency", regionMxBean.getPutRemoteLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteAvgLatency",
            regionMxBean.getPutRemoteAvgLatency(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "bucketCount", regionMxBean.getBucketCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "primaryBucketCount",
            regionMxBean.getPrimaryBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "configuredRedundancy",
            regionMxBean.getConfiguredRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "actualRedundancy", regionMxBean.getActualRedundancy(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "numBucketsWithoutRedundancy",
            regionMxBean.getNumBucketsWithoutRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBucketSize", regionMxBean.getTotalBucketSize(),
            csvBuilder);
      }
      /*
       * Disk store
       */
      if (categoriesMap.get("diskstore")) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalEntriesOnlyOnDisk",
            regionMxBean.getTotalEntriesOnlyOnDisk(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskReadsRate", "" + regionMxBean.getDiskReadsRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", regionMxBean.getDiskWritesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalDiskWriteInProgress",
            regionMxBean.getTotalDiskWritesProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskTaskWaiting", regionMxBean.getDiskTaskWaiting(),
            csvBuilder);
      }
      /*
       * LISTENER
       */
      if (categoriesMap.get("callback")) {
        writeToTableAndCsv(metricsTable, "callback", "cacheWriterCallsAvgLatency",
            regionMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency",
            regionMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
      }

      /*
       * Eviction
       */
      if (categoriesMap.get("eviction")) {
        writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate",
            regionMxBean.getLruEvictionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lruDestroyRate", regionMxBean.getLruDestroyRate(),
            csvBuilder);
      }
      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder.toString(), "Region Metrics exported to {0}.",
            false);
      }

      return crd;
    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR,
          "Region MBean for " + regionName + " on member "
              + MBeanJMXAdapter.getMemberNameOrId(distributedMember) + " not found");
      erd.addLine(errorMessage);
      return erd;
    }
  }

  /***
   * Writes an entry to a TabularResultData and writes a comma separated entry to a string builder
   */
  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      long metricValue, StringBuilder csvBuilder) {
    metricsTable.accumulate(CliStrings.SHOW_METRICS__TYPE__HEADER, type);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__METRIC__HEADER, metricName);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__VALUE__HEADER, metricValue);

    if (csvBuilder != null) {
      csvBuilder.append(type);
      csvBuilder.append(',');
      csvBuilder.append(metricName);
      csvBuilder.append(',');
      csvBuilder.append(metricValue);
      csvBuilder.append('\n');
    }
  }

  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      double metricValue, StringBuilder csvBuilder) {
    metricsTable.accumulate(CliStrings.SHOW_METRICS__TYPE__HEADER, type);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__METRIC__HEADER, metricName);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__VALUE__HEADER, metricValue);

    if (csvBuilder != null) {
      csvBuilder.append(type);
      csvBuilder.append(',');
      csvBuilder.append(metricName);
      csvBuilder.append(',');
      csvBuilder.append(metricValue);
      csvBuilder.append('\n');
    }
  }

  private Set<String> createSet(String[] categories) {
    Set<String> categoriesSet = new HashSet<>();
    Collections.addAll(categoriesSet, categories);
    return categoriesSet;
  }

  /**
   * Defines and returns map of categories for System metrics.
   *
   * @return map with categories for system metrics and display flag set to true
   */
  private Map<String, Boolean> getSystemMetricsCategories() {
    Map<String, Boolean> categories = new HashMap<>();
    categories.put("cluster", true);
    categories.put("cache", true);
    categories.put("diskstore", true);
    categories.put("query", true);
    return categories;
  }

  /**
   * Defines and returns map of categories for Region Metrics
   *
   * @return map with categories for region metrics and display flag set to true
   */
  private Map<String, Boolean> getRegionMetricsCategories() {
    Map<String, Boolean> categories = new HashMap<>();

    categories.put("region", true);
    categories.put("partition", true);
    categories.put("diskstore", true);
    categories.put("callback", true);
    categories.put("gatewayreceiver", true);
    categories.put("distribution", true);
    categories.put("query", true);
    categories.put("eviction", true);
    return categories;
  }

  /**
   * Defines and returns map of categories for system-wide region metrics
   *
   * @return map with categories for system wide region metrics and display flag set to true
   */
  private Map<String, Boolean> getSystemRegionMetricsCategories() {
    Map<String, Boolean> categories = getRegionMetricsCategories();
    categories.put("cluster", true);
    return categories;
  }

  /**
   * Defines and returns map of categories for member metrics
   *
   * @return map with categories for member metrics and display flag set to true
   */
  private Map<String, Boolean> getMemberMetricsCategories() {
    Map<String, Boolean> categories = new HashMap<>();
    categories.put("member", true);
    categories.put("jvm", true);
    categories.put("region", true);
    categories.put("serialization", true);
    categories.put("communication", true);
    categories.put("function", true);
    categories.put("transaction", true);
    categories.put("diskstore", true);
    categories.put("lock", true);
    categories.put("eviction", true);
    categories.put("distribution", true);
    categories.put("offheap", true);
    return categories;
  }

  /**
   * Converts an array of strings to a String delimited by a new line character for display purposes
   *
   * @return a String delimited by a new line character for display purposes
   */
  private String formatNames(String[] names, int startIndex) {
    StringBuilder sb = new StringBuilder();

    if (names != null) {
      for (String name : names) {
        sb.append(name.substring(startIndex));
        sb.append('\n');
      }
    }
    return sb.toString();
  }

  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      String metricValue[], StringBuilder csvBuilder) {
    if (metricValue != null) {
      for (int i = 0; i < metricValue.length; i++) {
        if (i == 0) {
          writeToTableAndCsv(metricsTable, type, metricName, metricValue[i], csvBuilder);
        } else {
          writeToTableAndCsv(metricsTable, "", "", metricValue[i], csvBuilder);
        }
      }
    }
  }

  /**
   * Writes to a TabularResultData and also appends a CSV string to a String builder
   */
  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      String metricValue, StringBuilder csvBuilder) {
    metricsTable.accumulate(CliStrings.SHOW_METRICS__TYPE__HEADER, type);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__METRIC__HEADER, metricName);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__VALUE__HEADER, metricValue);

    if (csvBuilder != null) {
      csvBuilder.append(type);
      csvBuilder.append(',');
      csvBuilder.append(metricName);
      csvBuilder.append(',');
      csvBuilder.append(metricValue);
      csvBuilder.append('\n');
    }
  }

  @CliCommand(value = CliStrings.CHANGE_LOGLEVEL, help = CliStrings.CHANGE_LOGLEVEL__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_LOGS},
      interceptor = "org.apache.geode.management.internal.cli.commands.MiscellaneousCommands$ChangeLogLevelInterceptor")
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.WRITE)
  public Result changeLogLevel(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.CHANGE_LOGLEVEL__MEMBER__HELP) String[] memberIds,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS}, unspecifiedDefaultValue = "",
          help = CliStrings.CHANGE_LOGLEVEL__GROUPS__HELP) String[] grps,
      @CliOption(key = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL,
          optionContext = ConverterHint.LOG_LEVEL, mandatory = true, unspecifiedDefaultValue = "",
          help = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL__HELP) String logLevel) {
    try {
      if ((memberIds == null || memberIds.length == 0) && (grps == null || grps.length == 0)) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG__SPECIFY_GRP_OR_MEMBER);
      }

      InternalCache cache = GemFireCacheImpl.getInstance();
      LogWriter logger = cache.getLogger();

      Set<DistributedMember> dsMembers = new HashSet<>();
      Set<DistributedMember> ds = CliUtil.getAllMembers(cache);

      if (grps != null && grps.length > 0) {
        for (String grp : grps) {
          dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(grp));
        }
      }

      if (memberIds != null && memberIds.length > 0) {
        for (String member : memberIds) {
          for (DistributedMember mem : ds) {
            if (mem.getName() != null
                && (mem.getName().equals(member) || mem.getId().equals(member))) {
              dsMembers.add(mem);
              break;
            }
          }
        }
      }

      if (dsMembers.size() == 0) {
        return ResultBuilder.createGemFireErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG_NO_MEMBERS);
      }

      Function logFunction = new ChangeLogLevelFunction();
      FunctionService.registerFunction(logFunction);
      Object[] functionArgs = new Object[1];
      functionArgs[0] = logLevel;

      CompositeResultData compositeResultData = ResultBuilder.createCompositeResultData();
      SectionResultData section = compositeResultData.addSection("section");
      TabularResultData resultTable = section.addTable("ChangeLogLevel");
      resultTable = resultTable.setHeader("Summary");

      Execution execution = FunctionService.onMembers(dsMembers).setArguments(functionArgs);
      if (execution == null) {
        return ResultBuilder.createUserErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG__CANNOT_EXECUTE);
      }
      List<?> resultList = (List<?>) execution.execute(logFunction).getResult();

      for (Object object : resultList) {
        try {
          if (object instanceof Throwable) {
            logger.warning(
                "Exception in ChangeLogLevelFunction " + ((Throwable) object).getMessage(),
                ((Throwable) object));
            continue;
          }

          if (object != null) {
            Map<String, String> resultMap = (Map<String, String>) object;
            Entry<String, String> entry = resultMap.entrySet().iterator().next();

            if (entry.getValue().contains("ChangeLogLevelFunction exception")) {
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_MEMBER, entry.getKey());
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_STATUS, "false");
            } else {
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_MEMBER, entry.getKey());
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_STATUS, "true");
            }

          }
        } catch (Exception ex) {
          LogWrapper.getInstance().warning("change log level command exception " + ex);
        }
      }

      Result result = ResultBuilder.buildResult(compositeResultData);
      logger.info("change log-level command result=" + result);
      return result;
    } catch (Exception ex) {
      GemFireCacheImpl.getInstance().getLogger().error("GFSH Changeloglevel exception: " + ex);
      return ResultBuilder.createUserErrorResult(ex.getMessage());
    }
  }

  public static class ChangeLogLevelInterceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Map<String, String> arguments = parseResult.getParamValueStrings();
      // validate log level
      String logLevel = arguments.get("loglevel");
      if (StringUtils.isBlank(logLevel) || LogLevel.getLevel(logLevel) == null) {
        return ResultBuilder.createUserErrorResult("Invalid log level: " + logLevel);
      }

      return ResultBuilder.createInfoResult("");
    }
  }

  private Set<String> getSetDifference(Set<String> set1, Set<String> set2) {
    Set<String> setDifference = new HashSet<>();
    for (String element : set1) {
      if (!(set2.contains(element.toLowerCase()))) {
        setDifference.add(element);
      }
    }
    return setDifference;
  }
}
