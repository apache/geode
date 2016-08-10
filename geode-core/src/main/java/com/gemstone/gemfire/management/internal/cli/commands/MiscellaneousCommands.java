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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Time;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.deadlock.DeadlockDetector;
import com.gemstone.gemfire.distributed.internal.deadlock.Dependency;
import com.gemstone.gemfire.distributed.internal.deadlock.DependencyGraph;
import com.gemstone.gemfire.distributed.internal.deadlock.GemFireDeadlockDetector;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.JVMMetrics;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.cli.AbstractCliAroundInterceptor;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.CliUtil.DeflaterInflaterData;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.domain.StackTracesPerMember;
import com.gemstone.gemfire.management.internal.cli.functions.ChangeLogLevelFunction;
import com.gemstone.gemfire.management.internal.cli.functions.GarbageCollectionFunction;
import com.gemstone.gemfire.management.internal.cli.functions.GetStackTracesFunction;
import com.gemstone.gemfire.management.internal.cli.functions.LogFileFunction;
import com.gemstone.gemfire.management.internal.cli.functions.NetstatFunction;
import com.gemstone.gemfire.management.internal.cli.functions.NetstatFunction.NetstatFunctionArgument;
import com.gemstone.gemfire.management.internal.cli.functions.NetstatFunction.NetstatFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.ShutDownFunction;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.remote.CommandExecutionContext;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData.SectionResultData;
import com.gemstone.gemfire.management.internal.cli.result.ErrorResultData;
import com.gemstone.gemfire.management.internal.cli.result.InfoResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.ResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultDataException;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.util.MergeLogs;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

/**
 *
 * @since GemFire 7.0
 */
public class MiscellaneousCommands implements CommandMarker {
  public static final String NETSTAT_FILE_REQUIRED_EXTENSION = ".txt";
  public final static String FORMAT = "yyyy/MM/dd/HH/mm/ss/SSS/z";
  public final static String ONLY_DATE_FORMAT = "yyyy/MM/dd";
  public final static String DEFAULT_TIME_OUT = "10";

  private final GetStackTracesFunction getStackTracesFunction = new GetStackTracesFunction();

  private Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  
  public void shutdownNode(final long timeout, final Set<DistributedMember> includeMembers) throws TimeoutException, InterruptedException, ExecutionException {
    Cache cache = CacheFactory.getAnyInstance();
    LogWriter logger = cache.getLogger();
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
            //Expected Exception as the function is shutting down the target members and the result collector will get member departed exception
          }
          return "SUCCESS";
        }

      };

      Future<String> result = exec.submit(shutdownNodes);
      result.get(timeout, TimeUnit.MILLISECONDS);

    } catch (TimeoutException te) {
      logger.error("TimeoutException in shutting down members."+includeMembers);
      throw te;
    } catch (InterruptedException e) {
      logger.error("InterruptedException in shutting down members."+includeMembers);
      throw e;
    } catch (ExecutionException e) {
      logger.error("ExecutionException in shutting down members."+includeMembers);
      throw e;
    } finally{
      exec.shutdownNow();
    }

  }


  @CliCommand(value = CliStrings.SHUTDOWN, help = CliStrings.SHUTDOWN__HELP)
  @CliMetaData(relatedTopic = { CliStrings.TOPIC_GEODE_LIFECYCLE },
      interceptor = "com.gemstone.gemfire.management.internal.cli.commands.MiscellaneousCommands$Interceptor")
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public Result shutdown(
      @CliOption(key = CliStrings.SHUTDOWN__TIMEOUT, unspecifiedDefaultValue = DEFAULT_TIME_OUT,
          help = CliStrings.SHUTDOWN__TIMEOUT__HELP) int userSpecifiedTimeout,
         @CliOption( key = CliStrings.INCLUDE_LOCATORS, unspecifiedDefaultValue="false", 
         help=CliStrings.INCLUDE_LOCATORS_HELP) boolean shutdownLocators) {
    try { 

      if(userSpecifiedTimeout < Integer.parseInt(DEFAULT_TIME_OUT)){
        return ResultBuilder.createInfoResult(CliStrings.SHUTDOWN__MSG__IMPROPER_TIMEOUT);
      }
      
      //convert to mili-seconds
      long timeout = userSpecifiedTimeout * 1000 ;
      
      Cache cache = CacheFactory.getAnyInstance();      

      int numDataNodes = CliUtil.getAllNormalMembers(cache).size();      

      Set<DistributedMember> locators = CliUtil.getAllMembers(cache);
     
      Set<DistributedMember> dataNodes = CliUtil.getAllNormalMembers(cache);
      
      locators.removeAll(dataNodes);    
      
      
      
      if(!shutdownLocators && numDataNodes == 0){
        return ResultBuilder.createInfoResult(CliStrings.SHUTDOWN__MSG__NO_DATA_NODE_FOUND);
      }
      
      GemFireCacheImpl gemFireCache = (GemFireCacheImpl) cache;
      String managerName = gemFireCache.getJmxManagerAdvisor().getDistributionManager().getId().getId();

      final DistributedMember manager = CliUtil.getDistributedMemberByNameOrId(managerName);
      
      dataNodes.remove(manager);
      
      //shut down all data members excluding this manager if manager is a data node
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
          Set<DistributedMember> lsSet = new HashSet<DistributedMember>();
          lsSet.add(locator);
          long elapsedTime = shutDownNodeWithTimeOut(timeout, lsSet);
          timeout = timeout - elapsedTime;
        }
      }

      if(locators.contains(manager) && !shutdownLocators){ // This means manager is a locator and shutdownLocators is false. Hence we should not stop the manager
        return ResultBuilder.createInfoResult("Shutdown is triggered");
      }
      // now shut down this manager
      Set<DistributedMember> mgrSet = new HashSet<DistributedMember>();
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

    //@TODO. List all the nodes which could be successfully shutdown
    return ResultBuilder.createInfoResult("Shutdown is triggered");

  }

  /**
   * 
   * @param timeout
   *          user specified timeout
   * @param nodesToBeStopped
   *          list of nodes to be stopped
   * @return Elapsed time to shutdown the given nodes;
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private long shutDownNodeWithTimeOut(long timeout, Set<DistributedMember> nodesToBeStopped) throws TimeoutException,
      InterruptedException, ExecutionException {
    
    long shutDownTimeStart = System.currentTimeMillis();
    shutdownNode(timeout, nodesToBeStopped);

    long shutDownTimeEnd = System.currentTimeMillis();

    long timeElapsed = shutDownTimeEnd - shutDownTimeStart;

    if (timeElapsed > timeout || Boolean.getBoolean("ThrowTimeoutException")) { //The second check for ThrowTimeoutException is a test hook
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
      if(Boolean.getBoolean(CliStrings.IGNORE_INTERCEPTORS)){
        return ResultBuilder
            .createInfoResult(CliStrings.SHUTDOWN__MSG__SHUTDOWN_ENTIRE_DS);
      }
      
      Response response = readYesNo(CliStrings.SHUTDOWN__MSG__WARN_USER,
          Response.YES);
      if (response == Response.NO) {
        return ResultBuilder
            .createShellClientAbortOperationResult(CliStrings.SHUTDOWN__MSG__ABORTING_SHUTDOWN);
      } else {
        return ResultBuilder
            .createInfoResult(CliStrings.SHUTDOWN__MSG__SHUTDOWN_ENTIRE_DS);
      }
    }
    @Override
    public Result postExecution(GfshParseResult parseResult,
        Result commandResult) {
      return commandResult;
    }
  }

  @CliCommand(value = CliStrings.GC, help = CliStrings.GC__HELP)
  @CliMetaData(relatedTopic = { CliStrings.TOPIC_GEODE_DEBUG_UTIL })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public Result gc(
      @CliOption(key = CliStrings.GC__GROUP, unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, help = CliStrings.GC__GROUP__HELP)
      String[] groups,
      @CliOption(key = CliStrings.GC__MEMBER, optionContext = ConverterHint.ALL_MEMBER_IDNAME, unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, help = CliStrings.GC__MEMBER__HELP)
      String memberId) {
    Cache cache = CacheFactory.getAnyInstance();
    Result result = null;
    CompositeResultData gcResultTable = ResultBuilder
        .createCompositeResultData();
    TabularResultData resultTable = gcResultTable.addSection().addTable(
        "Table1");
    String headerText = "GC Summary";
    resultTable.setHeader(headerText);
    Set<DistributedMember> dsMembers = new HashSet<DistributedMember>();
    if (memberId != null && memberId.length() > 0) {
      DistributedMember member = CliUtil
          .getDistributedMemberByNameOrId(memberId);
      if (member == null) {
        return ResultBuilder.createGemFireErrorResult(memberId
            + CliStrings.GC__MSG__MEMBER_NOT_FOUND);
      }
      dsMembers.add(member);
      result =  executeAndBuildResult(cache, resultTable, dsMembers);
    } else if (groups != null && groups.length > 0) {
      for (String group : groups) {
        dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(group));
      }
      result = executeAndBuildResult(cache, resultTable, dsMembers);

    } else {
      // gc on entire cluster
      //exclude locators
      dsMembers = CliUtil.getAllNormalMembers(cache);
      result = executeAndBuildResult(cache, resultTable, dsMembers);

    }
    return result;
  }

  Result executeAndBuildResult(Cache cache, TabularResultData resultTable,
      Set<DistributedMember> dsMembers) {
    try {
      List<?> resultList = null;
      Function garbageCollectionFunction = new GarbageCollectionFunction();
      resultList = (List<?>) CliUtil.executeFunction(garbageCollectionFunction, null, dsMembers).getResult();

      for (int i = 0; i < resultList.size(); i++) {
        Object object = resultList.get(i);
        if (object instanceof Exception) {
          LogWrapper.getInstance().fine("Exception in GC "+  ((Throwable) object).getMessage(),((Throwable) object));
         continue;
        } else if (object instanceof Throwable) {
          LogWrapper.getInstance().fine("Exception in GC "+  ((Throwable) object).getMessage(),((Throwable) object));
          continue;
        }

        if(object != null){
          if (object instanceof String) {
            // unexpected exception string - cache may be closed or something
            return ResultBuilder.createUserErrorResult((String)object);
          } else {
            Map<String, String> resultMap = (Map<String, String>) object;
            toTabularResultData(resultTable, (String) resultMap.get("MemberId"),
                (String) resultMap.get("HeapSizeBeforeGC"),
                (String) resultMap.get("HeapSizeAfterGC"),
                (String) resultMap.get("TimeSpentInGC"));
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
  @CliMetaData(relatedTopic = { CliStrings.TOPIC_GEODE_DEBUG_UTIL })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  //TODO : Verify the auto-completion for multiple values.
  public Result netstat(
      @CliOption(key = CliStrings.NETSTAT__MEMBER,
                mandatory = false,
                unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                optionContext = ConverterHint.ALL_MEMBER_IDNAME,
                help = CliStrings.NETSTAT__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",")
      String[] members,
      @CliOption(key = CliStrings.NETSTAT__GROUP,
                 mandatory = false,
                 unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                 optionContext = ConverterHint.MEMBERGROUP,
                 help = CliStrings.NETSTAT__GROUP__HELP)
      String group,
      @CliOption(key = CliStrings.NETSTAT__FILE,
                 optionContext = ConverterHint.FILE,
                 unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                 help = CliStrings.NETSTAT__FILE__HELP)
      String saveAs,
      @CliOption(key = CliStrings.NETSTAT__WITHLSOF,
                specifiedDefaultValue = "true",
                unspecifiedDefaultValue = "false",
                help = CliStrings.NETSTAT__WITHLSOF__HELP)
      boolean withlsof) {
    Result result = null;

    Map<String, DistributedMember> hostMemberMap     = new HashMap<String, DistributedMember>();
    Map<String, List<String>>      hostMemberListMap = new HashMap<String, List<String>>();

    try {
      if (members != null && members.length > 0 && group != null) {
        throw new IllegalArgumentException(CliStrings.NETSTAT__MSG__ONLY_ONE_OF_MEMBER_OR_GROUP_SHOULD_BE_SPECIFIED);
      }
      StringBuilder resultInfo = new StringBuilder();

      // Execute for remote members whose id or name matches
      InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();

      if (members != null) {
        Set<String> notFoundMembers = new HashSet<String>();
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
          throw new IllegalArgumentException(CliStrings.format(CliStrings.NETSTAT__MSG__COULD_NOT_FIND_MEMBERS_0, new Object[] { CliUtil.collectionToString(notFoundMembers, -1) }));
        }
      } else {
        Set<DistributedMember> membersToExecuteOn = null;
        if (group != null) {
          membersToExecuteOn = system.getGroupMembers(group);
        } else {
          // consider all members
          membersToExecuteOn = CliUtil.getAllMembers(system);
        }

        for (DistributedMember distributedMember : membersToExecuteOn) {
          String memberName = distributedMember.getName();
          String memberId   = distributedMember.getId();
          String memberIdOrName = memberName != null && !memberName.isEmpty() ? memberName : memberId;

          buildMaps(hostMemberMap, hostMemberListMap, memberIdOrName, distributedMember);
        }
      }

      String lineSeparatorToUse = null;
      lineSeparatorToUse = CommandExecutionContext.getShellLineSeparator();
      if (lineSeparatorToUse == null) {
        lineSeparatorToUse = GfshParser.LINE_SEPARATOR;
      }
      NetstatFunctionArgument nfa = new NetstatFunctionArgument(lineSeparatorToUse, withlsof);

      if (!hostMemberMap.isEmpty()) {
        Set<DistributedMember> membersToExecuteOn = new HashSet<DistributedMember>(hostMemberMap.values());
        ResultCollector<?, ?> netstatResult = CliUtil.executeFunction(NetstatFunction.INSTANCE, nfa, membersToExecuteOn);
        List<?> resultList = (List<?>) netstatResult.getResult();
        for (int i = 0; i < resultList.size(); i++) {
          NetstatFunctionResult netstatFunctionResult = (NetstatFunctionResult) resultList.get(i);
          DeflaterInflaterData deflaterInflaterData = netstatFunctionResult.getCompressedBytes();
          try {
            String remoteHost = netstatFunctionResult.getHost();
            List<String> membersList = hostMemberListMap.get(remoteHost);
            resultInfo.append(MessageFormat.format(netstatFunctionResult.getHeaderInfo(), CliUtil.collectionToString(membersList, 120)));
            DeflaterInflaterData uncompressedBytes = CliUtil.uncompressBytes(deflaterInflaterData.getData(), deflaterInflaterData.getDataLength());
            resultInfo.append(new String(uncompressedBytes.getData()));
          } catch (DataFormatException e) {
            resultInfo.append("Error in some data. Reason : "+e.getMessage());
          }
        }
      }

      InfoResultData resultData = ResultBuilder.createInfoResultData();
      if (saveAs != null && !saveAs.isEmpty()) {
        String saveToFile = saveAs;
        if (!saveAs.endsWith(NETSTAT_FILE_REQUIRED_EXTENSION)) {
          saveToFile = saveAs + NETSTAT_FILE_REQUIRED_EXTENSION;
        }
        resultData.addAsFile(saveToFile, resultInfo.toString(), CliStrings.NETSTAT__MSG__SAVED_OUTPUT_IN_0, false); // Note: substitution for {0} will happen on client side.
      } else {
        resultData.addLine(resultInfo.toString());
      }
      result = ResultBuilder.buildResult(resultData);
    } catch (IllegalArgumentException e) {
      LogWrapper.getInstance().info(CliStrings.format(CliStrings.NETSTAT__MSG__ERROR_OCCURRED_WHILE_EXECUTING_NETSTAT_ON_0, new Object[] {Arrays.toString(members)}));
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (RuntimeException e) {
      LogWrapper.getInstance().info(CliStrings.format(CliStrings.NETSTAT__MSG__ERROR_OCCURRED_WHILE_EXECUTING_NETSTAT_ON_0, new Object[] {Arrays.toString(members)}), e);
      result = ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.NETSTAT__MSG__ERROR_OCCURRED_WHILE_EXECUTING_NETSTAT_ON_0, new Object[] {Arrays.toString(members)}));
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
    List<String> list = null;
    if (!hostMemberListMap.containsKey(host)) {
      list = new ArrayList<String>();
      hostMemberListMap.put(host, list);
    } else {
      list = hostMemberListMap.get(host);
    }
    list.add(memberIdOrName);
  }

  @CliCommand(value = CliStrings.SHOW_DEADLOCK, help = CliStrings.SHOW_DEADLOCK__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEODE_DEBUG_UTIL })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result showDeadlock(
      @CliOption(key = CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE,
      help = CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE__HELP,
      mandatory = true) String filename) {

    Result result = null;
    try {
      if (!filename.endsWith(".txt")) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENTION, ".txt"));
      }
      Cache cache = CacheFactory.getAnyInstance();

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
          MessageFormat.format(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__REVIEW,filename), false);
      result = ResultBuilder.buildResult(resultData);

    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings.SHOW_DEADLOCK__ERROR + " : " + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = CliStrings.SHOW_LOG, help = CliStrings.SHOW_LOG_HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEODE_DEBUG_UTIL })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result showLog(
      @CliOption(key = CliStrings.SHOW_LOG_MEMBER, optionContext = ConverterHint.ALL_MEMBER_IDNAME, unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, help = CliStrings.SHOW_LOG_MEMBER_HELP, mandatory = true) String memberNameOrId,
      @CliOption(key = CliStrings.SHOW_LOG_LINE_NUM, unspecifiedDefaultValue = "0", help = CliStrings.SHOW_LOG_LINE_NUM_HELP, mandatory = false) int numberOfLines) {
    Result result = null;
    try {
      Cache cache = CacheFactory.getAnyInstance();
      SystemManagementService service = (SystemManagementService)ManagementService
          .getExistingManagementService(cache);
      MemberMXBean bean = null;
      DistributedMember memberToBeInvoked = CliUtil
          .getDistributedMemberByNameOrId(memberNameOrId);
      if (memberToBeInvoked != null) {
        String memberId = memberToBeInvoked.getId();

        if (cache.getDistributedSystem().getDistributedMember().getId().equals(
            memberId)) {
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
          ErrorResultData errorResultData = ResultBuilder
              .createErrorResultData().setErrorCode(
                  ResultBuilder.ERRORCODE_DEFAULT).addLine(
                  memberNameOrId + CliStrings.SHOW_LOG_MSG_MEMBER_NOT_FOUND);
          return (ResultBuilder.buildResult(errorResultData));

        }

        result = ResultBuilder.buildResult(resultData);
      } else {
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
            .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
                memberNameOrId + CliStrings.SHOW_LOG_MSG_MEMBER_NOT_FOUND);
        return (ResultBuilder.buildResult(errorResultData));

      }

    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings.SHOW_LOG_ERROR
          + CliUtil.stackTraceAsString(e));
    }
    return result;
  }

  Result exportLogsPreprocessing(String dirName, String[] groups,
      String memberId, String logLevel, boolean onlyLogLevel, boolean mergeLog,
      String start, String end, int numOfLogFilesForTesting) {
    Result result = null;
    try {
      LogWrapper.getInstance().fine("Exporting logs");
      Cache cache = CacheFactory.getAnyInstance();
      Set<DistributedMember> dsMembers = new HashSet<DistributedMember>();
      Time startTime = null, endTime = null;

      if (logLevel == null || logLevel.length() == 0) {
        // set default log level
        logLevel = LogWriterImpl.levelToString(InternalLogWriter.INFO_LEVEL);
      }
      if (start != null && end == null) {
        startTime = parseDate(start);
        endTime = new Time(System.currentTimeMillis());
      }

      if (end != null && start == null) {
        endTime = parseDate(end);
        startTime = new Time(0);
      }
      if (start != null && end != null) {
        startTime = parseDate(start);
        endTime = parseDate(end);
        if (endTime.getTime() - startTime.getTime() <= 0) {
          result = ResultBuilder
              .createUserErrorResult(CliStrings.EXPORT_LOGS__MSG__INVALID_TIMERANGE);
        }
      }
      if (end == null && start == null) {
        // set default time range as 1 day.
        endTime = new Time(System.currentTimeMillis());
        startTime = new Time(endTime.getTime() - 24 * 60 * 60 * 1000);
      }
      LogWrapper.getInstance().fine(
          "Exporting logs startTime=" + startTime.toGMTString() + " "
              + startTime.toLocaleString());
      LogWrapper.getInstance().fine(
          "Exporting logs endTime=" + endTime.toGMTString() + " "
              + endTime.toLocaleString());
      if (groups != null && memberId != null) {
        result = ResultBuilder
            .createUserErrorResult(CliStrings.EXPORT_LOGS__MSG__SPECIFY_ONE_OF_OPTION);
      } else if (groups != null && groups.length > 0) {
        for (String group : groups) {
          Set<DistributedMember> groupMembers = cache.getDistributedSystem()
              .getGroupMembers(group);
          if (groupMembers != null && groupMembers.size() > 0) {
            dsMembers.addAll(groupMembers);
          }
        }
        if (dsMembers.size() == 0) {
          result = ResultBuilder
              .createUserErrorResult(CliStrings.EXPORT_LOGS__MSG__NO_GROUPMEMBER_FOUND);
        }
        result = export(cache, dsMembers, dirName, logLevel,
            onlyLogLevel ? "true" : "false", mergeLog, startTime, endTime, numOfLogFilesForTesting);
      } else if (memberId != null) {
        DistributedMember member = CliUtil
            .getDistributedMemberByNameOrId(memberId);
        if (member == null) {
          result = ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.EXPORT_LOGS__MSG__INVALID_MEMBERID, memberId));
        }
        dsMembers.add(member);
        result = export(cache, dsMembers, dirName, logLevel,
            onlyLogLevel ? "true" : "false", mergeLog, startTime, endTime, numOfLogFilesForTesting);
      } else {
        // run in entire DS members and get all including locators
        dsMembers.addAll(CliUtil.getAllMembers(cache));
        result = export(cache, dsMembers, dirName, logLevel,
            onlyLogLevel ? "true" : "false", mergeLog, startTime, endTime, numOfLogFilesForTesting);
      }
    } catch (ParseException ex) {
      LogWrapper.getInstance().fine(ex.getMessage());
      result = ResultBuilder.createUserErrorResult(ex.getMessage());
    } catch (Exception ex) {
      LogWrapper.getInstance().fine(ex.getMessage());
      result = ResultBuilder.createUserErrorResult(ex.getMessage());
    }
    return result;
  }
  @CliCommand(value = CliStrings.EXPORT_LOGS, help = CliStrings.EXPORT_LOGS__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_DEBUG_UTIL })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result exportLogs(
      @CliOption(key = CliStrings.EXPORT_LOGS__DIR,
          help = CliStrings.EXPORT_LOGS__DIR__HELP, mandatory=true) String dirName,
      @CliOption(key = CliStrings.EXPORT_LOGS__GROUP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.EXPORT_LOGS__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.EXPORT_LOGS__MEMBER,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.EXPORT_LOGS__MEMBER__HELP) String memberId,
      @CliOption(key = CliStrings.EXPORT_LOGS__LOGLEVEL,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          optionContext = ConverterHint.LOG_LEVEL,
          help = CliStrings.EXPORT_LOGS__LOGLEVEL__HELP) String logLevel,
      @CliOption(key = CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL,
          unspecifiedDefaultValue = "false", help = CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL__HELP) boolean onlyLogLevel,
      @CliOption(key = CliStrings.EXPORT_LOGS__MERGELOG,
          unspecifiedDefaultValue = "false", help = CliStrings.EXPORT_LOGS__MERGELOG__HELP) boolean mergeLog,
      @CliOption(key = CliStrings.EXPORT_LOGS__STARTTIME,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, help = CliStrings.EXPORT_LOGS__STARTTIME__HELP) String start,
      @CliOption(key = CliStrings.EXPORT_LOGS__ENDTIME,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, help = CliStrings.EXPORT_LOGS__ENDTIME__HELP) String end) {
    Result result = null;
    try {
      result =  exportLogsPreprocessing(  dirName,   groups,   memberId, logLevel,  onlyLogLevel, mergeLog, start,  end, 0 );
    } catch (Exception ex) {
      LogWrapper.getInstance().fine(ex.getMessage());
      result= ResultBuilder.createUserErrorResult(ex.getMessage()) ;
    }
    LogWrapper.getInstance().fine("Exporting logs returning =" + result );
    return result;
  }

  Time parseDate(String dateString) throws ParseException{
    Time time = null;
    try{
      SimpleDateFormat df = new SimpleDateFormat(MiscellaneousCommands.FORMAT);
      time = new Time(df.parse(dateString).getTime());
    }catch(Exception e){
      SimpleDateFormat df = new SimpleDateFormat(MiscellaneousCommands.ONLY_DATE_FORMAT);
      time = new Time(df.parse(dateString).getTime());
    }
    return time;
  }

  Result export(Cache cache, Set<DistributedMember> dsMembers, String dirName,
      String logLevel, String onlyLogLevel, boolean mergeLog, Time startTime, Time endTime, int numOfLogFilesForTesting) {
    LogWrapper.getInstance().fine("Exporting logs in export membersize = " + dsMembers.size() + " dirname="+dirName + " logLevel="+logLevel
        +" onlyLogLevel="+onlyLogLevel + " mergeLog="+mergeLog +" startTime="+startTime.toGMTString() + "endTime="+endTime.toGMTString());
    Function function = new LogFileFunction();
    FunctionService.registerFunction(function);
    try {
      List<?> resultList = null;

      List<String> logsToMerge = new ArrayList<String>();

      Iterator<DistributedMember> it = dsMembers.iterator();
      Object[] args = new Object[6];
      args[0] = dirName;
      args[1] = logLevel;
      args[2] = onlyLogLevel;
      args[3] = startTime.getTime();
      args[4] = endTime.getTime();
      args[5] = numOfLogFilesForTesting;

      while (it.hasNext()) {
        boolean toContinueForRestOfmembers = false;
        DistributedMember member = it.next();
        LogWrapper.getInstance().fine("Exporting logs copy the logs for member="+member.getId());
        try{
        resultList = (ArrayList<?>) CliUtil.executeFunction(function, args,
            member).getResult();
        }catch(Exception ex){
          LogWrapper.getInstance().fine(CliStrings.format(CliStrings.EXPORT_LOGS__MSG__FAILED_TO_EXPORT_LOG_FILES_FOR_MEMBER_0,
              member.getId()),ex);
          //try for other members
          continue;
        }

        if (resultList != null && !resultList.isEmpty()) {
          for (int i = 0; i < resultList.size(); i++) {
            Object object = resultList.get(i);
            if (object instanceof Exception) {
              ResultBuilder
                  .createGemFireErrorResult(CliStrings
                      .format(
                          CliStrings.EXPORT_LOGS__MSG__FAILED_TO_EXPORT_LOG_FILES_FOR_MEMBER_0,
                          member.getId()));
              LogWrapper.getInstance().fine("Exporting logs for member="+member.getId() + " exception="+ ((Throwable) object).getMessage(),((Throwable) object));
              toContinueForRestOfmembers = true;
              break;
            } else if (object instanceof Throwable) {
              ResultBuilder
                  .createGemFireErrorResult(CliStrings
                      .format(
                          CliStrings.EXPORT_LOGS__MSG__FAILED_TO_EXPORT_LOG_FILES_FOR_MEMBER_0,
                          member.getId()));

              Throwable th = (Throwable) object;
              LogWrapper.getInstance().fine(CliUtil.stackTraceAsString((th)));
              LogWrapper.getInstance().fine("Exporting logs for member="+member.getId() + " exception="+ ((Throwable) object).getMessage(),((Throwable) object) );
              toContinueForRestOfmembers = true;
              break;
            }
          }
        }else{
          LogWrapper.getInstance().fine("Exporting logs for member="+member.getId() +" resultList is either null or empty") ;
          continue;
        }

        if(toContinueForRestOfmembers == true){
          LogWrapper.getInstance().fine("Exporting logs for member="+member.getId() + " toContinueForRestOfmembers="+ toContinueForRestOfmembers);
          //proceed for rest of the member
          continue;
        }

        String rstList = (String) resultList.get(0);
        LogWrapper.getInstance().fine("for member="+member.getId()+"Successfully exported to directory="+dirName+ " rstList="+rstList);
        if (rstList== null || rstList.length() == 0) {
          ResultBuilder.createGemFireErrorResult(CliStrings.format
              (CliStrings.EXPORT_LOGS__MSG__FAILED_TO_EXPORT_LOG_FILES_FOR_MEMBER_0,
              member.getId()));
          LogWrapper.getInstance().fine("for member="+member.getId()+"rstList is null");
          continue;
        } else if (rstList.contains("does not exist or cannot be created")) {
          LogWrapper.getInstance().fine("for member="+member.getId()+" does not exist or cannot be created");
          return ResultBuilder.createInfoResult(CliStrings.format(
              CliStrings.EXPORT_LOGS__MSG__TARGET_DIR_CANNOT_BE_CREATED,
              dirName));
        } else if (rstList
            .contains(LocalizedStrings.InternalDistributedSystem_THIS_CONNECTION_TO_A_DISTRIBUTED_SYSTEM_HAS_BEEN_DISCONNECTED
                .toLocalizedString())) {
          LogWrapper.getInstance().fine("for member="+member.getId()+LocalizedStrings.InternalDistributedSystem_THIS_CONNECTION_TO_A_DISTRIBUTED_SYSTEM_HAS_BEEN_DISCONNECTED
              .toLocalizedString());
          //proceed for rest of the members
          continue;
        }

        //maintain list of log files to merge only when merge option is true.
        if (mergeLog == true){
          StringTokenizer st = new StringTokenizer(rstList, ";");
          while (st.hasMoreTokens()) {
            logsToMerge.add(st.nextToken());
          }
        }
      }
      //merge log files
      if (mergeLog == true){
        LogWrapper.getInstance().fine("Successfully exported to directory="+dirName+ " and now merging logsToMerge="+logsToMerge.size());
        mergeLogs(logsToMerge);
        return ResultBuilder.createInfoResult("Successfully exported and merged in directory "+dirName);
      }
      LogWrapper.getInstance().fine("Successfully exported to directory without merging"+dirName);
      return ResultBuilder.createInfoResult("Successfully exported to directory "+dirName);
    } catch (Exception ex) {
      LogWrapper.getInstance().info(ex.getMessage(),ex);
      return ResultBuilder.createUserErrorResult(CliStrings.EXPORT_LOGS__MSG__FUNCTION_EXCEPTION + ((LogFileFunction)function).getId() ) ;
    }
  }

  Result mergeLogs(List<String> logsToMerge) {
    //create a new process for merging
    LogWrapper.getInstance().fine("Exporting logs merging logs" + logsToMerge.size());
    if (logsToMerge.size() > 1){
      List<String> commandList = new ArrayList<String>();
      commandList.add(System.getProperty("java.home") + File.separatorChar
          + "bin" + File.separatorChar + "java");
      commandList.add("-classpath");
      commandList.add(System.getProperty("java.class.path", "."));
      commandList.add(MergeLogs.class.getName());

      commandList.add(logsToMerge.get(0).substring(0,logsToMerge.get(0).lastIndexOf(File.separator) + 1));

      ProcessBuilder procBuilder = new ProcessBuilder(commandList);
      StringBuilder output = new StringBuilder();
      String errorString = new String();
      try {
        LogWrapper.getInstance().fine("Exporting logs now merging logs");
        Process mergeProcess = procBuilder.redirectErrorStream(true)
            .start();

        mergeProcess.waitFor();

        InputStream inputStream = mergeProcess.getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(
            inputStream));
        String line = null;

        while ((line = br.readLine()) != null) {
          output.append(line).append(GfshParser.LINE_SEPARATOR);
        }
        mergeProcess.destroy();
      } catch (Exception e) {
        LogWrapper.getInstance().fine(e.getMessage());
        return ResultBuilder.createUserErrorResult(CliStrings.EXPORT_LOGS__MSG__FUNCTION_EXCEPTION + "Could not merge" ) ;
      } finally {
        if (errorString != null) {
          output.append(errorString).append(GfshParser.LINE_SEPARATOR);
          LogWrapper.getInstance().fine("Exporting logs after merging logs "+output);
        }
      }
      if (output.toString().contains("Sucessfully merged logs")){
        LogWrapper.getInstance().fine("Exporting logs Sucessfully merged logs");
        return ResultBuilder.createInfoResult("Successfully merged");
      }else{
        LogWrapper.getInstance().fine("Could not merge");
        return ResultBuilder.createUserErrorResult(CliStrings.EXPORT_LOGS__MSG__FUNCTION_EXCEPTION + "Could not merge") ;
      }
    }
    return ResultBuilder.createInfoResult("Only one log file, nothing to merge");
  }

  /****
   * Current implementation supports writing it to a file and returning the location of the file
   *
   * @param memberNameOrId
   * @return Stack Trace
   */
  @CliCommand(value = CliStrings.EXPORT_STACKTRACE, help = CliStrings.EXPORT_STACKTRACE__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEODE_DEBUG_UTIL })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result exportStackTrace(
      @CliOption(key = CliStrings.EXPORT_STACKTRACE__MEMBER,
      optionContext = ConverterHint.ALL_MEMBER_IDNAME,
      help = CliStrings.EXPORT_STACKTRACE__HELP) String memberNameOrId,

      @CliOption(key = CliStrings.EXPORT_STACKTRACE__GROUP,
      optionContext = ConverterHint.ALL_MEMBER_IDNAME,
      help=CliStrings.EXPORT_STACKTRACE__GROUP) String group,

      @CliOption(key = CliStrings.EXPORT_STACKTRACE__FILE,
      mandatory = true,
      help = CliStrings.EXPORT_STACKTRACE__FILE__HELP) String fileName) {

    Result result = null;
    try {
      Cache cache = CacheFactory.getAnyInstance();
      GemFireCacheImpl gfeCacheImpl = (GemFireCacheImpl) cache;
      InternalDistributedSystem ads = gfeCacheImpl.getSystem();

      InfoResultData resultData = ResultBuilder.createInfoResultData();

      if (!fileName.endsWith(".txt")) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENTION, ".txt"));
      }

      Map<String, byte[]> dumps = new HashMap<String, byte[]>();
      Set<DistributedMember> targetMembers = null;

      if ((group == null || group.isEmpty()) && (memberNameOrId == null || memberNameOrId.isEmpty())) {
        targetMembers = CliUtil.getAllMembers(cache);
      } else {
        targetMembers = CliUtil.findAllMatchingMembers(group, memberNameOrId);
      }

      ResultCollector<?, ?> rc = CliUtil.executeFunction(getStackTracesFunction, null, targetMembers);
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
    } catch (CommandResultException crex) {
      return crex.getResult();
    }
    catch (Exception ex) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings.EXPORT_STACKTRACE__ERROR + ex.getMessage());
    }
    return result;
  }

  /***
   * Writes the Stack traces member-wise to a text file
   * @param dumps - Map containing key : member , value : zipped stack traces
   * @param fileName - Name of the file to which the stack-traces are written to
   * @return Canonical path of the file which contains the stack-traces
   * @throws IOException
   */
  private String writeStacksToFile(Map<String, byte[]> dumps, String fileName) throws IOException {

    String filePath = null;
    OutputStream os = null;
    PrintWriter ps = null;
    File outputFile = null;

    try {
      outputFile = new File(fileName);
      os = new FileOutputStream(outputFile);
      ps = new PrintWriter(os);

      for (Map.Entry<String, byte[]> entry: dumps.entrySet()) {
        ps.append("*** Stack-trace for member " + entry.getKey() +" ***");
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
    }  finally {
      os.close();
    }

    return filePath;
  }

  @CliCommand(value = CliStrings.SHOW_METRICS, help = CliStrings.SHOW_METRICS__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEODE_STATISTICS })
  @ResourceOperation(resource = Resource.CLUSTER, operation= Operation.READ)
  public Result showMetrics(
      @CliOption(key = { CliStrings.SHOW_METRICS__MEMBER }, optionContext = ConverterHint.ALL_MEMBER_IDNAME, help = CliStrings.SHOW_METRICS__MEMBER__HELP) String memberNameOrId,
      @CliOption(key = { CliStrings.SHOW_METRICS__REGION }, optionContext = ConverterHint.REGIONPATH, help = CliStrings.SHOW_METRICS__REGION__HELP) String regionName,
      @CliOption(key = { CliStrings.SHOW_METRICS__FILE}, help = CliStrings.SHOW_METRICS__FILE__HELP) String export_to_report_to,
      @CliOption(key = { CliStrings.SHOW_METRICS__CACHESERVER__PORT}, help = CliStrings.SHOW_METRICS__CACHESERVER__PORT__HELP) String cacheServerPortString,
      @CliOption(key = { CliStrings.SHOW_METRICS__CATEGORY} , help = CliStrings.SHOW_METRICS__CATEGORY__HELP) @CliMetaData (valueSeparator = ",") String[] categories ) {

    Result result = null;
    try {

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        if (!export_to_report_to.endsWith(".csv")) {
          return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENTION, ".csv"));
        }
      }
      if (regionName != null && !regionName.isEmpty()) {

        if (!com.gemstone.gemfire.internal.lang.StringUtils.isBlank(cacheServerPortString)) {
          return ResultBuilder.createUserErrorResult(CliStrings.SHOW_METRICS__CANNOT__USE__CACHESERVERPORT);
        }

        //MBean names contain the forward slash
        if (!regionName.startsWith("/")) {
          regionName = "/" + regionName;
        }

        if (memberNameOrId == null || memberNameOrId.isEmpty()) {
          result = ResultBuilder.buildResult(getDistributedRegionMetrics(regionName, export_to_report_to, categories));
        } else {
          DistributedMember member = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);

          if (member != null) {
            result = ResultBuilder.buildResult(getRegionMetricsFromMember(regionName, member, export_to_report_to, categories));
          } else {
            ErrorResultData erd = ResultBuilder.createErrorResultData();
            erd.addLine(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberNameOrId));
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
          result = ResultBuilder.buildResult(getMemberMetrics(member, export_to_report_to, categories, cacheServerPort));
        } else {
          ErrorResultData erd = ResultBuilder.createErrorResultData();
          erd.addLine(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberNameOrId));
          result = ResultBuilder.buildResult(erd);
        }
      } else {

        if (!com.gemstone.gemfire.internal.lang.StringUtils.isBlank(cacheServerPortString)) {
          return ResultBuilder.createUserErrorResult(CliStrings.SHOW_METRICS__CANNOT__USE__CACHESERVERPORT);
        }

        result = ResultBuilder.buildResult(getSystemWideMetrics(export_to_report_to, categories));
      }
    } catch (Exception e) {
      return ResultBuilder.createGemFireErrorResult("#SB" + CliUtil.stackTraceAsString(e));
    }
    return result;
  }

  /****
   * Gets the system wide metrics
   *
   * @param export_to_report_to
   * @return ResultData with required System wide statistics or ErrorResultData
   *         if DS MBean is not found to gather metrics
   * @throws Exception
   */
  private ResultData getSystemWideMetrics(String export_to_report_to, String[] categoriesArr) throws Exception {
    final Cache cache = CacheFactory.getAnyInstance();
    final ManagementService managmentService = ManagementService.getManagementService(cache);
    DistributedSystemMXBean dsMxBean = managmentService.getDistributedSystemMXBean();
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
      SectionResultData section =  crd.addSection();
      TabularResultData metricsTable = section.addTable();
      Map<String, Boolean> categoriesMap = getSystemMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<String>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        //Checking if the categories specified by the user are valid or not

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

      if (categoriesMap.get("cluster").booleanValue()) {
        writeToTableAndCsv(metricsTable, "cluster", "totalHeapSize", dsMxBean.getTotalHeapSize(), csvBuilder);
      }

      if (categoriesMap.get("cache").booleanValue()) {
        writeToTableAndCsv(metricsTable, "cache", "totalRegionEntryCount", dsMxBean.getTotalRegionEntryCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalRegionCount", dsMxBean.getTotalRegionCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalMissCount", dsMxBean.getTotalMissCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalHitCount", dsMxBean.getTotalHitCount(), csvBuilder);
      }

      if (categoriesMap.get("diskstore").booleanValue()) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalDiskUsage", dsMxBean.getTotalDiskUsage(), csvBuilder); // deadcoded to workaround bug 46397
        writeToTableAndCsv(metricsTable, ""/*46608*/, "diskReadsRate", dsMxBean.getDiskReadsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", dsMxBean.getDiskWritesRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "flushTimeAvgLatency", dsMxBean.getDiskFlushAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBackupInProgress", dsMxBean.getTotalBackupInProgress(), csvBuilder);
      }

      if (categoriesMap.get("query").booleanValue()) {
        writeToTableAndCsv(metricsTable, "query", "activeCQCount", dsMxBean.getActiveCQCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "queryRequestRate", dsMxBean.getQueryRequestRate(), csvBuilder);
      }
      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder.toString(), "Cluster wide metrics exported to {0}.", false);
      }

      return crd;
    } else {
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Distributed System MBean not found");
      return ResultBuilder.createErrorResultData().addLine(errorMessage);
    }

  }

  /***
   * Gets the Cluster wide metrics for a given member
   *
   * @param distributedMember
   * @param export_to_report_to
   * @return ResultData with required Member statistics or ErrorResultData if
   *         MemberMbean is not found to gather metrics
   * @throws ResultDataException
   *           if building result fails
   */
  private ResultData getMemberMetrics(DistributedMember distributedMember, String export_to_report_to, String [] categoriesArr, int cacheServerPort) throws ResultDataException {
    final Cache cache = CacheFactory.getAnyInstance();
    final SystemManagementService managementService = (SystemManagementService)ManagementService.getManagementService(cache);

    ObjectName memberMBeanName = managementService.getMemberMBeanName(distributedMember);
    MemberMXBean memberMxBean = managementService.getMBeanInstance(memberMBeanName, MemberMXBean.class);
    ObjectName csMxBeanName = null;
    CacheServerMXBean csMxBean = null;

    if (memberMxBean != null) {

      if (cacheServerPort != -1) {
         csMxBeanName = managementService.getCacheServerMBeanName(cacheServerPort, distributedMember);
         csMxBean = managementService.getMBeanInstance(csMxBeanName, CacheServerMXBean.class);

        if (csMxBean == null) {
          ErrorResultData erd = ResultBuilder.createErrorResultData();
          erd.addLine(CliStrings.format(CliStrings.SHOW_METRICS__CACHE__SERVER__NOT__FOUND, cacheServerPort, MBeanJMXAdapter.getMemberNameOrId(distributedMember)));
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
        Set<String> checkSet = new HashSet<String>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);


        //Checking if the categories specified by the user are valid or not
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

      /****
       * Member Metrics
       */
      //member, jvm, region, serialization, communication, function, transaction, diskstore, lock, eviction, distribution
      if (categoriesMap.get("member").booleanValue()) {
        writeToTableAndCsv(metricsTable, "member", "upTime", memberMxBean.getMemberUpTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cpuUsage", memberMxBean.getCpuUsage(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "currentHeapSize", memberMxBean.getCurrentHeapSize(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "maximumHeapSize", memberMxBean.getMaximumHeapSize(), csvBuilder);
      }
      /****
       * JVM Metrics
       */
      if (categoriesMap.get("jvm").booleanValue()) {
        writeToTableAndCsv(metricsTable, "jvm ", "jvmThreads ", jvmMetrics.getTotalThreads(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "fileDescriptorLimit", memberMxBean.getFileDescriptorLimit(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalFileDescriptorOpen", memberMxBean.getTotalFileDescriptorOpen(), csvBuilder);
      }
      /***
      Member wide region metrics
       */
      if (categoriesMap.get("region").booleanValue()) {
        writeToTableAndCsv(metricsTable, "region ", "totalRegionCount ", memberMxBean.getTotalRegionCount(), csvBuilder);
        String[] regionNames = memberMxBean.listRegions();
        if (regionNames != null) {
          for (int i=0 ; i < regionNames.length; i++) {
            if (i == 0) {
              writeToTableAndCsv(metricsTable, "", "listOfRegions", regionNames[i].substring(1), csvBuilder);
            } else {
              writeToTableAndCsv(metricsTable, "", "", regionNames[i].substring(1), csvBuilder);
            }
          }
        }

        String[] rootRegionNames = memberMxBean.getRootRegionNames();
        if (rootRegionNames != null) {
          for (int i=0 ; i < rootRegionNames.length; i++) {
            if (i == 0) {
              writeToTableAndCsv(metricsTable, "", "rootRegions", rootRegionNames[i], csvBuilder);
            } else {
              writeToTableAndCsv(metricsTable, "", "", rootRegionNames[i], csvBuilder);
            }
          }
        }
        writeToTableAndCsv(metricsTable, "", "totalRegionEntryCount", memberMxBean.getTotalRegionEntryCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBucketCount", memberMxBean.getTotalBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalPrimaryBucketCount", memberMxBean.getTotalPrimaryBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsAvgLatency", memberMxBean.getGetsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsAvgLatency", memberMxBean.getPutsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "createsRate", memberMxBean.getCreatesRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "destroyRate", memberMxBean.getDestroysRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putAllAvgLatency", memberMxBean.getPutAllAvgLatency(), csvBuilder);
        //Not available from stats. After Stats re-org it will be avaialble
       // writeToTableAndCsv(metricsTable, "", "getAllAvgLatency", memberMxBean.getGetAllAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalMissCount", memberMxBean.getTotalMissCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalHitCount", memberMxBean.getTotalHitCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsRate", memberMxBean.getGetsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsRate", memberMxBean.getPutsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheWriterCallsAvgLatency", memberMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency", memberMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalLoadsCompleted", memberMxBean.getTotalLoadsCompleted(), csvBuilder);

      }

      /******
       * SERIALIZATION
       */
      if (categoriesMap.get("serialization").booleanValue()) {
        writeToTableAndCsv(metricsTable, "serialization", "serializationRate", memberMxBean.getSerializationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "serializationLatency", memberMxBean.getSerializationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "deserializationRate", memberMxBean.getDeserializationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "deserializationLatency", memberMxBean.getDeserializationLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "deserializationAvgLatency", memberMxBean.getDeserializationAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "PDXDeserializationAvgLatency", memberMxBean.getPDXDeserializationAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "PDXDeserializationRate", memberMxBean.getPDXDeserializationRate(), csvBuilder);
      }

      /*** Communication Metrics
       *
       */
      if (categoriesMap.get("communication").booleanValue()) {
        writeToTableAndCsv(metricsTable, "communication", "bytesSentRate", memberMxBean.getBytesSentRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "bytesReceivedRate", memberMxBean.getBytesReceivedRate(), csvBuilder);
        String [] connectedGatewayReceivers = memberMxBean.listConnectedGatewayReceivers();
        writeToTableAndCsv(metricsTable, "", "connectedGatewayReceivers", connectedGatewayReceivers, csvBuilder);

        String [] connectedGatewaySenders = memberMxBean.listConnectedGatewaySenders();
        writeToTableAndCsv(metricsTable, "", "connectedGatewaySenders", connectedGatewaySenders, csvBuilder);

      }

      /***
       * Member wide function metrics
       *
       */
      if (categoriesMap.get("function").booleanValue()) {
        writeToTableAndCsv(metricsTable, "function", "numRunningFunctions", memberMxBean.getNumRunningFunctions(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "functionExecutionRate", memberMxBean.getFunctionExecutionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "numRunningFunctionsHavingResults", memberMxBean.getNumRunningFunctionsHavingResults(), csvBuilder);
        //Not Avaialble from Stats
        //writeToTableAndCsv(metricsTable, "", "funcExecutionQueueSize", memberMxBean.getFuncExecutionQueueSize(), csvBuilder);
      }

      /***
       * totalTransactionsCount
         currentTransactionalThreadIds
         transactionCommitsAvgLatency
         transactionCommittedTotalCount
         transactionRolledBackTotalCount
         transactionCommitsRate
       */
      if (categoriesMap.get("transaction").booleanValue()) {
        writeToTableAndCsv(metricsTable, "transaction", "totalTransactionsCount", memberMxBean.getTotalTransactionsCount(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "transactionCommitsAvgLatency", memberMxBean.getTransactionCommitsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "transactionCommittedTotalCount", memberMxBean.getTransactionCommittedTotalCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "transactionRolledBackTotalCount", memberMxBean.getTransactionRolledBackTotalCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "transactionCommitsRate", memberMxBean.getTransactionCommitsRate(), csvBuilder);
      }
      /***
       * Member wide disk metrics
       */
      if (categoriesMap.get("diskstore").booleanValue()) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalDiskUsage", memberMxBean.getTotalDiskUsage(), csvBuilder); // deadcoded to workaround bug 46397
        writeToTableAndCsv(metricsTable, ""/*46608*/, "diskReadsRate", memberMxBean.getDiskReadsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", memberMxBean.getDiskWritesRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "flushTimeAvgLatency", memberMxBean.getDiskFlushAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalQueueSize", memberMxBean.getTotalDiskTasksWaiting(), csvBuilder); // deadcoded to workaround bug 46397
        writeToTableAndCsv(metricsTable, "", "totalBackupInProgress", memberMxBean.getTotalBackupInProgress(), csvBuilder);
      }
      /***
       * Member wide Lock
       */
      if (categoriesMap.get("lock").booleanValue()) {
        writeToTableAndCsv(metricsTable, "lock", "lockWaitsInProgress", memberMxBean.getLockWaitsInProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalLockWaitTime", memberMxBean.getTotalLockWaitTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalNumberOfLockService", memberMxBean.getTotalNumberOfLockService(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "requestQueues", memberMxBean.getLockRequestQueues(), csvBuilder);
      }
      /****
       * Eviction
       */
      if (categoriesMap.get("eviction").booleanValue()) {
        writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate", memberMxBean.getLruEvictionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lruDestroyRate", memberMxBean.getLruDestroyRate(), csvBuilder);
      }
      /***
       * Distribution
       */
      if (categoriesMap.get("distribution").booleanValue()) {
        writeToTableAndCsv(metricsTable, "distribution", "getInitialImagesInProgress", memberMxBean.getInitialImagesInProgres(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getInitialImageTime", memberMxBean.getInitialImageTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getInitialImageKeysReceived", memberMxBean.getInitialImageKeysReceived(), csvBuilder);
      }

      /***
       * OffHeap
       */
      if (categoriesMap.get("offheap").booleanValue()) {
        writeToTableAndCsv(metricsTable, "offheap", "maxMemory", memberMxBean.getOffHeapMaxMemory(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "freeMemory", memberMxBean.getOffHeapFreeMemory(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "usedMemory", memberMxBean.getOffHeapUsedMemory(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "objects", memberMxBean.getOffHeapObjects(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "fragmentation", memberMxBean.getOffHeapFragmentation(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "compactionTime", memberMxBean.getOffHeapCompactionTime(), csvBuilder);
      }

      /***
       * CacheServer stats
       */
      if (csMxBean != null) {
        writeToTableAndCsv(metricsTable, "cache-server", "clientConnectionCount", csMxBean.getClientConnectionCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hostnameForClients", csMxBean.getHostNameForClients(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getRequestAvgLatency", csMxBean.getGetRequestAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRequestAvgLatency", csMxBean.getPutRequestAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalConnectionsTimedOut", csMxBean.getTotalConnectionsTimedOut(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "threadQueueSize", csMxBean.getPutRequestAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "connectionThreads", csMxBean.getConnectionThreads(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "connectionLoad", csMxBean.getConnectionLoad(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "loadPerConnection", csMxBean.getLoadPerConnection(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "queueLoad", csMxBean.getQueueLoad(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "loadPerQueue", csMxBean.getLoadPerQueue(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getRequestRate", csMxBean.getGetRequestRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRequestRate", csMxBean.getPutRequestRate(), csvBuilder);

        /*****
         * Notification
         */
        writeToTableAndCsv(metricsTable, "notification", "numClientNotificationRequests", csMxBean.getNumClientNotificationRequests(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "clientNotificationRate", csMxBean.getClientNotificationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "clientNotificationAvgLatency", csMxBean.getClientNotificationAvgLatency(), csvBuilder);

        /***
         * Query
         */
        writeToTableAndCsv(metricsTable, "query", "activeCQCount", csMxBean.getActiveCQCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "query", "queryRequestRate", csMxBean.getQueryRequestRate(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "indexCount", csMxBean.getIndexCount(), csvBuilder);

        String [] indexList = csMxBean.getIndexList();
        writeToTableAndCsv(metricsTable, "", "index list", indexList, csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalIndexMaintenanceTime", csMxBean.getTotalIndexMaintenanceTime(), csvBuilder);

      }

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder.toString(), "Member metrics exported to {0}.", false);
      }
      return crd;

    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Member MBean for " + MBeanJMXAdapter.getMemberNameOrId(distributedMember) + " not found");
      return ResultBuilder.createErrorResultData().addLine(errorMessage);
    }
  }

  /****
   * Gets the Cluster-wide metrics for a region
   *
   * @param regionName
   * @return ResultData containing the table
   * @throws ResultDataException
   *           if building result fails
   */
  private ResultData getDistributedRegionMetrics(String regionName, String export_to_report_to, String[] categoriesArr) throws ResultDataException {

    final Cache cache = CacheFactory.getAnyInstance();
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
    	Set<String> checkSet = new HashSet<String>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        //Checking if the categories specified by the user are valid or not
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
      /***
       * General System metrics
       */
      //cluster, region, partition , diskstore, callback, eviction
      if (categoriesMap.get("cluster").booleanValue()) {
        writeToTableAndCsv(metricsTable, "cluster", "member count", regionMxBean.getMemberCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "region entry count", regionMxBean.getSystemRegionEntryCount(), csvBuilder);
      }

      if (categoriesMap.get("region").booleanValue()) {
        writeToTableAndCsv(metricsTable, "region", "lastModifiedTime", regionMxBean.getLastModifiedTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lastAccessedTime", regionMxBean.getLastAccessedTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "missCount", regionMxBean.getMissCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitCount", regionMxBean.getHitCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitRatio", regionMxBean.getHitRatio(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsRate", regionMxBean.getGetsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsRate", regionMxBean.getPutsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "createsRate", regionMxBean.getCreatesRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "destroyRate", regionMxBean.getDestroyRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putAllRate", regionMxBean.getPutAllRate(), csvBuilder);

      }

      if (categoriesMap.get("partition").booleanValue()) {
        writeToTableAndCsv(metricsTable, "partition", "putLocalRate", regionMxBean.getPutLocalRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteRate", regionMxBean.getPutRemoteRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteLatency", regionMxBean.getPutRemoteLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteAvgLatency", regionMxBean.getPutRemoteAvgLatency(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "bucketCount", regionMxBean.getBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "primaryBucketCount", regionMxBean.getPrimaryBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "numBucketsWithoutRedundancy", regionMxBean.getNumBucketsWithoutRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBucketSize", regionMxBean.getTotalBucketSize(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "averageBucketSize", regionMxBean.getAvgBucketSize(), csvBuilder);
      }
      /*****
       * Disk store
       */
      if (categoriesMap.get("diskstore").booleanValue()) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalEntriesOnlyOnDisk", regionMxBean.getTotalEntriesOnlyOnDisk(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskReadsRate", regionMxBean.getDiskReadsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", regionMxBean.getDiskWritesRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalDiskWriteInProgress", regionMxBean.getTotalDiskWritesProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskTaskWaiting", regionMxBean.getDiskTaskWaiting(), csvBuilder);

      }
      /*****
       * LISTENER
       */
      if (categoriesMap.get("callback").booleanValue()) {
        writeToTableAndCsv(metricsTable, "callback", "cacheWriterCallsAvgLatency", regionMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency", regionMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
      }

      /****
       * Eviction
       */
      if (categoriesMap.get("eviction").booleanValue()) {
        writeToTableAndCsv(metricsTable, "eviction" , "lruEvictionRate", regionMxBean.getLruEvictionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "" , "lruDestroyRate", regionMxBean.getLruDestroyRate(), csvBuilder);
      }

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder.toString(), "Aggregate Region Metrics exported to {0}.", false);
      }

      return crd;
    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Distributed Region MBean for " + regionName + " not found");
      erd.addLine(errorMessage);
      return erd;
    }
  }

  /***
   * Gets the metrics of region on a given member
   *
   * @param regionName
   * @param distributedMember
   * @param export_to_report_to
   * @return ResultData with required Region statistics or ErrorResultData if
   *         Region MBean is not found to gather metrics
   * @throws ResultDataException
   *           if building result fails
   */
  private ResultData getRegionMetricsFromMember(String regionName, DistributedMember distributedMember, String export_to_report_to, String[] categoriesArr) throws ResultDataException {

    final Cache cache = CacheFactory.getAnyInstance();
    final SystemManagementService managementService = (SystemManagementService) ManagementService
        .getManagementService(cache);

    ObjectName regionMBeanName = managementService.getRegionMBeanName(distributedMember, regionName);
    RegionMXBean regionMxBean = managementService.getMBeanInstance(regionMBeanName, RegionMXBean.class);

    if (regionMxBean != null) {
      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      SectionResultData section = crd.addSection();
      TabularResultData metricsTable = section.addTable();
      metricsTable.setHeader("Metrics for region:" + regionName+ " On Member " + MBeanJMXAdapter.getMemberNameOrId(distributedMember));
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

      /****
       * Region Metrics
       */
      Map<String, Boolean> categoriesMap = getRegionMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<String>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        //Checking if the categories specified by the user are valid or not
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

      if (categoriesMap.get("region").booleanValue()) {
        writeToTableAndCsv(metricsTable, "region", "lastModifiedTime", regionMxBean.getLastModifiedTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lastAccessedTime", regionMxBean.getLastAccessedTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "missCount", regionMxBean.getMissCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitCount", regionMxBean.getHitCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitRatio", regionMxBean.getHitRatio(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsRate", regionMxBean.getGetsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsRate", regionMxBean.getPutsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "createsRate", regionMxBean.getCreatesRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "destroyRate", regionMxBean.getDestroyRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putAllRate", regionMxBean.getPutAllRate(), csvBuilder);
      }

      if (categoriesMap.get("partition").booleanValue()) {
        writeToTableAndCsv(metricsTable, "partition", "putLocalRate", regionMxBean.getPutLocalRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteRate", regionMxBean.getPutRemoteRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteLatency", regionMxBean.getPutRemoteLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteAvgLatency", regionMxBean.getPutRemoteAvgLatency(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "bucketCount", regionMxBean.getBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "primaryBucketCount", regionMxBean.getPrimaryBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "configuredRedundancy", regionMxBean.getConfiguredRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "actualRedundancy", regionMxBean.getActualRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "numBucketsWithoutRedundancy", regionMxBean.getNumBucketsWithoutRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBucketSize", regionMxBean.getTotalBucketSize(), csvBuilder);
        //writeToTableAndCsv(metricsTable, "", "averageBucketSize", regionMxBean.getAvgBucketSize(), csvBuilder);
      }
      /*****
       * Disk store
       */
      if (categoriesMap.get("diskstore").booleanValue()) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalEntriesOnlyOnDisk", regionMxBean.getTotalEntriesOnlyOnDisk(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskReadsRate", ""+regionMxBean.getDiskReadsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", regionMxBean.getDiskWritesRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalDiskWriteInProgress", regionMxBean.getTotalDiskWritesProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskTaskWaiting", regionMxBean.getDiskTaskWaiting(), csvBuilder);
      }
      /*****
       * LISTENER
       */
      if (categoriesMap.get("callback").booleanValue()) {
        writeToTableAndCsv(metricsTable, "callback", "cacheWriterCallsAvgLatency", regionMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency", regionMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
      }

      /****
       * Eviction
       */
      if (categoriesMap.get("eviction").booleanValue()) {
        writeToTableAndCsv(metricsTable, "eviction" , "lruEvictionRate", regionMxBean.getLruEvictionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "" , "lruDestroyRate", regionMxBean.getLruDestroyRate(), csvBuilder);
      }
      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder.toString(), "Region Metrics exported to {0}.", false);
      }

      return crd;
    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Region MBean for " + regionName + " on member " +  MBeanJMXAdapter.getMemberNameOrId(distributedMember) + " not found");
      erd.addLine(errorMessage);
      return erd;
    }

  }


  /*** Writes an entry to a TabularResultData and writes a comma separated entry to a string builder
   * @param metricsTable
   * @param type
   * @param metricName
   * @param metricValue
   * @param csvBuilder
   */
  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName, long metricValue, StringBuilder csvBuilder) {
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

  /***
   *
   * @param metricsTable
   * @param type
   * @param metricName
   * @param metricValue
   * @param csvBuilder
   */
  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName, double metricValue, StringBuilder csvBuilder) {
    metricsTable.accumulate(CliStrings.SHOW_METRICS__TYPE__HEADER, type);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__METRIC__HEADER, metricName);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__VALUE__HEADER, Double.valueOf(metricValue));

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
    Set<String> categoriesSet = new HashSet<String>();
    for (String category : categories) {
      categoriesSet.add(category);
    }
    return categoriesSet;
  }

  /****
   * Defines and returns map of categories for System metrics.
   * @return map with categories for system metrics and display flag set to true
   */
  private Map<String, Boolean> getSystemMetricsCategories() {
    Map<String, Boolean> categories = new HashMap<String, Boolean>();
    categories.put("cluster", true);
    categories.put("cache", true);
    categories.put("diskstore", true);
    categories.put("query", true);
    return categories;
  }

  /****
   * Defines and returns map of categories for Region Metrics
   * @return map with categories for region metrics and display flag set to true
   */
  private Map<String, Boolean> getRegionMetricsCategories() {
    Map<String, Boolean> categories = new HashMap<String, Boolean>();

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

  /****
   * Defines and returns map of categories for system-wide region metrics
   * @return map with categories for system wide region metrics and display flag set to true
   */
  private Map<String, Boolean> getSystemRegionMetricsCategories() {
    Map<String, Boolean> categories = getRegionMetricsCategories();
    categories.put("cluster", true);
    return categories;
  }

  /*****
   * Defines and returns map of categories for member metrics
   * @return map with categories for member metrics and display flag set to true
   */
  private Map<String, Boolean> getMemberMetricsCategories() {
    Map<String, Boolean> categories = new HashMap<String, Boolean>();
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

  /***
   * Converts an array of strings to a String delimited by a new line character
   * for display purposes
   *
   * @param names
   * @param startIndex
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

  private void writeToTableAndCsv(TabularResultData metricsTable, String type,
      String metricName, String metricValue[], StringBuilder csvBuilder) {
    if (metricValue != null) {
      for (int i=0 ; i < metricValue.length; i++) {
        if (i == 0) {
          writeToTableAndCsv(metricsTable, type, metricName, metricValue[i], csvBuilder);
        } else {
          writeToTableAndCsv(metricsTable, "", "", metricValue[i], csvBuilder);
        }
      }
    }
  }

  /***
   * Writes to a TabularResultData and also appends a CSV string to a String builder
   * @param metricsTable
   * @param type
   * @param metricName
   * @param metricValue
   * @param csvBuilder
   */
  private void writeToTableAndCsv(TabularResultData metricsTable, String type,
      String metricName, String metricValue, StringBuilder csvBuilder) {
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
  @CliMetaData(relatedTopic = { CliStrings.TOPIC_CHANGELOGLEVEL })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.WRITE)
  public Result changeLogLevel(
      @CliOption(key = CliStrings.CHANGE_LOGLEVEL__MEMBER, unspecifiedDefaultValue = "", help = CliStrings.CHANGE_LOGLEVEL__MEMBER__HELP) String[] memberIds, 
      @CliOption(key = CliStrings.CHANGE_LOGLEVEL__GROUPS, unspecifiedDefaultValue = "", help = CliStrings.CHANGE_LOGLEVEL__GROUPS__HELP) String[] grps,
      @CliOption(key = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL, optionContext = ConverterHint.LOG_LEVEL, mandatory = true, unspecifiedDefaultValue = "", help = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL__HELP) String logLevel ) {
    try {      
      if( (memberIds == null || memberIds.length == 0 ) && (grps== null || grps.length == 0   ) ){
        return ResultBuilder.createUserErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG__SPECIFY_GRP_OR_MEMBER);
      }
      
      if(logLevel == null || logLevel.length() == 0){
        return ResultBuilder.createUserErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG__SPECIFY_LOG_LEVEL);
      }
      
      Cache cache = GemFireCacheImpl.getInstance();
      LogWriter logger = cache.getLogger();
      
      boolean validLogLevel = false;     
      
      for(int i = 0; i < LogWriterImpl.allLevels.length -1 ; i++){        
        if(LogWriterImpl.allLevels[i] == LogWriterImpl.levelNameToCode(logLevel)){
          validLogLevel = true;
          break;
        }
      }
      
      if(!validLogLevel){
        return ResultBuilder.createUserErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG__INVALID_LOG_LEVEL);
      }
      
      Set<DistributedMember> dsMembers = new HashSet<DistributedMember>();
      Set<DistributedMember> ds = CliUtil.getAllMembers(cache);
      
      
      if(grps != null && grps.length > 0){
        for(String grp : grps){
          dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(grp));
        }
      }
      
      if(memberIds != null && memberIds.length > 0){
        for(String member : memberIds){          
          Iterator<DistributedMember> it = ds.iterator();
          while(it.hasNext()){
            DistributedMember mem = it.next();            
            if( mem.getName() == null ? false : mem.getName().equals(member) || 
                mem.getId().equals(member)){
              dsMembers.add(mem);
              break;    
            }
          }
        }
      }  
      
      if(dsMembers.size() == 0){
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

      Execution execution = FunctionService.onMembers(dsMembers).withArgs(functionArgs);
      if (execution == null) {
        return ResultBuilder.createUserErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG__CANNOT_EXECUTE);
      }
      List<?> resultList = (List<?>) execution.execute(logFunction).getResult();  
      
      for (Object object : resultList) {
        try {
          if (object instanceof Throwable) {
            logger.warning("Exception in ChangeLogLevelFunction " + ((Throwable) object).getMessage(), ((Throwable) object));
            continue;
          }
            
          if (object != null) {
            Map<String, String> resultMap = (Map<String, String>) object;              
            Entry<String, String> entry = resultMap.entrySet().iterator().next();
              
            if(entry.getValue().contains("ChangeLogLevelFunction exception")){
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_MEMBER, entry.getKey());
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_STATUS, "false");
            }else{
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_MEMBER, entry.getKey());
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_STATUS, "true");
            }           
              
          }
        } catch (Exception ex) {
          LogWrapper.getInstance().warning("change log level command exception " + ex);
          continue;
        }
      }     
            
      Result result = ResultBuilder.buildResult(compositeResultData);
      logger.info("change log-level command result=" +result);      
      return result;      
    } catch (Exception ex) {
      GemFireCacheImpl.getInstance().getLogger().error("GFSH Changeloglevel exception: " + ex);
      return ResultBuilder.createUserErrorResult( ex.getMessage());
    }
  }
  
  

  @CliAvailabilityIndicator({ CliStrings.SHUTDOWN, CliStrings.GC,
    CliStrings.SHOW_DEADLOCK, CliStrings.SHOW_METRICS, CliStrings.SHOW_LOG,
    CliStrings.EXPORT_STACKTRACE, CliStrings.NETSTAT, CliStrings.EXPORT_LOGS, CliStrings.CHANGE_LOGLEVEL })
  public boolean shutdownCommandAvailable() {
    boolean isAvailable = true; // always available on server
    if (CliUtil.isGfshVM()) { // in gfsh check if connected
      isAvailable = getGfsh() != null && getGfsh().isConnectedAndReady();
    }
    return isAvailable;
  }

  private Set<String> getSetDifference(Set<String> set1, Set<String> set2) {
    Set<String> setDifference = new HashSet<String>();
    for (String element : set1) {
      if (!(set2.contains(element.toLowerCase()))) {
    	setDifference.add(element);
      }
    }
    return setDifference;
  }
}
