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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.GatewayReceiverMXBean;
import com.gemstone.gemfire.management.GatewaySenderMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.GatewayReceiverCreateFunction;
import com.gemstone.gemfire.management.internal.cli.functions.GatewayReceiverFunctionArgs;
import com.gemstone.gemfire.management.internal.cli.functions.GatewaySenderCreateFunction;
import com.gemstone.gemfire.management.internal.cli.functions.GatewaySenderFunctionArgs;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.configuration.SharedConfigurationWriter;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

public class WanCommands implements CommandMarker {

  private Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  @CliCommand(value = CliStrings.CREATE_GATEWAYSENDER, help = CliStrings.CREATE_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN, writesToSharedConfiguration=true)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result createGatewaySender(
      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.CREATE_GATEWAYSENDER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String[] onGroups,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.CREATE_GATEWAYSENDER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ID,
      mandatory = true,
      help = CliStrings.CREATE_GATEWAYSENDER__ID__HELP) String id,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID,
      mandatory = true,
      help = CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID__HELP) Integer remoteDistributedSystemId,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__PARALLEL,
      help = CliStrings.CREATE_GATEWAYSENDER__PARALLEL__HELP) Boolean parallel,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__MANUALSTART,
      help = CliStrings.CREATE_GATEWAYSENDER__MANUALSTART__HELP) Boolean manualStart,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE,
      help = CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE__HELP) Integer socketBufferSize,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT,
      help = CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT__HELP) Integer socketReadTimeout,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION,
      help = CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION__HELP) Boolean enableBatchConflation,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE,
      help = CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE__HELP) Integer batchSize,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL,
      help = CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL__HELP) Integer batchTimeInterval,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE,
      help = CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE__HELP) Boolean enablePersistence,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME,
      help = CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME__HELP) String diskStoreName,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS,
      help = CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS__HELP) Boolean diskSynchronous,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY,
      help = CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY__HELP) Integer maxQueueMemory,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD,
      help = CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD__HELP) Integer alertThreshold,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS,
      help = CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS__HELP) Integer dispatcherThreads,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY,
      help = CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY__HELP) String orderPolicy,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER,
      help = CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER__HELP)
      @CliMetaData (valueSeparator = ",")
      String[] gatewayEventFilters,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER,
      help = CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER__HELP)
      @CliMetaData (valueSeparator = ",")
      String[] gatewayTransportFilter) {

    Result result = null;

    XmlEntity xmlEntity = null;
    try {
      GatewaySenderFunctionArgs gatewaySenderFunctionArgs =
        new GatewaySenderFunctionArgs(id,
            remoteDistributedSystemId, parallel, manualStart, socketBufferSize, socketReadTimeout,
            enableBatchConflation, batchSize, batchTimeInterval, enablePersistence, diskStoreName,
            diskSynchronous, maxQueueMemory, alertThreshold, dispatcherThreads, orderPolicy,
            gatewayEventFilters, gatewayTransportFilter);

      Set<DistributedMember> membersToCreateGatewaySenderOn = CliUtil.findAllMatchingMembers(onGroups, onMember == null ? null : onMember.split(","));

      ResultCollector<?, ?> resultCollector = CliUtil.executeFunction(GatewaySenderCreateFunction.INSTANCE, gatewaySenderFunctionArgs, membersToCreateGatewaySenderOn);
      @SuppressWarnings("unchecked")
      List<CliFunctionResult> gatewaySenderCreateResults = (List<CliFunctionResult>) resultCollector.getResult();

      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      final String errorPrefix = "ERROR: ";
      for (CliFunctionResult gatewaySenderCreateResult : gatewaySenderCreateResults) {
        boolean success  = gatewaySenderCreateResult.isSuccessful();
        tabularResultData.accumulate("Member", gatewaySenderCreateResult.getMemberIdOrName());
        tabularResultData.accumulate("Status", (success ? "" : errorPrefix) + gatewaySenderCreateResult.getMessage());

        if (success && xmlEntity == null) {
          xmlEntity = gatewaySenderCreateResult.getXmlEntity();
        }
      }
      result = ResultBuilder.buildResult(tabularResultData);
    } catch (IllegalArgumentException e) {
      LogWrapper.getInstance().info(e.getMessage());
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    }

    if (xmlEntity != null) {
      result.setCommandPersisted((new SharedConfigurationWriter()).addXmlEntity(xmlEntity, onGroups));
    }
    return result;
  }

  @CliCommand(value = CliStrings.START_GATEWAYSENDER, help = CliStrings.START_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result startGatewaySender(
      @CliOption(key = CliStrings.START_GATEWAYSENDER__ID,
      mandatory = true,
      optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.START_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = CliStrings.START_GATEWAYSENDER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.START_GATEWAYSENDER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String onGroup,

      @CliOption(key = CliStrings.START_GATEWAYSENDER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      help = CliStrings.START_GATEWAYSENDER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember) {

    Result result = null;
    final String id = senderId.trim();

    try {
      final Cache cache = CacheFactory.getAnyInstance();
      final SystemManagementService service = (SystemManagementService)ManagementService
          .getExistingManagementService(cache);

      TabularResultData resultData = ResultBuilder.createTabularResultData();
      Set<DistributedMember> dsMembers = CliUtil.findAllMatchingMembers(
          onGroup, onMember);

      ExecutorService execService = Executors
          .newCachedThreadPool(new ThreadFactory() {
            AtomicInteger threadNum = new AtomicInteger();

            public Thread newThread(final Runnable r) {
              Thread result = new Thread(r, "Start Sender Command Thread "
                  + threadNum.incrementAndGet());
              result.setDaemon(true);
              return result;
            }
          });

      List<Callable<List>> callables = new ArrayList<Callable<List>>();

      for (final DistributedMember member : dsMembers) {

        callables.add(new Callable<List>() {

          public List call() throws Exception {

            GatewaySenderMXBean bean = null;
            ArrayList<String> statusList = new ArrayList<String>();
            if (cache.getDistributedSystem().getDistributedMember().getId()
                .equals(member.getId())) {
              bean = service.getLocalGatewaySenderMXBean(id);
            }
            else {
              ObjectName objectName = service.getGatewaySenderMBeanName(member,
                  id);
              bean = service.getMBeanProxy(objectName,
                  GatewaySenderMXBean.class);
            }
            if (bean != null) {
              if (bean.isRunning()) {
                statusList.add(member.getId());
                statusList.add(CliStrings.GATEWAY_ERROR);
                statusList.add(CliStrings.format(
                    CliStrings.GATEWAY_SENDER_0_IS_ALREADY_STARTED_ON_MEMBER_1,
                    new Object[] { id, member.getId() }));
              }
              else {
                bean.start();
                statusList.add(member.getId());
                statusList.add(CliStrings.GATEWAY_OK);
                statusList.add(CliStrings.format(
                    CliStrings.GATEWAY_SENDER_0_IS_STARTED_ON_MEMBER_1,
                    new Object[] { id, member.getId() }));
              }
            }
            else {
              statusList.add(member.getId());
              statusList.add(CliStrings.GATEWAY_ERROR);
              statusList.add(CliStrings.format(
                  CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
                  new Object[] { id, member.getId() }));
            }
            return statusList;
          }
        });
      }

      Iterator<DistributedMember> memberIterator = dsMembers.iterator();
      List<Future<List>> futures = null;

      try {
        futures = execService.invokeAll(callables);
      }
      catch (InterruptedException ite) {
        accumulateStartResult(resultData, null,
            CliStrings.GATEWAY_ERROR, CliStrings.format(
                CliStrings.GATEWAY_SENDER_0_COULD_NOT_BE_INVOKED_DUE_TO_1,
                new Object[] { id, ite.getMessage() }));
      }

      for (Future<List> future : futures) {
        DistributedMember member = memberIterator.next();
        List<String> memberStatus = null;
        try {
          memberStatus = future.get();
          accumulateStartResult(resultData, memberStatus.get(0),
              memberStatus.get(1), memberStatus.get(2));
        }
        catch (InterruptedException ite) {
          accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR, CliStrings.format(
                  CliStrings.GATEWAY_SENDER_0_COULD_NOT_BE_STARTED_ON_MEMBER_DUE_TO_1,
                  new Object[] { id, ite.getMessage() }));
          continue;
        }
        catch (ExecutionException ee) {
          accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR, CliStrings.format(
                  CliStrings.GATEWAY_SENDER_0_COULD_NOT_BE_STARTED_ON_MEMBER_DUE_TO_1,
                  new Object[] { id, ee.getMessage() }));
          continue;
        }
      }
      execService.shutdown();
      result = ResultBuilder.buildResult(resultData);
    }
    catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }

    return result;
  }

  @CliCommand(value = CliStrings.PAUSE_GATEWAYSENDER, help = CliStrings.PAUSE_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result pauseGatewaySender(
      @CliOption(key = CliStrings.PAUSE_GATEWAYSENDER__ID,
      mandatory = true,
      optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.PAUSE_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = CliStrings.PAUSE_GATEWAYSENDER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.PAUSE_GATEWAYSENDER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",")  String onGroup,

      @CliOption(key = CliStrings.PAUSE_GATEWAYSENDER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      help = CliStrings.PAUSE_GATEWAYSENDER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember) {

    Result result = null;
    if (senderId != null)
      senderId = senderId.trim();
//    if (memberNameOrId != null)
//      memberNameOrId = memberNameOrId.trim();
//
//    if (memberNameOrId != null && onGroup != null) {
//      return ResultBuilder
//          .createUserErrorResult(CliStrings.GATEWAY__MSG__OPTIONS);
//    }

    try {
      Cache cache = CacheFactory.getAnyInstance();
      SystemManagementService service = (SystemManagementService) ManagementService
          .getExistingManagementService(cache);

      GatewaySenderMXBean bean = null;


      TabularResultData resultData = ResultBuilder.createTabularResultData();
      Set<DistributedMember> dsMembers = null;

      dsMembers = CliUtil.findAllMatchingMembers(onGroup, onMember);
      for (DistributedMember member : dsMembers) {
        if (cache.getDistributedSystem().getDistributedMember().getId().equals(
            member.getId())) {
          bean = service.getLocalGatewaySenderMXBean(senderId);
        } else {
          ObjectName objectName = service.getGatewaySenderMBeanName(member, senderId);
          bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
        }
        if (bean != null) {
          if (bean.isRunning()) {
            if (bean.isPaused()) {
              accumulateStartResult(
                  resultData,
                  member.getId(),
                  CliStrings.GATEWAY_ERROR,
                  CliStrings
                      .format(
                          CliStrings.GATEWAY_SENDER_0_IS_ALREADY_PAUSED_ON_MEMBER_1,
                          new Object[] { senderId, member.getId() }));
            } else {
              bean.pause();
              accumulateStartResult(resultData, member.getId(),
                  CliStrings.GATEWAY_OK, CliStrings.format(
                      CliStrings.GATEWAY_SENDER_0_IS_PAUSED_ON_MEMBER_1,
                      new Object[] { senderId, member.getId() }));
            }
          } else {
            accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_ERROR, CliStrings.format(
                    CliStrings.GATEWAY_SENDER_0_IS_NOT_RUNNING_ON_MEMBER_1,
                    new Object[] { senderId, member.getId() }));
          }
        } else {
          accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR, CliStrings.format(
                  CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
                  new Object[] { senderId, member.getId() }));
        }
      }
      result = ResultBuilder.buildResult(resultData);
    } catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = CliStrings.RESUME_GATEWAYSENDER, help = CliStrings.RESUME_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource=Resource.DATA, operation = Operation.MANAGE)
  public Result resumeGatewaySender(
      @CliOption(key = CliStrings.RESUME_GATEWAYSENDER__ID,
      mandatory = true,
      optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.RESUME_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = CliStrings.RESUME_GATEWAYSENDER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.RESUME_GATEWAYSENDER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String onGroup,

      @CliOption(key = CliStrings.RESUME_GATEWAYSENDER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      help = CliStrings.RESUME_GATEWAYSENDER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember) {

    Result result = null;
    if (senderId != null)
      senderId = senderId.trim();
//    if (memberNameOrId != null)
//      memberNameOrId = memberNameOrId.trim();
//
//    if (memberNameOrId != null && onGroup != null) {
//      return ResultBuilder
//          .createUserErrorResult(CliStrings.GATEWAY__MSG__OPTIONS);
//    }

    try {
      Cache cache = CacheFactory.getAnyInstance();
      SystemManagementService service = (SystemManagementService) ManagementService
          .getExistingManagementService(cache);


      GatewaySenderMXBean bean = null;
//
//      if (memberNameOrId != null && memberNameOrId.length() > 0) {
//        InfoResultData resultData = ResultBuilder.createInfoResultData();
//        DistributedMember memberToBeInvoked = CliUtil
//            .getDistributedMemberByNameOrId(memberNameOrId);
//
//        if (memberToBeInvoked != null) {
//          String memberId = memberToBeInvoked.getId();
//          if (cache.getDistributedSystem().getDistributedMember().getId()
//              .equals(memberId)) {
//            bean = service.getLocalGatewaySenderMXBean(senderId);
//          } else {
//            ObjectName objectName = service.getGatewaySenderMBeanName(memberToBeInvoked,
//                senderId);
//            bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
//          }
//          if (bean != null) {
//            if (bean.isRunning()) {
//              if (bean.isPaused()) {
//                bean.resume();
//                resultData.addLine(CliStrings.format(
//                    CliStrings.GATEWAY_SENDER_0_IS_RESUMED_ON_MEMBER_1,
//                    new Object[] { senderId, memberId }));
//                return ResultBuilder.buildResult(resultData);
//              }
//              resultData.addLine(CliStrings.format(
//                  CliStrings.GATEWAY_SENDER_0_IS_NOT_PAUSED_ON_MEMBER_1,
//                  new Object[] { senderId, memberId }));
//              return ResultBuilder.buildResult(resultData);
//            }
//            resultData.addLine(CliStrings.format(
//                CliStrings.GATEWAY_SENDER_0_IS_NOT_RUNNING_ON_MEMBER_1,
//                new Object[] { senderId, memberId }));
//            return ResultBuilder.buildResult(resultData);
//          }
//          return ResultBuilder.createBadConfigurationErrorResult(CliStrings
//              .format(CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
//                  new Object[] { senderId, memberId }));
//        }
//        return ResultBuilder.createUserErrorResult(CliStrings.format(
//            CliStrings.GATEWAY_MSG_MEMBER_0_NOT_FOUND,
//            new Object[] { memberNameOrId }));
//      }

      TabularResultData resultData = ResultBuilder.createTabularResultData();
      Set<DistributedMember> dsMembers = null;
//      if (onGroup != null && onGroup.length > 0) {
//        dsMembers = CliUtil.getDistributedMembersByGroup(cache, onGroup);
//      } else {
//        dsMembers = CliUtil.getAllNormalMembers(cache);
//      }
//      if (dsMembers.isEmpty()) {
//        return ResultBuilder
//            .createUserErrorResult(CliStrings.GATEWAY_MSG_MEMBERS_NOT_FOUND);
//      }
      dsMembers = CliUtil.findAllMatchingMembers(onGroup, onMember);
      for (DistributedMember member : dsMembers) {
        if (cache.getDistributedSystem().getDistributedMember().getId().equals(
            member.getId())) {
          bean = service.getLocalGatewaySenderMXBean(senderId);
        } else {
          ObjectName objectName = service.getGatewaySenderMBeanName(member, senderId);
          bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
        }
        if (bean != null) {
          if (bean.isRunning()) {
            if (bean.isPaused()) {
              bean.resume();
              accumulateStartResult(resultData, member.getId(),
                  CliStrings.GATEWAY_OK, CliStrings.format(
                      CliStrings.GATEWAY_SENDER_0_IS_RESUMED_ON_MEMBER_1,
                      new Object[] { senderId, member.getId() }));
            } else {
              accumulateStartResult(resultData, member.getId(),
                  CliStrings.GATEWAY_ERROR, CliStrings.format(
                      CliStrings.GATEWAY_SENDER_0_IS_NOT_PAUSED_ON_MEMBER_1,
                      new Object[] { senderId, member.getId() }));
            }
          } else {
            accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_ERROR, CliStrings.format(
                    CliStrings.GATEWAY_SENDER_0_IS_NOT_RUNNING_ON_MEMBER_1,
                    new Object[] { senderId, member.getId() }));
          }
        } else {
          accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR, CliStrings.format(
                  CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
                  new Object[] { senderId, member.getId() }));
        }
      }
      result = ResultBuilder.buildResult(resultData);
    } catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = CliStrings.STOP_GATEWAYSENDER, help = CliStrings.STOP_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result stopGatewaySender(
      @CliOption(key = CliStrings.STOP_GATEWAYSENDER__ID,
      mandatory = true,
      optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.STOP_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = CliStrings.STOP_GATEWAYSENDER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.STOP_GATEWAYSENDER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String onGroup,

      @CliOption(key = CliStrings.STOP_GATEWAYSENDER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      help = CliStrings.STOP_GATEWAYSENDER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember) {

    Result result = null;
    if (senderId != null)
      senderId = senderId.trim();

    try {
      Cache cache = CacheFactory.getAnyInstance();
      SystemManagementService service = (SystemManagementService) ManagementService
          .getExistingManagementService(cache);


      GatewaySenderMXBean bean = null;


      TabularResultData resultData = ResultBuilder.createTabularResultData();
      Set<DistributedMember> dsMembers = CliUtil.findAllMatchingMembers(onGroup, onMember);

      for (DistributedMember member : dsMembers) {
        if (cache.getDistributedSystem().getDistributedMember().getId().equals(
            member.getId())) {
          bean = service.getLocalGatewaySenderMXBean(senderId);
        } else {
          ObjectName objectName = service.getGatewaySenderMBeanName(member, senderId);
          bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
        }
        if (bean != null) {
          if (bean.isRunning()) {
            bean.stop();
            accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_OK, CliStrings.format(
                    CliStrings.GATEWAY_SENDER_0_IS_STOPPED_ON_MEMBER_1,
                    new Object[] { senderId, member.getId() }));

          } else {
            accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_ERROR, CliStrings.format(
                    CliStrings.GATEWAY_SENDER_0_IS_NOT_RUNNING_ON_MEMBER_1,
                    new Object[] { senderId, member.getId() }));
          }
        } else {
          accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR, CliStrings.format(
                  CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
                  new Object[] { senderId, member.getId() }));
        }
      }
      result = ResultBuilder.buildResult(resultData);
    } catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = CliStrings.CREATE_GATEWAYRECEIVER, help = CliStrings.CREATE_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation( resource=Resource.DATA, operation = Operation.MANAGE)
  public Result createGatewayReceiver(
      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.CREATE_GATEWAYRECEIVER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String[] onGroups,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.CREATE_GATEWAYRECEIVER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART,
      help = CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART__HELP) Boolean manualStart,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT,
      help = CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT__HELP) Integer startPort,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT,
      help = CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT__HELP) Integer endPort,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS,
      help = CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS__HELP) String bindAddress,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS,
      help = CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS__HELP) Integer maximumTimeBetweenPings,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE,
      help = CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE__HELP) Integer socketBufferSize,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER,
      help = CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER__HELP)
      @CliMetaData (valueSeparator = ",")
      String[] gatewayTransportFilters) {

    Result result = null;

    XmlEntity xmlEntity = null;
    try {
      GatewayReceiverFunctionArgs gatewayReceiverFunctionArgs = new GatewayReceiverFunctionArgs(manualStart, startPort, endPort, bindAddress,
          socketBufferSize, maximumTimeBetweenPings, gatewayTransportFilters);

      Set<DistributedMember> membersToCreateGatewayReceiverOn = CliUtil.findAllMatchingMembers(onGroups, onMember == null ? null : onMember.split(","));

      ResultCollector<?, ?> resultCollector = CliUtil.executeFunction(GatewayReceiverCreateFunction.INSTANCE,
          gatewayReceiverFunctionArgs, membersToCreateGatewayReceiverOn);
      @SuppressWarnings("unchecked")
      List<CliFunctionResult> gatewayReceiverCreateResults = (List<CliFunctionResult>) resultCollector.getResult();

      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      final String errorPrefix = "ERROR: ";

      for (CliFunctionResult gatewayReceiverCreateResult : gatewayReceiverCreateResults) {
        boolean success = gatewayReceiverCreateResult.isSuccessful();
        tabularResultData.accumulate("Member", gatewayReceiverCreateResult.getMemberIdOrName());
        tabularResultData.accumulate("Status", (success ? "" : errorPrefix) + gatewayReceiverCreateResult.getMessage());

        if (success && xmlEntity == null) {
          xmlEntity = gatewayReceiverCreateResult.getXmlEntity();
        }
      }
      result = ResultBuilder.buildResult(tabularResultData);
    } catch (IllegalArgumentException e) {
      LogWrapper.getInstance().info(e.getMessage());
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    }
    catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    }

    if (xmlEntity != null) {
      result.setCommandPersisted((new SharedConfigurationWriter()).addXmlEntity(xmlEntity, onGroups));
    }

    return result;
  }

  @CliCommand(value = CliStrings.LOAD_BALANCE_GATEWAYSENDER, help = CliStrings.LOAD_BALANCE_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result loadBalanceGatewaySender(
      @CliOption(key = CliStrings.LOAD_BALANCE_GATEWAYSENDER__ID,
      mandatory = true,
      optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.LOAD_BALANCE_GATEWAYSENDER__ID__HELP) String senderId) {

    Result result = null;
    if (senderId != null) {
      senderId = senderId.trim();
    }

    try {
      Cache cache = CacheFactory.getAnyInstance();
      SystemManagementService service = (SystemManagementService) ManagementService
          .getExistingManagementService(cache);
      TabularResultData resultData = ResultBuilder.createTabularResultData();
      Set<DistributedMember> dsMembers = CliUtil.getAllNormalMembers(cache);

      if (dsMembers.isEmpty()) {
        result = ResultBuilder.createInfoResult(CliStrings.GATEWAY_MSG_MEMBERS_NOT_FOUND);
      } else {
        boolean gatewaySenderExists = false;
        for (DistributedMember member : dsMembers) {
          GatewaySenderMXBean bean = null;
          if (cache.getDistributedSystem().getDistributedMember().getId().equals(
              member.getId())) {
            bean = service.getLocalGatewaySenderMXBean(senderId);
          } else {
            ObjectName objectName = service.getGatewaySenderMBeanName(member, senderId);
            bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
          }
          if (bean != null) {
            gatewaySenderExists = true;
            bean.rebalance();
            accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_OK, CliStrings.format(
                    CliStrings.GATEWAY_SENDER_0_IS_REBALANCED_ON_MEMBER_1,
                    new Object[] { senderId, member.getId() }));
          } else {
            accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_ERROR, CliStrings.format(
                    CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
                    new Object[] { senderId, member.getId() }));
          }
        }
        if (gatewaySenderExists) {
          result = ResultBuilder.buildResult(resultData);
        } else {
          result = ResultBuilder.createInfoResult(CliStrings.format(
              CliStrings.GATEWAY_SENDER_0_IS_NOT_FOUND_ON_ANY_MEMBER,
              new Object[] { senderId }));
        }
      }
    } catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }

    return result;
  }

  @CliCommand(value = CliStrings.START_GATEWAYRECEIVER, help = CliStrings.START_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result startGatewayReceiver(
      @CliOption(key = CliStrings.START_GATEWAYRECEIVER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.START_GATEWAYRECEIVER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",")  String onGroup,

      @CliOption(key = CliStrings.START_GATEWAYRECEIVER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      help = CliStrings.START_GATEWAYRECEIVER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember) {
    Result result = null;


    try {
      Cache cache = CacheFactory.getAnyInstance();
      SystemManagementService service = (SystemManagementService) ManagementService.getExistingManagementService(cache);

      GatewayReceiverMXBean receieverBean = null;

      TabularResultData resultData = ResultBuilder.createTabularResultData();
      Set<DistributedMember> dsMembers = CliUtil.findAllMatchingMembers(onGroup, onMember);

      for (DistributedMember member : dsMembers) {
        ObjectName gatewayReceiverObjectName = MBeanJMXAdapter.getGatewayReceiverMBeanName(member);

        if (gatewayReceiverObjectName != null) {
          receieverBean = service.getMBeanProxy(gatewayReceiverObjectName, GatewayReceiverMXBean.class);
          if (receieverBean != null) {
            if (receieverBean.isRunning()) {
              accumulateStartResult(resultData, member.getId(), CliStrings.GATEWAY_ERROR, CliStrings.format(
                  CliStrings.GATEWAY_RECEIVER_IS_ALREADY_STARTED_ON_MEMBER_0, new Object[] { member.getId() }));
            } else {
              receieverBean.start();
              accumulateStartResult(resultData, member.getId(), CliStrings.GATEWAY_OK, CliStrings.format(
                  CliStrings.GATEWAY_RECEIVER_IS_STARTED_ON_MEMBER_0, new Object[] { member.getId() }));
            }
          } else {
            accumulateStartResult(resultData, member.getId(), CliStrings.GATEWAY_ERROR, CliStrings.format(
                CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_ON_MEMBER_0, new Object[] { member.getId() }));
          }
        } else {
          accumulateStartResult(resultData, member.getId(), CliStrings.GATEWAY_ERROR, CliStrings.format(
              CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_ON_MEMBER_0, new Object[] { member.getId() }));
        }
      }
      result = ResultBuilder.buildResult(resultData);
    }
    catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    }
    catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = CliStrings.STOP_GATEWAYRECEIVER, help = CliStrings.STOP_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result stopGatewayReceiver(

      @CliOption(key = CliStrings.STOP_GATEWAYRECEIVER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.STOP_GATEWAYRECEIVER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String onGroup,

      @CliOption(key = CliStrings.STOP_GATEWAYRECEIVER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      help = CliStrings.STOP_GATEWAYRECEIVER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember) {

    Result result = null;

    try {
      Cache cache = CacheFactory.getAnyInstance();
      SystemManagementService service = (SystemManagementService) ManagementService
          .getExistingManagementService(cache);


      GatewayReceiverMXBean receieverBean = null;


      TabularResultData resultData = ResultBuilder.createTabularResultData();
      Set<DistributedMember> dsMembers = CliUtil.findAllMatchingMembers(onGroup, onMember);

      for (DistributedMember member : dsMembers) {
        ObjectName gatewayReceiverObjectName = MBeanJMXAdapter
            .getGatewayReceiverMBeanName(member);

        if (gatewayReceiverObjectName != null) {
          receieverBean = service.getMBeanProxy(gatewayReceiverObjectName,
              GatewayReceiverMXBean.class);
          if (receieverBean != null) {
            if (receieverBean.isRunning()) {
              receieverBean.stop();
              accumulateStartResult(resultData, member.getId(),
                  CliStrings.GATEWAY_OK, CliStrings.format(
                      CliStrings.GATEWAY_RECEIVER_IS_STOPPED_ON_MEMBER_0,
                      new Object[] { member.getId() }));
            } else {
              accumulateStartResult(resultData, member.getId(),
                  CliStrings.GATEWAY_ERROR, CliStrings.format(
                      CliStrings.GATEWAY_RECEIVER_IS_NOT_RUNNING_ON_MEMBER_0,
                      new Object[] { member.getId() }));
            }
          } else {
            accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_ERROR, CliStrings.format(
                    CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_ON_MEMBER_0,
                    new Object[] { member.getId() }));
          }
        } else {
          accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR, CliStrings.format(
                  CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_ON_MEMBER_0,
                  new Object[] { member.getId() }));
        }
      }
      result = ResultBuilder.buildResult(resultData);
    } catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = CliStrings.LIST_GATEWAY, help = CliStrings.LIST_GATEWAY__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listGateway(
      @CliOption(key = CliStrings.LIST_GATEWAY__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      help = CliStrings.LIST_GATEWAY__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember,
      @CliOption(key = CliStrings.LIST_GATEWAY__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.LIST_GATEWAY__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String onGroup) {

    Result result = null;
    Cache cache = CacheFactory.getAnyInstance();
    try {
      SystemManagementService service = (SystemManagementService) ManagementService
          .getExistingManagementService(cache);

      Set<DistributedMember> dsMembers = null;
//      if (onGroup != null && onGroup.length > 0) {
//        dsMembers = CliUtil.getDistributedMembersByGroup(cache, onGroup);
//      } else {
//        dsMembers = CliUtil.getAllNormalMembers(cache);
//      }
//      if (dsMembers.isEmpty()) {
//        return ResultBuilder
//            .createUserErrorResult(CliStrings.GATEWAY_MSG_MEMBERS_NOT_FOUND);
//      }
      dsMembers = CliUtil.findAllMatchingMembers(onGroup, onMember);

      Map<String, Map<String, GatewaySenderMXBean>> gatewaySenderBeans = new TreeMap<String, Map<String, GatewaySenderMXBean>>();
      Map<String, GatewayReceiverMXBean> gatewayReceiverBeans = new TreeMap<String, GatewayReceiverMXBean>();

      DistributedSystemMXBean dsMXBean = service.getDistributedSystemMXBean();
      for (DistributedMember member : dsMembers) {
        String memberName = member.getName();
        String memberNameOrId = (memberName != null  && !memberName.isEmpty())? memberName : member.getId();
        ObjectName gatewaySenderObjectNames[] = dsMXBean
            .listGatewaySenderObjectNames(memberNameOrId);
        // gateway senders : a member can have multiple gateway sendersdefined
        // on it
        if (gatewaySenderObjectNames != null) {
          for (ObjectName name : gatewaySenderObjectNames) {
            GatewaySenderMXBean senderBean = service.getMBeanProxy(name,
                GatewaySenderMXBean.class);
            if (senderBean != null) {
              if (gatewaySenderBeans.containsKey(senderBean.getSenderId())) {
                Map<String, GatewaySenderMXBean> memberToBeanMap = gatewaySenderBeans
                    .get(senderBean.getSenderId());
                memberToBeanMap.put(member.getId(), senderBean);
              } else {
                Map<String, GatewaySenderMXBean> memberToBeanMap = new TreeMap<String, GatewaySenderMXBean>();
                memberToBeanMap.put(member.getId(), senderBean);
                gatewaySenderBeans.put(senderBean.getSenderId(),
                    memberToBeanMap);
              }
            }
          }
        }
        // gateway receivers : a member can have only one gateway receiver
        ObjectName gatewayReceiverObjectName = MBeanJMXAdapter
            .getGatewayReceiverMBeanName(member);
        if (gatewayReceiverObjectName != null) {
          GatewayReceiverMXBean receieverBean = null;
          receieverBean = service.getMBeanProxy(gatewayReceiverObjectName,
              GatewayReceiverMXBean.class);
          if (receieverBean != null) {
            gatewayReceiverBeans.put(member.getId(), receieverBean);
          }
        }
      }
      if (gatewaySenderBeans.isEmpty() && gatewayReceiverBeans.isEmpty()) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.GATEWAYS_ARE_NOT_AVAILABLE_IN_CLUSTER);
      }
      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      crd.setHeader(CliStrings.HEADER_GATEWAYS);
      accumulateListGatewayResult(crd, gatewaySenderBeans, gatewayReceiverBeans);
      result = ResultBuilder.buildResult(crd);
    } catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = CliStrings.STATUS_GATEWAYSENDER, help = CliStrings.STATUS_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result statusGatewaySender(
      @CliOption(key = CliStrings.STATUS_GATEWAYSENDER__ID,
      mandatory = true,
      optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.STATUS_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = CliStrings.STATUS_GATEWAYSENDER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.STATUS_GATEWAYSENDER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String onGroup,

      @CliOption(key = CliStrings.STATUS_GATEWAYSENDER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      help = CliStrings.STATUS_GATEWAYSENDER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember) {

    Result result = null;
    if (senderId != null)
      senderId = senderId.trim();
    try {
      Cache cache = CacheFactory.getAnyInstance();
      SystemManagementService service = (SystemManagementService) ManagementService
          .getExistingManagementService(cache);

      GatewaySenderMXBean bean = null;


      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      TabularResultData availableSenderData = crd.addSection(
          CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE).addTable(
          CliStrings.TABLE_GATEWAY_SENDER);

      TabularResultData notAvailableSenderData = crd.addSection(
          CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE).addTable(
          CliStrings.TABLE_GATEWAY_SENDER);

      Set<DistributedMember> dsMembers = null;
      dsMembers = CliUtil.findAllMatchingMembers(onGroup, onMember);
      for (DistributedMember member : dsMembers) {
        if (cache.getDistributedSystem().getDistributedMember().getId().equals(
            member.getId())) {
          bean = service.getLocalGatewaySenderMXBean(senderId);
        } else {
          ObjectName objectName = service.getGatewaySenderMBeanName(member, senderId);
          bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
        }
        if (bean != null) {
          buildSenderStatus(member.getId(), bean, availableSenderData);
        } else {
          buildSenderStatus(member.getId(), bean, notAvailableSenderData);
        }
      }
      result = ResultBuilder.buildResult(crd);
    } catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = CliStrings.STATUS_GATEWAYRECEIVER, help = CliStrings.STATUS_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result statusGatewayReceiver(
      @CliOption(key = CliStrings.STATUS_GATEWAYRECEIVER__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.STATUS_GATEWAYRECEIVER__GROUP__HELP)
      @CliMetaData (valueSeparator = ",")  String onGroup,

      @CliOption(key = CliStrings.STATUS_GATEWAYRECEIVER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      help = CliStrings.STATUS_GATEWAYRECEIVER__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String onMember) {

    Result result = null;

    try {
      Cache cache = CacheFactory.getAnyInstance();
      SystemManagementService service = (SystemManagementService) ManagementService
          .getExistingManagementService(cache);


      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      TabularResultData availableReceiverData = crd.addSection(
          CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE).addTable(
          CliStrings.TABLE_GATEWAY_RECEIVER);

      TabularResultData notAvailableReceiverData = crd.addSection(
          CliStrings.SECTION_GATEWAY_RECEIVER_NOT_AVAILABLE).addTable(
          CliStrings.TABLE_GATEWAY_RECEIVER);

      Set<DistributedMember> dsMembers = CliUtil.findAllMatchingMembers(onGroup, onMember);

      for (DistributedMember member : dsMembers) {
        ObjectName gatewayReceiverObjectName = MBeanJMXAdapter
            .getGatewayReceiverMBeanName(member);
        if (gatewayReceiverObjectName != null) {
          GatewayReceiverMXBean receieverBean = service.getMBeanProxy(
              gatewayReceiverObjectName, GatewayReceiverMXBean.class);
          if (receieverBean != null) {
            buildReceiverStatus(member.getId(), receieverBean, availableReceiverData);
            continue;
          }
        }
        buildReceiverStatus(member.getId(), null, notAvailableReceiverData);
      }
      result = ResultBuilder.buildResult(crd);
    } catch (CommandResultException crex) {
      result = handleCommandResultException(crex);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + e.getMessage());
    }
    return result;
  }

  private TabularResultData buildReceiverStatus(String memberId,
      GatewayReceiverMXBean bean, TabularResultData resultData) {
    resultData.accumulate(CliStrings.RESULT_HOST_MEMBER, memberId);
    if (bean != null) {
      resultData.accumulate(CliStrings.RESULT_PORT, bean.getPort());
      resultData.accumulate(CliStrings.RESULT_STATUS,
          bean.isRunning() ? CliStrings.GATEWAY_RUNNING
              : CliStrings.GATEWAY_NOT_RUNNING);
    } else {
      resultData.accumulate(CliStrings.GATEWAY_ERROR,
          CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_OR_STOPPED);
    }
    return resultData;
  }

  private TabularResultData buildSenderStatus(String memberId,
      GatewaySenderMXBean bean, TabularResultData resultData) {
    resultData.accumulate(CliStrings.RESULT_HOST_MEMBER, memberId);
    if (bean != null) {
      resultData.accumulate(CliStrings.RESULT_TYPE,
          bean.isParallel() ? CliStrings.SENDER_PARALLEL
              : CliStrings.SENDER_SERIAL);
      if (!bean.isParallel()) {
        resultData.accumulate(CliStrings.RESULT_POLICY,
            bean.isPrimary() ? CliStrings.SENDER_PRIMARY
                : CliStrings.SENDER_SECONADRY);
      }
      if (bean.isRunning()) {
        if (bean.isPaused()) {
          resultData.accumulate(CliStrings.RESULT_STATUS,
              CliStrings.SENDER_PAUSED);
        } else {
          resultData.accumulate(CliStrings.RESULT_STATUS,
              CliStrings.GATEWAY_RUNNING);
        }
      } else {
        resultData.accumulate(CliStrings.RESULT_STATUS,
            CliStrings.GATEWAY_NOT_RUNNING);
      }
    } else {
      resultData.accumulate(CliStrings.GATEWAY_ERROR,
          CliStrings.GATEWAY_SENDER_IS_NOT_AVAILABLE);
    }

    return resultData;
  }

  // CliStrings.format(
  // CliStrings.GATEWAY_SENDER_0_IS_STARTED_ON_MEMBER_1,
  // new Object[] {senderId, memberId });
  //  
  // CliStrings.format(
  // CliStrings.GATEWAY_SENDER_0_IS_ALREADY_STARTED_ON_MEMBER_1,
  // new Object[] {senderId, memberId });
  //  
  // CliStrings.format(
  // CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
  // new Object[] {senderId, memberId });

  private void accumulateListGatewayResult(CompositeResultData crd,
      Map<String, Map<String, GatewaySenderMXBean>> gatewaySenderBeans,
      Map<String, GatewayReceiverMXBean> gatewayReceiverBeans) {

    if (!gatewaySenderBeans.isEmpty()) {
      TabularResultData gatewaySenderData = crd.addSection(
          CliStrings.SECTION_GATEWAY_SENDER).addTable(
          CliStrings.TABLE_GATEWAY_SENDER).setHeader(
          CliStrings.HEADER_GATEWAY_SENDER);
      for (Map.Entry<String, Map<String, GatewaySenderMXBean>> entry : gatewaySenderBeans
          .entrySet()) {
        for (Map.Entry<String, GatewaySenderMXBean> memberToBean : entry
            .getValue().entrySet()) {
          gatewaySenderData.accumulate(CliStrings.RESULT_GATEWAY_SENDER_ID,
              entry.getKey());
          gatewaySenderData.accumulate(CliStrings.RESULT_HOST_MEMBER,
              memberToBean.getKey());
          gatewaySenderData.accumulate(CliStrings.RESULT_REMOTE_CLUSTER,
              memberToBean.getValue().getRemoteDSId());
          gatewaySenderData.accumulate(CliStrings.RESULT_TYPE, memberToBean
              .getValue().isParallel() ? CliStrings.SENDER_PARALLEL
              : CliStrings.SENDER_SERIAL);
          gatewaySenderData.accumulate(CliStrings.RESULT_STATUS, memberToBean
              .getValue().isRunning() ? CliStrings.GATEWAY_RUNNING
              : CliStrings.GATEWAY_NOT_RUNNING);
          gatewaySenderData.accumulate(CliStrings.RESULT_QUEUED_EVENTS,
              memberToBean.getValue().getEventQueueSize());
          gatewaySenderData.accumulate(CliStrings.RESULT_RECEIVER, memberToBean
              .getValue().getGatewayReceiver());
        }
      }
    }

    if (!gatewayReceiverBeans.isEmpty()) {
      TabularResultData gatewayReceiverData = crd.addSection(
          CliStrings.SECTION_GATEWAY_RECEIVER).addTable(
          CliStrings.TABLE_GATEWAY_RECEIVER).setHeader(
          CliStrings.HEADER_GATEWAY_RECEIVER);
      for (Map.Entry<String, GatewayReceiverMXBean> entry : gatewayReceiverBeans
          .entrySet()) {
        gatewayReceiverData.accumulate(CliStrings.RESULT_HOST_MEMBER, entry
            .getKey());
        gatewayReceiverData.accumulate(CliStrings.RESULT_PORT, entry.getValue()
            .getPort());
        gatewayReceiverData.accumulate(CliStrings.RESULT_SENDERS_COUNT, entry
            .getValue().getClientConnectionCount());
        gatewayReceiverData.accumulate(CliStrings.RESULT_SENDER_CONNECTED,
            entry.getValue().getConnectedGatewaySenders());
      }
    }

  }

  private void accumulateStartResult(TabularResultData resultData,
      String member, String Status, String message) {
    if (member != null) {
      resultData.accumulate("Member", member);
    }
    resultData.accumulate("Result", Status);
    resultData.accumulate("Message", message);
  }

  @CliAvailabilityIndicator( { CliStrings.CREATE_GATEWAYSENDER,
      CliStrings.START_GATEWAYSENDER, CliStrings.PAUSE_GATEWAYSENDER,
      CliStrings.RESUME_GATEWAYSENDER, CliStrings.STOP_GATEWAYSENDER,
      CliStrings.CREATE_GATEWAYRECEIVER, CliStrings.START_GATEWAYRECEIVER,
      CliStrings.STOP_GATEWAYRECEIVER, CliStrings.LIST_GATEWAY,
      CliStrings.STATUS_GATEWAYSENDER, CliStrings.STATUS_GATEWAYRECEIVER,
      CliStrings.LOAD_BALANCE_GATEWAYSENDER })
  public boolean isWanCommandsAvailable() {
    boolean isAvailable = true; // always available on server
    if (CliUtil.isGfshVM()) {
      isAvailable = getGfsh() != null && getGfsh().isConnectedAndReady();
    }
    return isAvailable;
  }

  private Result handleCommandResultException(CommandResultException crex) {
    Result result = null;
    if (crex.getResult() != null) {
      result = crex.getResult();
    }
    else {
      LogWrapper.getInstance().warning(
          CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(crex));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR
          + crex.getMessage());
    }
    return result;
  }
}
