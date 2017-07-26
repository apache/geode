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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class StartGatewaySenderCommand implements GfshCommand {

  @CliCommand(value = CliStrings.START_GATEWAYSENDER, help = CliStrings.START_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public Result startGatewaySender(@CliOption(key = CliStrings.START_GATEWAYSENDER__ID,
      mandatory = true, optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.START_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.START_GATEWAYSENDER__GROUP__HELP) String[] onGroup,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.START_GATEWAYSENDER__MEMBER__HELP) String[] onMember) {

    Result result;
    final String id = senderId.trim();

    try {
      final InternalCache cache = getCache();
      final SystemManagementService service =
          (SystemManagementService) ManagementService.getExistingManagementService(cache);

      TabularResultData resultData = ResultBuilder.createTabularResultData();

      Set<DistributedMember> dsMembers = CliUtil.findMembers(onGroup, onMember);

      if (dsMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ExecutorService execService = Executors.newCachedThreadPool(new ThreadFactory() {
        AtomicInteger threadNum = new AtomicInteger();

        public Thread newThread(final Runnable r) {
          Thread result =
              new Thread(r, "Start Sender Command Thread " + threadNum.incrementAndGet());
          result.setDaemon(true);
          return result;
        }
      });

      List<Callable<List>> callables = new ArrayList<>();

      for (final DistributedMember member : dsMembers) {

        callables.add(() -> {

          GatewaySenderMXBean bean;
          ArrayList<String> statusList = new ArrayList<>();
          if (cache.getDistributedSystem().getDistributedMember().getId().equals(member.getId())) {
            bean = service.getLocalGatewaySenderMXBean(id);
          } else {
            ObjectName objectName = service.getGatewaySenderMBeanName(member, id);
            bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
          }
          if (bean != null) {
            if (bean.isRunning()) {
              statusList.add(member.getId());
              statusList.add(CliStrings.GATEWAY_ERROR);
              statusList.add(CliStrings.format(
                  CliStrings.GATEWAY_SENDER_0_IS_ALREADY_STARTED_ON_MEMBER_1, id, member.getId()));
            } else {
              bean.start();
              statusList.add(member.getId());
              statusList.add(CliStrings.GATEWAY_OK);
              statusList.add(CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_STARTED_ON_MEMBER_1,
                  id, member.getId()));
            }
          } else {
            statusList.add(member.getId());
            statusList.add(CliStrings.GATEWAY_ERROR);
            statusList.add(CliStrings.format(
                CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1, id, member.getId()));
          }
          return statusList;

        });
      }

      Iterator<DistributedMember> memberIterator = dsMembers.iterator();
      List<Future<List>> futures = null;

      try {
        futures = execService.invokeAll(callables);
      } catch (InterruptedException ite) {
        GatewayCommandsUtils.accumulateStartResult(resultData, null, CliStrings.GATEWAY_ERROR,
            CliStrings.format(CliStrings.GATEWAY_SENDER_0_COULD_NOT_BE_INVOKED_DUE_TO_1, id,
                ite.getMessage()));
      }

      for (Future<List> future : futures) {
        DistributedMember member = memberIterator.next();
        List<String> memberStatus;
        try {
          memberStatus = future.get();
          GatewayCommandsUtils.accumulateStartResult(resultData, memberStatus.get(0),
              memberStatus.get(1), memberStatus.get(2));
        } catch (InterruptedException | ExecutionException ite) {
          GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR,
              CliStrings.format(CliStrings.GATEWAY_SENDER_0_COULD_NOT_BE_STARTED_ON_MEMBER_DUE_TO_1,
                  id, ite.getMessage()));
        }
      }
      execService.shutdown();
      result = ResultBuilder.buildResult(resultData);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR + e.getMessage());
    }
    return result;
  }
}
