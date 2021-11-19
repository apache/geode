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

import static org.apache.geode.logging.internal.executors.LoggingExecutors.newCachedThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.commands.StopGatewaySenderCommand.StopGatewaySenderCommandDelegate;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

class StopGatewaySenderCommandDelegateParallelImpl
    implements StopGatewaySenderCommandDelegate {

  private final ExecutorService executorService;
  private final SystemManagementService managementService;
  private final Supplier<StopGatewaySenderOnMember> stopperOnMemberFactory;

  StopGatewaySenderCommandDelegateParallelImpl(SystemManagementService managementService) {
    this(managementService,
        newCachedThreadPool("Stop Sender Command Thread ", true),
        StopGatewaySenderOnMemberWithBeanImpl::new);
  }

  StopGatewaySenderCommandDelegateParallelImpl(SystemManagementService managementService,
      ExecutorService executorService,
      Supplier<StopGatewaySenderOnMember> stopperOnMemberFactory) {
    this.managementService = managementService;
    this.executorService = executorService;
    this.stopperOnMemberFactory = stopperOnMemberFactory;
  }

  public ResultModel executeStopGatewaySender(String id, Cache cache,
      Set<DistributedMember> dsMembers) {
    List<DistributedMember> dsMembersList = new ArrayList<>(dsMembers);
    List<Callable<List<String>>> callables = new ArrayList<>();
    for (final DistributedMember member : dsMembersList) {
      callables.add(() -> stopperOnMemberFactory.get()
          .executeStopGatewaySenderOnMember(id,
              cache, managementService, member));
    }

    List<Future<List<String>>> futures;
    try {
      futures = executorService.invokeAll(callables);
    } catch (InterruptedException ite) {
      Thread.currentThread().interrupt();
      return ResultModel.createError(
          CliStrings.format(CliStrings.GATEWAY_SENDER_STOP_0_COULD_NOT_BE_INVOKED_DUE_TO_1, id,
              ite.getMessage()));
    } finally {
      executorService.shutdown();
    }

    return buildResultModelFromMembersResponses(id, dsMembersList, futures);
  }

  private ResultModel buildResultModelFromMembersResponses(String id,
      List<DistributedMember> dsMembers, List<Future<List<String>>> futures) {
    ResultModel resultModel = new ResultModel();
    TabularResultModel resultData = resultModel.addTable(CliStrings.STOP_GATEWAYSENDER);
    Iterator<DistributedMember> memberIterator = dsMembers.iterator();
    for (Future<List<String>> future : futures) {
      DistributedMember member = memberIterator.next();
      List<String> memberStatus;
      try {
        memberStatus = future.get();
        resultData.addMemberStatusResultRow(memberStatus.get(0),
            memberStatus.get(1), memberStatus.get(2));
      } catch (InterruptedException | ExecutionException ite) {
        resultData.addMemberStatusResultRow(member.getId(),
            CliStrings.GATEWAY_ERROR,
            CliStrings.format(CliStrings.GATEWAY_SENDER_0_COULD_NOT_BE_STOPPED_ON_MEMBER_DUE_TO_1,
                id, ite.getMessage()));
      }
    }
    return resultModel;
  }

  @FunctionalInterface
  interface StopGatewaySenderOnMember {
    List<String> executeStopGatewaySenderOnMember(String id, Cache cache,
        SystemManagementService managementService, DistributedMember member);

  }
}
