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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

public class StopGatewaySenderCommandDelegateParallelImplTest {
  private final String senderId = "sender1";
  private Cache cache;
  Set<DistributedMember> members;
  ExecutorService executorService;
  SystemManagementService managementService;
  StopGatewaySenderOnMemberFactory stopperOnMemberFactory;

  @Before
  public void setUp() {
    cache = mock(Cache.class);
    members =
        Stream.generate(() -> mock(DistributedMember.class)).limit(3)
            .collect(Collectors.toSet());

    executorService = mock(ExecutorService.class);
    managementService = mock(SystemManagementService.class);
    stopperOnMemberFactory = mock(StopGatewaySenderOnMemberFactory.class);
    doReturn(mock(StopGatewaySenderOnMember.class)).when(stopperOnMemberFactory).create();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void stopGatewaySenderStartsOneThreadPerMemberAndBuildsOutputAccordingToFuturesOutput()
      throws InterruptedException, ExecutionException {
    // arrange
    String gatewaySenderIsStoppedMsg = "GatewaySender ln is stopped on member";
    List<Future> futures = new ArrayList<>();
    for (int memberIndex = 0; memberIndex < members.size(); memberIndex++) {
      Future future = mock(Future.class);
      List<String> list = Arrays.asList("member" + memberIndex, "OK", gatewaySenderIsStoppedMsg);
      doReturn(list).when(future).get();
      futures.add(future);
    }
    doReturn(futures).when(executorService).invokeAll(any());
    StopGatewaySenderCommandDelegateParallelImpl command =
        new StopGatewaySenderCommandDelegateParallelImpl(executorService, managementService,
            stopperOnMemberFactory);

    // act
    ResultModel result = command.executeStopGatewaySender(senderId, cache, members);

    // assert
    assertThat(result.isSuccessful()).isTrue();

    TabularResultModel resultData = result.getTableSection(CliStrings.STOP_GATEWAYSENDER);
    List<String> member = resultData.getValuesInColumn("Member");
    assertThat(member).containsExactlyInAnyOrder("member0", "member1", "member2");
    List<String> status = resultData.getValuesInColumn("Result");
    assertThat(status).containsExactlyInAnyOrder("OK", "OK", "OK");
    List<String> message = resultData.getValuesInColumn("Message");
    assertThat(message).containsExactlyInAnyOrder(gatewaySenderIsStoppedMsg,
        gatewaySenderIsStoppedMsg, gatewaySenderIsStoppedMsg);

    ArgumentCaptor<Collection> callablesCaptor =
        ArgumentCaptor.forClass(Collection.class);
    verify(executorService, times(1)).invokeAll((callablesCaptor.capture()));
    assertThat(callablesCaptor.getValue().size()).isEqualTo(members.size());
    verify(executorService, times(1)).shutdown();
  }

  @Test
  public void stopGatewaySenderInterruptedReturnsError() throws InterruptedException {
    // arrange
    InterruptedException exception = new InterruptedException("interruption2");
    doThrow(exception).when(executorService).invokeAll(any());
    StopGatewaySenderCommandDelegateParallelImpl command =
        new StopGatewaySenderCommandDelegateParallelImpl(executorService, managementService,
            stopperOnMemberFactory);

    // act
    ResultModel result = command.executeStopGatewaySender(senderId, cache, members);

    // assert
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getInfoSection("info").getContent().get(0)).isEqualTo(
        "Could not invoke stop gateway sender sender1 operation on members due to interruption2");
  }
}
