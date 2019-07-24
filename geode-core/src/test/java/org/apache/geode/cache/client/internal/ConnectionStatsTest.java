/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.client.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.cache.PoolStats;

public class ConnectionStatsTest {
  private final Statistics stats = mock(Statistics.class);
  private final Statistics sendStats = mock(Statistics.class);
  private final StatisticsFactory statisticsFactory = createStatisticsFactory(sendStats);
  private final PoolStats poolStats = mock(PoolStats.class);
  private final ConnectionStats connectionStats =
      new ConnectionStats(statisticsFactory, "Client", "name", poolStats);

  private final int addPdxTypeDurationId = ConnectionStats.getType().nameToId("addPdxTypeTime");
  private final int addPdxTypeInProgressId =
      ConnectionStats.getType().nameToId("addPdxTypeInProgress");
  private final int addPdxTypeSendDurationId =
      ConnectionStats.getSendType().nameToId("addPdxTypeSendTime");
  private final int addPdxTypeSendInProgressId =
      ConnectionStats.getSendType().nameToId("addPdxTypeSendsInProgress");

  private final int clearDurationId = ConnectionStats.getType().nameToId("clearTime");
  private final int clearInProgressId = ConnectionStats.getType().nameToId("clearsInProgress");
  private final int clearSendDurationId = ConnectionStats.getSendType().nameToId("clearSendTime");
  private final int clearSendInProgressId =
      ConnectionStats.getSendType().nameToId("clearSendsInProgress");

  private final int closeConDurationId = ConnectionStats.getType().nameToId("closeConTime");
  private final int closeConInProgressId =
      ConnectionStats.getType().nameToId("closeConsInProgress");
  private final int closeConSendDurationId =
      ConnectionStats.getSendType().nameToId("closeConSendTime");
  private final int closeConSendInProgressId =
      ConnectionStats.getSendType().nameToId("closeConSendsInProgress");

  private final int closeCQDurationId = ConnectionStats.getType().nameToId("closeCQTime");
  private final int closeCQInProgressId =
      ConnectionStats.getType().nameToId("closeCQsInProgress");
  private final int closeCQSendDurationId =
      ConnectionStats.getSendType().nameToId("closeCQSendTime");
  private final int closeCQSendInProgressId =
      ConnectionStats.getSendType().nameToId("closeCQSendsInProgress");

  private final int createCQDurationId = ConnectionStats.getType().nameToId("createCQTime");
  private final int createCQInProgressId =
      ConnectionStats.getType().nameToId("createCQsInProgress");
  private final int createCQSendDurationId =
      ConnectionStats.getSendType().nameToId("createCQSendTime");
  private final int createCQSendInProgressId =
      ConnectionStats.getSendType().nameToId("createCQSendsInProgress");

  private final int commitDurationId = ConnectionStats.getType().nameToId("commitTime");
  private final int commitInProgressId =
      ConnectionStats.getType().nameToId("commitsInProgress");
  private final int commitSendDurationId =
      ConnectionStats.getSendType().nameToId("commitSendTime");
  private final int commitSendInProgressId =
      ConnectionStats.getSendType().nameToId("commitSendsInProgress");

  private final int containsKeyDurationId = ConnectionStats.getType().nameToId("containsKeyTime");
  private final int containsKeyInProgressId =
      ConnectionStats.getType().nameToId("containsKeysInProgress");
  private final int containsKeySendDurationId =
      ConnectionStats.getSendType().nameToId("containsKeySendTime");
  private final int containsKeySendInProgressId =
      ConnectionStats.getSendType().nameToId("containsKeySendsInProgress");

  private final int destroyRegionDurationId =
      ConnectionStats.getType().nameToId("destroyRegionTime");
  private final int destroyRegionInProgressId =
      ConnectionStats.getType().nameToId("destroyRegionsInProgress");
  private final int destroyRegionSendDurationId =
      ConnectionStats.getSendType().nameToId("destroyRegionSendTime");
  private final int destroyRegionSendInProgressId =
      ConnectionStats.getSendType().nameToId("destroyRegionSendsInProgress");

  private StatisticsFactory createStatisticsFactory(Statistics sendStats) {
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    when(statisticsFactory.createAtomicStatistics(any(), eq("ClientStats-name")))
        .thenReturn(stats);
    when(statisticsFactory.createAtomicStatistics(any(), eq("ClientSendStats-name")))
        .thenReturn(sendStats);
    return statisticsFactory;
  }

  @Test
  public void endAddPdxType_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("addPdxTypeTimeouts");

    connectionStats.endAddPdxType(1, true, true);

    verify(stats).incInt(eq(addPdxTypeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(addPdxTypeDurationId), anyLong());
  }

  @Test
  public void endAddPdxType_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("addPdxTypeTimeouts");

    connectionStats.endAddPdxType(1, true, false);

    verify(stats).incInt(eq(addPdxTypeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(addPdxTypeDurationId), anyLong());
  }

  @Test
  public void endAddPdxType_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("addPdxTypeFailures");

    connectionStats.endAddPdxType(1, false, true);

    verify(stats).incInt(eq(addPdxTypeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(addPdxTypeDurationId), anyLong());
  }

  @Test
  public void endAddPdxType_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("addPdxTypeSuccessful");

    connectionStats.endAddPdxType(1, false, false);

    verify(stats).incInt(eq(addPdxTypeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(addPdxTypeDurationId), anyLong());
  }

  @Test
  public void endAddPdxTypeSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("addPdxTypeSendFailures");

    connectionStats.endAddPdxTypeSend(1, true);

    verify(sendStats).incInt(eq(addPdxTypeSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(addPdxTypeSendDurationId), anyLong());
  }

  @Test
  public void endAddPdxTypeSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("addPdxTypeSendsSuccessful");

    connectionStats.endAddPdxTypeSend(1, false);

    verify(sendStats).incInt(eq(addPdxTypeSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(addPdxTypeSendDurationId), anyLong());
  }

  @Test
  public void startAddPdx() {
    int statId = ConnectionStats.getType().nameToId("addPdxTypeInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("addPdxTypeSendsInProgress");

    connectionStats.startAddPdxType();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endClearSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("clearSendFailures");

    connectionStats.endClearSend(1, true);

    verify(sendStats).incInt(eq(clearSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(clearSendDurationId), anyLong());
  }

  @Test
  public void endClearSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("clearSends");

    connectionStats.endClearSend(1, false);

    verify(sendStats).incInt(eq(clearSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(clearSendDurationId), anyLong());
  }

  @Test
  public void endClear_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("clearTimeouts");

    connectionStats.endClear(1, true, true);

    verify(stats).incInt(eq(clearInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(clearDurationId), anyLong());
  }

  @Test
  public void endClear_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("clearTimeouts");

    connectionStats.endClear(1, true, false);

    verify(stats).incInt(eq(clearInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(clearDurationId), anyLong());
  }

  @Test
  public void endClear_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("clearFailures");

    connectionStats.endClear(1, false, true);

    verify(stats).incInt(eq(clearInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(clearDurationId), anyLong());
  }

  @Test
  public void endClear_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("clears");

    connectionStats.endClear(1, false, false);

    verify(stats).incInt(eq(clearInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(clearDurationId), anyLong());
  }

  @Test
  public void startClear() {
    int statId = ConnectionStats.getType().nameToId("clearsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("clearSendsInProgress");

    connectionStats.startClear();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endCloseConSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("closeConSendFailures");

    connectionStats.endCloseConSend(1, true);

    verify(sendStats).incInt(eq(closeConSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(closeConSendDurationId), anyLong());
  }

  @Test
  public void endCloseConSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("closeConSends");

    connectionStats.endCloseConSend(1, false);

    verify(sendStats).incInt(eq(closeConSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(closeConSendDurationId), anyLong());
  }

  @Test
  public void endCloseCon_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("closeConTimeouts");

    connectionStats.endCloseCon(1, true, true);

    verify(stats).incInt(eq(closeConInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(closeConDurationId), anyLong());
  }

  @Test
  public void endCloseCon_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("closeConTimeouts");

    connectionStats.endCloseCon(1, true, false);

    verify(stats).incInt(eq(closeConInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(closeConDurationId), anyLong());
  }

  @Test
  public void endCloseCon_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("closeConFailures");

    connectionStats.endCloseCon(1, false, true);

    verify(stats).incInt(eq(closeConInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(closeConDurationId), anyLong());
  }

  @Test
  public void endCloseCon_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("closeCons");

    connectionStats.endCloseCon(1, false, false);

    verify(stats).incInt(eq(closeConInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(closeConDurationId), anyLong());
  }

  @Test
  public void startCloseCon() {
    int statId = ConnectionStats.getType().nameToId("closeConsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("closeConSendsInProgress");

    connectionStats.startCloseCon();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endCloseCQSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("closeCQSendFailures");

    connectionStats.endCloseCQSend(1, true);

    verify(sendStats).incInt(eq(closeCQSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(closeCQSendDurationId), anyLong());
  }

  @Test
  public void endCloseCQSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("closeCQSends");

    connectionStats.endCloseCQSend(1, false);

    verify(sendStats).incInt(eq(closeCQSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(closeCQSendDurationId), anyLong());
  }

  @Test
  public void endCloseCQ_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("closeCQTimeouts");

    connectionStats.endCloseCQ(1, true, true);

    verify(stats).incInt(eq(closeCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(closeCQDurationId), anyLong());
  }

  @Test
  public void endCloseCQ_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("closeCQTimeouts");

    connectionStats.endCloseCQ(1, true, false);

    verify(stats).incInt(eq(closeCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(closeCQDurationId), anyLong());
  }

  @Test
  public void endCloseCQ_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("closeCQFailures");

    connectionStats.endCloseCQ(1, false, true);

    verify(stats).incInt(eq(closeCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(closeCQDurationId), anyLong());
  }

  @Test
  public void endCloseCQ_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("closeCQs");

    connectionStats.endCloseCQ(1, false, false);

    verify(stats).incInt(eq(closeCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(closeCQDurationId), anyLong());
  }

  @Test
  public void startCloseCQ() {
    int statId = ConnectionStats.getType().nameToId("closeCQsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("closeCQSendsInProgress");

    connectionStats.startCloseCQ();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endCreateCQSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("createCQSendFailures");

    connectionStats.endCreateCQSend(1, true);

    verify(sendStats).incInt(eq(createCQSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(createCQSendDurationId), anyLong());
  }

  @Test
  public void endCreateCQSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("createCQSends");

    connectionStats.endCreateCQSend(1, false);

    verify(sendStats).incInt(eq(createCQSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(createCQSendDurationId), anyLong());
  }

  @Test
  public void endCreateCQ_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("createCQTimeouts");

    connectionStats.endCreateCQ(1, true, true);

    verify(stats).incInt(eq(createCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(createCQDurationId), anyLong());
  }

  @Test
  public void endCreateCQ_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("createCQTimeouts");

    connectionStats.endCreateCQ(1, true, false);

    verify(stats).incInt(eq(createCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(createCQDurationId), anyLong());
  }

  @Test
  public void endCreateCQ_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("createCQFailures");

    connectionStats.endCreateCQ(1, false, true);

    verify(stats).incInt(eq(createCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(createCQDurationId), anyLong());
  }

  @Test
  public void endCreateCQ_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("createCQs");

    connectionStats.endCreateCQ(1, false, false);

    verify(stats).incInt(eq(createCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(createCQDurationId), anyLong());
  }

  @Test
  public void startCreateCQ() {
    int statId = ConnectionStats.getType().nameToId("createCQsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("createCQSendsInProgress");

    connectionStats.startCreateCQ();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endCommitSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("commitSendFailures");

    connectionStats.endCommitSend(1, true);

    verify(sendStats).incInt(eq(commitSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(commitSendDurationId), anyLong());
  }

  @Test
  public void endCommitSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("commitSends");

    connectionStats.endCommitSend(1, false);

    verify(sendStats).incInt(eq(commitSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(commitSendDurationId), anyLong());
  }

  @Test
  public void endCommit_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("commitTimeouts");

    connectionStats.endCommit(1, true, true);

    verify(stats).incInt(eq(commitInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(commitDurationId), anyLong());
  }

  @Test
  public void endCommit_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("commitTimeouts");

    connectionStats.endCommit(1, true, false);

    verify(stats).incInt(eq(commitInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(commitDurationId), anyLong());
  }

  @Test
  public void endCommit_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("commitFailures");

    connectionStats.endCommit(1, false, true);

    verify(stats).incInt(eq(commitInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(commitDurationId), anyLong());
  }

  @Test
  public void endCommit_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("commits");

    connectionStats.endCommit(1, false, false);

    verify(stats).incInt(eq(commitInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(commitDurationId), anyLong());
  }

  @Test
  public void startCommit() {
    int statId = ConnectionStats.getType().nameToId("commitsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("commitSendsInProgress");

    connectionStats.startCommit();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endContainsKeySend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("containsKeySendFailures");

    connectionStats.endContainsKeySend(1, true);

    verify(sendStats).incInt(eq(containsKeySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(containsKeySendDurationId), anyLong());
  }

  @Test
  public void endContainsKeySend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("containsKeySends");

    connectionStats.endContainsKeySend(1, false);

    verify(sendStats).incInt(eq(containsKeySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(containsKeySendDurationId), anyLong());
  }

  @Test
  public void endContainsKey_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("containsKeyTimeouts");

    connectionStats.endContainsKey(1, true, true);

    verify(stats).incInt(eq(containsKeyInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(containsKeyDurationId), anyLong());
  }

  @Test
  public void endContainsKey_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("containsKeyTimeouts");

    connectionStats.endContainsKey(1, true, false);

    verify(stats).incInt(eq(containsKeyInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(containsKeyDurationId), anyLong());
  }

  @Test
  public void endContainsKey_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("containsKeyFailures");

    connectionStats.endContainsKey(1, false, true);

    verify(stats).incInt(eq(containsKeyInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(containsKeyDurationId), anyLong());
  }

  @Test
  public void endContainsKey_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("containsKeys");

    connectionStats.endContainsKey(1, false, false);

    verify(stats).incInt(eq(containsKeyInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(containsKeyDurationId), anyLong());
  }

  @Test
  public void startContainsKey() {
    int statId = ConnectionStats.getType().nameToId("containsKeysInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("containsKeySendsInProgress");

    connectionStats.startContainsKey();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endDestroyRegionSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("destroyRegionSendFailures");

    connectionStats.endDestroyRegionSend(1, true);

    verify(sendStats).incInt(eq(destroyRegionSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(destroyRegionSendDurationId), anyLong());
  }

  @Test
  public void endDestroyRegionSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("destroyRegionSends");

    connectionStats.endDestroyRegionSend(1, false);

    verify(sendStats).incInt(eq(destroyRegionSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(destroyRegionSendDurationId), anyLong());
  }

  @Test
  public void endDestroyRegion_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("destroyRegionTimeouts");

    connectionStats.endDestroyRegion(1, true, true);

    verify(stats).incInt(eq(destroyRegionInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(destroyRegionDurationId), anyLong());
  }

  @Test
  public void endDestroyRegion_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("destroyRegionTimeouts");

    connectionStats.endDestroyRegion(1, true, false);

    verify(stats).incInt(eq(destroyRegionInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(destroyRegionDurationId), anyLong());
  }

  @Test
  public void endDestroyRegion_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("destroyRegionFailures");

    connectionStats.endDestroyRegion(1, false, true);

    verify(stats).incInt(eq(destroyRegionInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(destroyRegionDurationId), anyLong());
  }

  @Test
  public void endDestroyRegion_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("destroyRegions");

    connectionStats.endDestroyRegion(1, false, false);

    verify(stats).incInt(eq(destroyRegionInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(destroyRegionDurationId), anyLong());
  }

  @Test
  public void startDestroyRegion() {
    int statId = ConnectionStats.getType().nameToId("destroyRegionsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("destroyRegionSendsInProgress");

    connectionStats.startDestroyRegion();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGetSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getSendTime");

    connectionStats.endGetSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getSends");

    connectionStats.endGetSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getSendFailures");

    connectionStats.endGetSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endPutSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("putSendTime");

    connectionStats.endPutSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endPutSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("putSends");

    connectionStats.endPutSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endPutSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("putSendFailures");

    connectionStats.endPutSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endDestroySendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("destroySendTime");

    connectionStats.endDestroySend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endDestroySendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("destroySends");

    connectionStats.endDestroySend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endDestroySendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("destroySendFailures");

    connectionStats.endDestroySend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endKeySetSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("keySetSendTime");

    connectionStats.endKeySetSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endKeySetSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("keySetSends");

    connectionStats.endKeySetSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endKeySetSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("keySetSendFailures");

    connectionStats.endKeySetSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRegisterInterestSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("registerInterestSendTime");

    connectionStats.endRegisterInterestSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRegisterInterestSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("registerInterestSends");

    connectionStats.endRegisterInterestSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRegisterInterestSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("registerInterestSendFailures");

    connectionStats.endRegisterInterestSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endUnregisterInterestSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("unregisterInterestSendTime");

    connectionStats.endUnregisterInterestSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endUnregisterInterestSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("unregisterInterestSends");

    connectionStats.endUnregisterInterestSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endUnregisterInterestSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("unregisterInterestSendFailures");

    connectionStats.endUnregisterInterestSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endQuerySendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("querySendTime");

    connectionStats.endQuerySend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endQuerySendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("querySends");

    connectionStats.endQuerySend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endQuerySendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("querySendFailures");

    connectionStats.endQuerySend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endCreateCQSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("createCQSendTime");

    connectionStats.endCreateCQSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endCreateCQSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("createCQSends");

    connectionStats.endCreateCQSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endCreateCQSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("createCQSendFailures");

    connectionStats.endCreateCQSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endStopCQSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("stopCQSendTime");

    connectionStats.endStopCQSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endStopCQSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("stopCQSends");

    connectionStats.endStopCQSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endStopCQSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("stopCQSendFailures");

    connectionStats.endStopCQSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endCloseCQSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("closeCQSendTime");

    connectionStats.endCloseCQSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endCloseCQSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("closeCQSends");

    connectionStats.endCloseCQSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endCloseCQSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("closeCQSendFailures");

    connectionStats.endCloseCQSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetDurableCQsSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getDurableCQsSendTime");

    connectionStats.endGetDurableCQsSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetDurableCQsSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getDurableCQsSends");

    connectionStats.endGetDurableCQsSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetDurableCQsSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getDurableCQsSendFailures");

    connectionStats.endGetDurableCQsSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGatewayBatchSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("gatewayBatchSendTime");

    connectionStats.endGatewayBatchSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGatewayBatchSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("gatewayBatchSends");

    connectionStats.endGatewayBatchSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGatewayBatchSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("gatewayBatchSendFailures");

    connectionStats.endGatewayBatchSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endReadyForEventsSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("readyForEventsSendTime");

    connectionStats.endReadyForEventsSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endReadyForEventsSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("readyForEventsSends");

    connectionStats.endReadyForEventsSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endReadyForEventsSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("readyForEventsSendFailures");

    connectionStats.endReadyForEventsSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endMakePrimarySendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("makePrimarySendTime");

    connectionStats.endMakePrimarySend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endMakePrimarySendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("makePrimarySends");

    connectionStats.endMakePrimarySend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endMakePrimarySendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("makePrimarySendFailures");

    connectionStats.endMakePrimarySend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endPrimaryAckSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("primaryAckSendTime");

    connectionStats.endPrimaryAckSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endPrimaryAckSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("primaryAckSends");

    connectionStats.endPrimaryAckSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endPrimaryAckSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("primaryAckSendFailures");

    connectionStats.endPrimaryAckSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endPingSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("pingSendTime");

    connectionStats.endPingSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endPingSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("pingSends");

    connectionStats.endPingSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endPingSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("pingSendFailures");

    connectionStats.endPingSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRegisterInstantiatorsSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("registerInstantiatorsSendTime");

    connectionStats.endRegisterInstantiatorsSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRegisterInstantiatorsSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("registerInstantiatorsSends");

    connectionStats.endRegisterInstantiatorsSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRegisterInstantiatorsSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("registerInstantiatorsSendFailures");

    connectionStats.endRegisterInstantiatorsSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRegisterDataSerializersSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("registerDataSerializersSendTime");

    connectionStats.endRegisterDataSerializersSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRegisterDataSerializersSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("registerDataSerializersSends");

    connectionStats.endRegisterDataSerializersSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRegisterDataSerializersSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("registerDataSerializersSendFailures");

    connectionStats.endRegisterDataSerializersSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endPutAllSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("putAllSendTime");

    connectionStats.endPutAllSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endPutAllSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("putAllSends");

    connectionStats.endPutAllSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endPutAllSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("putAllSendFailures");

    connectionStats.endPutAllSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRemoveAllSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("removeAllSendTime");

    connectionStats.endRemoveAllSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRemoveAllSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("removeAllSends");

    connectionStats.endRemoveAllSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRemoveAllSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("removeAllSendFailures");

    connectionStats.endRemoveAllSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetAllSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getAllSendTime");

    connectionStats.endGetAllSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetAllSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getAllSends");

    connectionStats.endGetAllSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetAllSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getAllSendFailures");

    connectionStats.endGetAllSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endExecuteFunctionSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("executeFunctionSendTime");

    connectionStats.endExecuteFunctionSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endExecuteFunctionSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("executeFunctionSends");

    connectionStats.endExecuteFunctionSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endExecuteFunctionSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("executeFunctionSendFailures");

    connectionStats.endExecuteFunctionSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetClientPRMetadataSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPRMetadataSendTime");

    connectionStats.endGetClientPRMetadataSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetClientPRMetadataSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPRMetadataSendsSuccessful");

    connectionStats.endGetClientPRMetadataSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetClientPRMetadataSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPRMetadataSendFailures");

    connectionStats.endGetClientPRMetadataSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetClientPartitionAttributesSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPartitionAttributesSendTime");

    connectionStats.endGetClientPartitionAttributesSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetClientPartitionAttributesSendIncsSendStatsSuccessfulOpCount() {
    int statId =
        ConnectionStats.getSendType().nameToId("getClientPartitionAttributesSendsSuccessful");

    connectionStats.endGetClientPartitionAttributesSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetClientPartitionAttributesSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPartitionAttributesSendFailures");

    connectionStats.endGetClientPartitionAttributesSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetPDXTypeByIdSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXTypeByIdSendTime");

    connectionStats.endGetPDXTypeByIdSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetPDXTypeByIdSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXTypeByIdSendsSuccessful");

    connectionStats.endGetPDXTypeByIdSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetPDXTypeByIdSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXTypeByIdSendFailures");

    connectionStats.endGetPDXTypeByIdSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetPDXIdForTypeSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXIdForTypeSendTime");

    connectionStats.endGetPDXIdForTypeSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetPDXIdForTypeSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXIdForTypeSendsSuccessful");

    connectionStats.endGetPDXIdForTypeSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetPDXIdForTypeSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXIdForTypeSendFailures");

    connectionStats.endGetPDXIdForTypeSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endAddPdxTypeSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("addPdxTypeSendTime");

    connectionStats.endAddPdxTypeSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }



  @Test
  public void endSizeSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("sizeSendTime");

    connectionStats.endSizeSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endSizeSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("sizeSends");

    connectionStats.endSizeSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endSizeSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("sizeSendFailures");

    connectionStats.endSizeSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endInvalidateSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("invalidateSendTime");

    connectionStats.endInvalidateSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endInvalidateSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("invalidateSends");

    connectionStats.endInvalidateSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endInvalidateSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("invalidateSendFailures");

    connectionStats.endInvalidateSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetEntrySendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getEntrySendTime");

    connectionStats.endGetEntrySend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetEntrySendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getEntrySends");

    connectionStats.endGetEntrySend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endGetEntrySendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("getEntrySendFailures");

    connectionStats.endGetEntrySend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRollbackSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("rollbackSendTime");

    connectionStats.endRollbackSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRollbackSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("rollbackSends");

    connectionStats.endRollbackSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endRollbackSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("rollbackSendFailures");

    connectionStats.endRollbackSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endTxFailoverSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("txFailoverSendTime");

    connectionStats.endTxFailoverSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endTxFailoverSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("txFailoverSends");

    connectionStats.endTxFailoverSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endTxFailoverSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("txFailoverSendFailures");

    connectionStats.endTxFailoverSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endTxSynchronizationSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("jtaSynchronizationSendTime");

    connectionStats.endTxSynchronizationSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endTxSynchronizationSendIncsSendStatsSuccessfulOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("jtaSynchronizationSends");

    connectionStats.endTxSynchronizationSend(1, false);

    verify(sendStats).incInt(statId, 1);
  }

  @Test
  public void endTxSynchronizationSendIncsSendStatsFailureOpCount() {
    int statId = ConnectionStats.getSendType().nameToId("jtaSynchronizationSendFailures");

    connectionStats.endTxSynchronizationSend(1, true);

    verify(sendStats).incInt(statId, 1);
  }

}
