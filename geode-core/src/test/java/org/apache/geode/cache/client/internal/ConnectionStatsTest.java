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

  private final int destroyDurationId = ConnectionStats.getType().nameToId("destroyTime");
  private final int destroyInProgressId = ConnectionStats.getType().nameToId("destroysInProgress");
  private final int destroySendDurationId =
      ConnectionStats.getSendType().nameToId("destroySendTime");
  private final int destroySendInProgressId =
      ConnectionStats.getSendType().nameToId("destroySendsInProgress");

  private final int executeFunctionDurationId =
      ConnectionStats.getType().nameToId("executeFunctionTime");
  private final int executeFunctionInProgressId =
      ConnectionStats.getType().nameToId("executeFunctionsInProgress");
  private final int executeFunctionSendDurationId =
      ConnectionStats.getSendType().nameToId("executeFunctionSendTime");
  private final int executeFunctionSendInProgressId =
      ConnectionStats.getSendType().nameToId("executeFunctionSendsInProgress");

  private final int gatewayBatchDurationId = ConnectionStats.getType().nameToId("gatewayBatchTime");
  private final int gatewayBatchInProgressId =
      ConnectionStats.getType().nameToId("gatewayBatchsInProgress");
  private final int gatewayBatchSendDurationId =
      ConnectionStats.getSendType().nameToId("gatewayBatchSendTime");
  private final int gatewayBatchSendInProgressId =
      ConnectionStats.getSendType().nameToId("gatewayBatchSendsInProgress");

  private final int getAllDurationId = ConnectionStats.getType().nameToId("getAllTime");
  private final int getAllInProgressId = ConnectionStats.getType().nameToId("getAllsInProgress");
  private final int getAllSendDurationId = ConnectionStats.getSendType().nameToId("getAllSendTime");
  private final int getAllSendInProgressId =
      ConnectionStats.getSendType().nameToId("getAllSendsInProgress");

  private final int getClientPartitionAttributesDurationId =
      ConnectionStats.getType().nameToId("getClientPartitionAttributesTime");
  private final int getClientPartitionAttributesInProgressId =
      ConnectionStats.getType().nameToId("getClientPartitionAttributesInProgress");
  private final int getClientPartitionAttributesSendDurationId =
      ConnectionStats.getSendType().nameToId("getClientPartitionAttributesSendTime");
  private final int getClientPartitionAttributesSendInProgressId =
      ConnectionStats.getSendType().nameToId("getClientPartitionAttributesSendsInProgress");

  private final int getClientPRMetadataDurationId =
      ConnectionStats.getType().nameToId("getClientPRMetadataTime");
  private final int getClientPRMetadataInProgressId =
      ConnectionStats.getType().nameToId("getClientPRMetadataInProgress");
  private final int getClientPRMetadataSendDurationId =
      ConnectionStats.getSendType().nameToId("getClientPRMetadataSendTime");
  private final int getClientPRMetadataSendInProgressId =
      ConnectionStats.getSendType().nameToId("getClientPRMetadataSendsInProgress");

  private final int getPDXIdForTypeDurationId =
      ConnectionStats.getType().nameToId("getPDXIdForTypeTime");
  private final int getPDXIdForTypeInProgressId =
      ConnectionStats.getType().nameToId("getPDXIdForTypeInProgress");
  private final int getPDXIdForTypeSendDurationId =
      ConnectionStats.getSendType().nameToId("getPDXIdForTypeSendTime");
  private final int getPDXIdForTypeSendInProgressId =
      ConnectionStats.getSendType().nameToId("getPDXIdForTypeSendsInProgress");

  private final int getPDXTypeByIdDurationId =
      ConnectionStats.getType().nameToId("getPDXTypeByIdTime");
  private final int getPDXTypeByIdInProgressId =
      ConnectionStats.getType().nameToId("getPDXTypeByIdInProgress");
  private final int getPDXTypeByIdSendDurationId =
      ConnectionStats.getSendType().nameToId("getPDXTypeByIdSendTime");
  private final int getPDXTypeByIdSendInProgressId =
      ConnectionStats.getSendType().nameToId("getPDXTypeByIdSendsInProgress");

  private final int getEntryDurationId = ConnectionStats.getType().nameToId("getEntryTime");
  private final int getEntryInProgressId =
      ConnectionStats.getType().nameToId("getEntrysInProgress");
  private final int getEntrySendDurationId =
      ConnectionStats.getSendType().nameToId("getEntrySendTime");
  private final int getEntrySendInProgressId =
      ConnectionStats.getSendType().nameToId("getEntrySendsInProgress");

  private final int getDurableCQsDurationId =
      ConnectionStats.getType().nameToId("getDurableCQsTime");
  private final int getDurableCQsInProgressId =
      ConnectionStats.getType().nameToId("getDurableCQsInProgress");
  private final int getDurableCQsSendDurationId =
      ConnectionStats.getSendType().nameToId("getDurableCQsSendTime");
  private final int getDurableCQsSendInProgressId =
      ConnectionStats.getSendType().nameToId("getDurableCQsSendsInProgress");

  private final int getDurationId = ConnectionStats.getType().nameToId("getTime");
  private final int getInProgressId = ConnectionStats.getType().nameToId("getsInProgress");
  private final int getSendDurationId = ConnectionStats.getSendType().nameToId("getSendTime");
  private final int getSendInProgressId =
      ConnectionStats.getSendType().nameToId("getSendsInProgress");

  private final int invalidateDurationId = ConnectionStats.getType().nameToId("invalidateTime");
  private final int invalidateInProgressId =
      ConnectionStats.getType().nameToId("invalidatesInProgress");
  private final int invalidateSendDurationId =
      ConnectionStats.getSendType().nameToId("invalidateSendTime");
  private final int invalidateSendInProgressId =
      ConnectionStats.getSendType().nameToId("invalidateSendsInProgress");

  private final int jtaSynchronizationDurationId =
      ConnectionStats.getType().nameToId("jtaSynchronizationTime");
  private final int jtaSynchronizationInProgressId =
      ConnectionStats.getType().nameToId("jtaSynchronizationsInProgress");
  private final int jtaSynchronizationSendDurationId =
      ConnectionStats.getSendType().nameToId("jtaSynchronizationSendTime");
  private final int jtaSynchronizationSendInProgressId =
      ConnectionStats.getSendType().nameToId("jtaSynchronizationSendsInProgress");

  private final int keySetDurationId = ConnectionStats.getType().nameToId("keySetTime");
  private final int keySetInProgressId = ConnectionStats.getType().nameToId("keySetsInProgress");
  private final int keySetSendDurationId = ConnectionStats.getSendType().nameToId("keySetSendTime");
  private final int keySetSendInProgressId =
      ConnectionStats.getSendType().nameToId("keySetSendsInProgress");

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
  public void endDestroySend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("destroySendFailures");

    connectionStats.endDestroySend(1, true);

    verify(sendStats).incInt(eq(destroySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(destroySendDurationId), anyLong());
  }

  @Test
  public void endDestroySend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("destroySends");

    connectionStats.endDestroySend(1, false);

    verify(sendStats).incInt(eq(destroySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(destroySendDurationId), anyLong());
  }

  @Test
  public void endDestroy_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("destroyTimeouts");

    connectionStats.endDestroy(1, true, true);

    verify(stats).incInt(eq(destroyInProgressId), eq(-1));
    verify(stats).incLong(statId, 1);
    verify(stats).incLong(eq(destroyDurationId), anyLong());
  }

  @Test
  public void endDestroy_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("destroyTimeouts");

    connectionStats.endDestroy(1, true, false);

    verify(stats).incInt(eq(destroyInProgressId), eq(-1));
    verify(stats).incLong(statId, 1);
    verify(stats).incLong(eq(destroyDurationId), anyLong());
  }

  @Test
  public void endExecuteFunctionSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("executeFunctionSendFailures");

    connectionStats.endExecuteFunctionSend(1, true);

    verify(sendStats).incInt(eq(executeFunctionSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(executeFunctionSendDurationId), anyLong());
  }

  @Test
  public void endExecuteFunctionSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("executeFunctionSends");

    connectionStats.endExecuteFunctionSend(1, false);

    verify(sendStats).incInt(eq(executeFunctionSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(executeFunctionSendDurationId), anyLong());
  }

  @Test
  public void endExecuteFunction_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("executeFunctionTimeouts");

    connectionStats.endExecuteFunction(1, true, true);

    verify(stats).incInt(eq(executeFunctionInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(executeFunctionDurationId), anyLong());
  }

  @Test
  public void endExecuteFunction_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("executeFunctionTimeouts");

    connectionStats.endExecuteFunction(1, true, false);

    verify(stats).incInt(eq(executeFunctionInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(executeFunctionDurationId), anyLong());
  }

  @Test
  public void endExecuteFunction_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("executeFunctionFailures");

    connectionStats.endExecuteFunction(1, false, true);

    verify(stats).incInt(eq(executeFunctionInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(executeFunctionDurationId), anyLong());
  }

  @Test
  public void endExecuteFunction_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("executeFunctions");

    connectionStats.endExecuteFunction(1, false, false);

    verify(stats).incInt(eq(executeFunctionInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(executeFunctionDurationId), anyLong());
  }

  @Test
  public void startExecuteFunction() {
    int statId = ConnectionStats.getType().nameToId("executeFunctionsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("executeFunctionSendsInProgress");

    connectionStats.startExecuteFunction();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGatewayBatchSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("gatewayBatchSendFailures");

    connectionStats.endGatewayBatchSend(1, true);

    verify(sendStats).incInt(eq(gatewayBatchSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(gatewayBatchSendDurationId), anyLong());
  }

  @Test
  public void endGatewayBatchSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("gatewayBatchSends");

    connectionStats.endGatewayBatchSend(1, false);

    verify(sendStats).incInt(eq(gatewayBatchSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(gatewayBatchSendDurationId), anyLong());
  }

  @Test
  public void endGatewayBatch_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("gatewayBatchTimeouts");

    connectionStats.endGatewayBatch(1, true, true);

    verify(stats).incInt(eq(gatewayBatchInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(gatewayBatchDurationId), anyLong());
  }

  @Test
  public void endGatewayBatch_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("gatewayBatchTimeouts");

    connectionStats.endGatewayBatch(1, true, false);

    verify(stats).incInt(eq(gatewayBatchInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(gatewayBatchDurationId), anyLong());
  }

  @Test
  public void endGatewayBatch_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("gatewayBatchFailures");

    connectionStats.endGatewayBatch(1, false, true);

    verify(stats).incInt(eq(gatewayBatchInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(gatewayBatchDurationId), anyLong());
  }

  @Test
  public void endGatewayBatch_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("gatewayBatchs");

    connectionStats.endGatewayBatch(1, false, false);

    verify(stats).incInt(eq(gatewayBatchInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(gatewayBatchDurationId), anyLong());
  }

  @Test
  public void startGatewayBatch() {
    int statId = ConnectionStats.getType().nameToId("gatewayBatchsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("gatewayBatchSendsInProgress");

    connectionStats.startGatewayBatch();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGetAllSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getAllSendFailures");

    connectionStats.endGetAllSend(1, true);

    verify(sendStats).incInt(eq(getAllSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getAllSendDurationId), anyLong());
  }

  @Test
  public void endGetAllSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getAllSends");

    connectionStats.endGetAllSend(1, false);

    verify(sendStats).incInt(eq(getAllSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getAllSendDurationId), anyLong());
  }

  @Test
  public void endGetAll_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("getAllTimeouts");

    connectionStats.endGetAll(1, true, true);

    verify(stats).incInt(eq(getAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getAllDurationId), anyLong());
  }

  @Test
  public void endGetAll_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("getAllTimeouts");

    connectionStats.endGetAll(1, true, false);

    verify(stats).incInt(eq(getAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getAllDurationId), anyLong());
  }

  @Test
  public void endGetAll_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("getAllFailures");

    connectionStats.endGetAll(1, false, true);

    verify(stats).incInt(eq(getAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getAllDurationId), anyLong());
  }

  @Test
  public void endGetAll_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("getAlls");

    connectionStats.endGetAll(1, false, false);

    verify(stats).incInt(eq(getAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getAllDurationId), anyLong());
  }

  @Test
  public void startGetAll() {
    int statId = ConnectionStats.getType().nameToId("getAllsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("getAllSendsInProgress");

    connectionStats.startGetAll();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGetClientPartitionAttributesSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPartitionAttributesSendFailures");

    connectionStats.endGetClientPartitionAttributesSend(1, true);

    verify(sendStats).incInt(eq(getClientPartitionAttributesSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getClientPartitionAttributesSendDurationId), anyLong());
  }

  @Test
  public void endGetClientPartitionAttributesSend_SuccessfulOperation() {
    int statId =
        ConnectionStats.getSendType().nameToId("getClientPartitionAttributesSendsSuccessful");

    connectionStats.endGetClientPartitionAttributesSend(1, false);

    verify(sendStats).incInt(eq(getClientPartitionAttributesSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getClientPartitionAttributesSendDurationId), anyLong());
  }

  @Test
  public void endGetClientPartitionAttributes_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("getClientPartitionAttributesTimeouts");

    connectionStats.endGetClientPartitionAttributes(1, true, true);

    verify(stats).incInt(eq(getClientPartitionAttributesInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getClientPartitionAttributesDurationId), anyLong());
  }

  @Test
  public void endGetClientPartitionAttributes_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("getClientPartitionAttributesTimeouts");

    connectionStats.endGetClientPartitionAttributes(1, true, false);

    verify(stats).incInt(eq(getClientPartitionAttributesInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getClientPartitionAttributesDurationId), anyLong());
  }

  @Test
  public void endGetClientPartitionAttributes_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("getClientPartitionAttributesFailures");

    connectionStats.endGetClientPartitionAttributes(1, false, true);

    verify(stats).incInt(eq(getClientPartitionAttributesInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getClientPartitionAttributesDurationId), anyLong());
  }

  @Test
  public void endGetClientPartitionAttributes_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("getClientPartitionAttributesSuccessful");

    connectionStats.endGetClientPartitionAttributes(1, false, false);

    verify(stats).incInt(eq(getClientPartitionAttributesInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getClientPartitionAttributesDurationId), anyLong());
  }

  @Test
  public void startGetClientPartitionAttributes() {
    int statId = ConnectionStats.getType().nameToId("getClientPartitionAttributesInProgress");
    int sendStatId =
        ConnectionStats.getSendType().nameToId("getClientPartitionAttributesSendsInProgress");

    connectionStats.startGetClientPartitionAttributes();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGetClientPRMetadataSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPRMetadataSendFailures");

    connectionStats.endGetClientPRMetadataSend(1, true);

    verify(sendStats).incInt(eq(getClientPRMetadataSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getClientPRMetadataSendDurationId), anyLong());
  }

  @Test
  public void endGetClientPRMetadataSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPRMetadataSendsSuccessful");

    connectionStats.endGetClientPRMetadataSend(1, false);

    verify(sendStats).incInt(eq(getClientPRMetadataSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getClientPRMetadataSendDurationId), anyLong());
  }

  @Test
  public void endGetClientPRMetadata_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("getClientPRMetadataTimeouts");

    connectionStats.endGetClientPRMetadata(1, true, true);

    verify(stats).incInt(eq(getClientPRMetadataInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getClientPRMetadataDurationId), anyLong());
  }

  @Test
  public void endGetClientPRMetadata_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("getClientPRMetadataTimeouts");

    connectionStats.endGetClientPRMetadata(1, true, false);

    verify(stats).incInt(eq(getClientPRMetadataInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getClientPRMetadataDurationId), anyLong());
  }

  @Test
  public void endGetClientPRMetadata_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("getClientPRMetadataFailures");

    connectionStats.endGetClientPRMetadata(1, false, true);

    verify(stats).incInt(eq(getClientPRMetadataInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getClientPRMetadataDurationId), anyLong());
  }

  @Test
  public void endGetClientPRMetadata_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("getClientPRMetadataSuccessful");

    connectionStats.endGetClientPRMetadata(1, false, false);

    verify(stats).incInt(eq(getClientPRMetadataInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getClientPRMetadataDurationId), anyLong());
  }

  @Test
  public void startGetClientPRMetadata() {
    int statId = ConnectionStats.getType().nameToId("getClientPRMetadataInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("getClientPRMetadataSendsInProgress");

    connectionStats.startGetClientPRMetadata();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGetPDXIdForTypeSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXIdForTypeSendFailures");

    connectionStats.endGetPDXIdForTypeSend(1, true);

    verify(sendStats).incInt(eq(getPDXIdForTypeSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getPDXIdForTypeSendDurationId), anyLong());
  }

  @Test
  public void endGetPDXIdForTypeSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXIdForTypeSendsSuccessful");

    connectionStats.endGetPDXIdForTypeSend(1, false);

    verify(sendStats).incInt(eq(getPDXIdForTypeSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getPDXIdForTypeSendDurationId), anyLong());
  }

  @Test
  public void endGetPDXIdForType_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("getPDXIdForTypeTimeouts");

    connectionStats.endGetPDXIdForType(1, true, true);

    verify(stats).incInt(eq(getPDXIdForTypeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getPDXIdForTypeDurationId), anyLong());
  }

  @Test
  public void endGetPDXIdForType_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("getPDXIdForTypeTimeouts");

    connectionStats.endGetPDXIdForType(1, true, false);

    verify(stats).incInt(eq(getPDXIdForTypeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getPDXIdForTypeDurationId), anyLong());
  }

  @Test
  public void endGetPDXIdForType_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("getPDXIdForTypeFailures");

    connectionStats.endGetPDXIdForType(1, false, true);

    verify(stats).incInt(eq(getPDXIdForTypeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getPDXIdForTypeDurationId), anyLong());
  }

  @Test
  public void endGetPDXIdForType_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("getPDXIdForTypeSuccessful");

    connectionStats.endGetPDXIdForType(1, false, false);

    verify(stats).incInt(eq(getPDXIdForTypeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getPDXIdForTypeDurationId), anyLong());
  }

  @Test
  public void startGetPDXIdForType() {
    int statId = ConnectionStats.getType().nameToId("getPDXIdForTypeInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("getPDXIdForTypeSendsInProgress");

    connectionStats.startGetPDXIdForType();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGetPDXTypeByIdSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXTypeByIdSendFailures");

    connectionStats.endGetPDXTypeByIdSend(1, true);

    verify(sendStats).incInt(eq(getPDXTypeByIdSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getPDXTypeByIdSendDurationId), anyLong());
  }

  @Test
  public void endGetPDXTypeByIdSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXTypeByIdSendsSuccessful");

    connectionStats.endGetPDXTypeByIdSend(1, false);

    verify(sendStats).incInt(eq(getPDXTypeByIdSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getPDXTypeByIdSendDurationId), anyLong());
  }

  @Test
  public void endGetPDXTypeById_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("getPDXTypeByIdTimeouts");

    connectionStats.endGetPDXTypeById(1, true, true);

    verify(stats).incInt(eq(getPDXTypeByIdInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getPDXTypeByIdDurationId), anyLong());
  }

  @Test
  public void endGetPDXTypeById_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("getPDXTypeByIdTimeouts");

    connectionStats.endGetPDXTypeById(1, true, false);

    verify(stats).incInt(eq(getPDXTypeByIdInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getPDXTypeByIdDurationId), anyLong());
  }

  @Test
  public void endGetPDXTypeById_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("getPDXTypeByIdFailures");

    connectionStats.endGetPDXTypeById(1, false, true);

    verify(stats).incInt(eq(getPDXTypeByIdInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getPDXTypeByIdDurationId), anyLong());
  }

  @Test
  public void endGetPDXTypeById_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("getPDXTypeByIdSuccessful");

    connectionStats.endGetPDXTypeById(1, false, false);

    verify(stats).incInt(eq(getPDXTypeByIdInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getPDXTypeByIdDurationId), anyLong());
  }

  @Test
  public void startGetPDXTypeById() {
    int statId = ConnectionStats.getType().nameToId("getPDXTypeByIdInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("getPDXTypeByIdSendsInProgress");

    connectionStats.startGetPDXTypeById();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGetEntrySend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getEntrySendFailures");

    connectionStats.endGetEntrySend(1, true);

    verify(sendStats).incInt(eq(getEntrySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getEntrySendDurationId), anyLong());
  }

  @Test
  public void endGetEntrySend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getEntrySends");

    connectionStats.endGetEntrySend(1, false);

    verify(sendStats).incInt(eq(getEntrySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getEntrySendDurationId), anyLong());
  }

  @Test
  public void endGetEntry_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("getEntryTimeouts");

    connectionStats.endGetEntry(1, true, true);

    verify(stats).incInt(eq(getEntryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getEntryDurationId), anyLong());
  }

  @Test
  public void endGetEntry_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("getEntryTimeouts");

    connectionStats.endGetEntry(1, true, false);

    verify(stats).incInt(eq(getEntryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getEntryDurationId), anyLong());
  }

  @Test
  public void endGetEntry_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("getEntryFailures");

    connectionStats.endGetEntry(1, false, true);

    verify(stats).incInt(eq(getEntryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getEntryDurationId), anyLong());
  }

  @Test
  public void endGetEntry_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("getEntrys");

    connectionStats.endGetEntry(1, false, false);

    verify(stats).incInt(eq(getEntryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getEntryDurationId), anyLong());
  }

  @Test
  public void startGetEntry() {
    int statId = ConnectionStats.getType().nameToId("getEntrysInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("getEntrySendsInProgress");

    connectionStats.startGetEntry();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGetDurableCQsSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getDurableCQsSendFailures");

    connectionStats.endGetDurableCQsSend(1, true);

    verify(sendStats).incInt(eq(getDurableCQsSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getDurableCQsSendDurationId), anyLong());
  }

  @Test
  public void endGetDurableCQsSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getDurableCQsSends");

    connectionStats.endGetDurableCQsSend(1, false);

    verify(sendStats).incInt(eq(getDurableCQsSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getDurableCQsSendDurationId), anyLong());
  }

  @Test
  public void endGetDurableCQs_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("getDurableCQsTimeouts");

    connectionStats.endGetDurableCQs(1, true, true);

    verify(stats).incInt(eq(getDurableCQsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getDurableCQsDurationId), anyLong());
  }

  @Test
  public void endGetDurableCQs_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("getDurableCQsTimeouts");

    connectionStats.endGetDurableCQs(1, true, false);

    verify(stats).incInt(eq(getDurableCQsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getDurableCQsDurationId), anyLong());
  }

  @Test
  public void endGetDurableCQs_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("getDurableCQsFailures");

    connectionStats.endGetDurableCQs(1, false, true);

    verify(stats).incInt(eq(getDurableCQsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getDurableCQsDurationId), anyLong());
  }

  @Test
  public void endGetDurableCQs_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("getDurableCQs");

    connectionStats.endGetDurableCQs(1, false, false);

    verify(stats).incInt(eq(getDurableCQsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(getDurableCQsDurationId), anyLong());
  }

  @Test
  public void startGetDurableCQs() {
    int statId = ConnectionStats.getType().nameToId("getDurableCQsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("getDurableCQsSendsInProgress");

    connectionStats.startGetDurableCQs();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endGetSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getSendFailures");

    connectionStats.endGetSend(1, true);

    verify(sendStats).incInt(eq(getSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getSendDurationId), anyLong());
  }

  @Test
  public void endGetSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("getSends");

    connectionStats.endGetSend(1, false);

    verify(sendStats).incInt(eq(getSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(getSendDurationId), anyLong());
  }

  @Test
  public void endGet_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("getTimeouts");

    connectionStats.endGet(1, true, true);

    verify(stats).incInt(eq(getInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(getDurationId), anyLong());
  }

  @Test
  public void endGet_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("getTimeouts");

    connectionStats.endGet(1, true, false);

    verify(stats).incInt(eq(getInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(getDurationId), anyLong());
  }

  @Test
  public void endGet_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("getFailures");

    connectionStats.endGet(1, false, true);

    verify(stats).incInt(eq(getInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(getDurationId), anyLong());
  }

  @Test
  public void endGet_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("gets");

    connectionStats.endGet(1, false, false);

    verify(stats).incInt(eq(getInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(getDurationId), anyLong());
  }

  @Test
  public void startGet() {
    int statId = ConnectionStats.getType().nameToId("getsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("getSendsInProgress");

    connectionStats.startGet();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endInvalidateSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("invalidateSendFailures");

    connectionStats.endInvalidateSend(1, true);

    verify(sendStats).incInt(eq(invalidateSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(invalidateSendDurationId), anyLong());
  }

  @Test
  public void endInvalidateSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("invalidateSends");

    connectionStats.endInvalidateSend(1, false);

    verify(sendStats).incInt(eq(invalidateSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(invalidateSendDurationId), anyLong());
  }

  @Test
  public void endInvalidate_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("invalidateTimeouts");

    connectionStats.endInvalidate(1, true, true);

    verify(stats).incInt(eq(invalidateInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(invalidateDurationId), anyLong());
  }

  @Test
  public void endInvalidate_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("invalidateTimeouts");

    connectionStats.endInvalidate(1, true, false);

    verify(stats).incInt(eq(invalidateInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(invalidateDurationId), anyLong());
  }

  @Test
  public void endInvalidate_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("invalidateFailures");

    connectionStats.endInvalidate(1, false, true);

    verify(stats).incInt(eq(invalidateInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(invalidateDurationId), anyLong());
  }

  @Test
  public void endInvalidate_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("invalidates");

    connectionStats.endInvalidate(1, false, false);

    verify(stats).incInt(eq(invalidateInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(invalidateDurationId), anyLong());
  }

  @Test
  public void startInvalidate() {
    int statId = ConnectionStats.getType().nameToId("invalidatesInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("invalidateSendsInProgress");

    connectionStats.startInvalidate();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endTxSynchronizationSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("jtaSynchronizationSendFailures");

    connectionStats.endTxSynchronizationSend(1, true);

    verify(sendStats).incInt(eq(jtaSynchronizationSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(jtaSynchronizationSendDurationId), anyLong());
  }

  @Test
  public void endTxSynchronizationSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("jtaSynchronizationSends");

    connectionStats.endTxSynchronizationSend(1, false);

    verify(sendStats).incInt(eq(jtaSynchronizationSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(jtaSynchronizationSendDurationId), anyLong());
  }

  @Test
  public void endTxSynchronization_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("jtaSynchronizationTimeouts");

    connectionStats.endTxSynchronization(1, true, true);

    verify(stats).incInt(eq(jtaSynchronizationInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(jtaSynchronizationDurationId), anyLong());
  }

  @Test
  public void endTxSynchronization_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("jtaSynchronizationTimeouts");

    connectionStats.endTxSynchronization(1, true, false);

    verify(stats).incInt(eq(jtaSynchronizationInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(jtaSynchronizationDurationId), anyLong());
  }

  @Test
  public void endTxSynchronization_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("jtaSynchronizationFailures");

    connectionStats.endTxSynchronization(1, false, true);

    verify(stats).incInt(eq(jtaSynchronizationInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(jtaSynchronizationDurationId), anyLong());
  }

  @Test
  public void endTxSynchronization_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("jtaSynchronizations");

    connectionStats.endTxSynchronization(1, false, false);

    verify(stats).incInt(eq(jtaSynchronizationInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(jtaSynchronizationDurationId), anyLong());
  }

  @Test
  public void startTxSynchronization() {
    int statId = ConnectionStats.getType().nameToId("jtaSynchronizationsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("jtaSynchronizationSendsInProgress");

    connectionStats.startTxSynchronization();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endKeySetSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("keySetSendFailures");

    connectionStats.endKeySetSend(1, true);

    verify(sendStats).incInt(eq(keySetSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(keySetSendDurationId), anyLong());
  }

  @Test
  public void endKeySetSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("keySetSends");

    connectionStats.endKeySetSend(1, false);

    verify(sendStats).incInt(eq(keySetSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(keySetSendDurationId), anyLong());
  }

  @Test
  public void endKeySet_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("keySetTimeouts");

    connectionStats.endKeySet(1, true, true);

    verify(stats).incInt(eq(keySetInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(keySetDurationId), anyLong());
  }

  @Test
  public void endKeySet_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("keySetTimeouts");

    connectionStats.endKeySet(1, true, false);

    verify(stats).incInt(eq(keySetInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(keySetDurationId), anyLong());
  }

  @Test
  public void endKeySet_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("keySetFailures");

    connectionStats.endKeySet(1, false, true);

    verify(stats).incInt(eq(keySetInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(keySetDurationId), anyLong());
  }

  @Test
  public void endKeySet_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("keySets");

    connectionStats.endKeySet(1, false, false);

    verify(stats).incInt(eq(keySetInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(keySetDurationId), anyLong());
  }

  @Test
  public void startKeySet() {
    int statId = ConnectionStats.getType().nameToId("keySetsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("keySetSendsInProgress");

    connectionStats.startKeySet();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
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
}
