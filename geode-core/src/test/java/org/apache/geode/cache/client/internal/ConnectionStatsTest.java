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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

  private final int makePrimaryDurationId = ConnectionStats.getType().nameToId("makePrimaryTime");
  private final int makePrimaryInProgressId =
      ConnectionStats.getType().nameToId("makePrimarysInProgress");
  private final int makePrimarySendDurationId =
      ConnectionStats.getSendType().nameToId("makePrimarySendTime");
  private final int makePrimarySendInProgressId =
      ConnectionStats.getSendType().nameToId("makePrimarySendsInProgress");

  private final int pingDurationId = ConnectionStats.getType().nameToId("pingTime");
  private final int pingInProgressId = ConnectionStats.getType().nameToId("pingsInProgress");
  private final int pingSendDurationId = ConnectionStats.getSendType().nameToId("pingSendTime");
  private final int pingSendInProgressId =
      ConnectionStats.getSendType().nameToId("pingSendsInProgress");

  private final int primaryAckDurationId = ConnectionStats.getType().nameToId("primaryAckTime");
  private final int primaryAckInProgressId =
      ConnectionStats.getType().nameToId("primaryAcksInProgress");
  private final int primaryAckSendDurationId =
      ConnectionStats.getSendType().nameToId("primaryAckSendTime");
  private final int primaryAckSendInProgressId =
      ConnectionStats.getSendType().nameToId("primaryAckSendsInProgress");

  private final int putAllDurationId = ConnectionStats.getType().nameToId("putAllTime");
  private final int putAllInProgressId = ConnectionStats.getType().nameToId("putAllsInProgress");
  private final int putAllSendDurationId = ConnectionStats.getSendType().nameToId("putAllSendTime");
  private final int putAllSendInProgressId =
      ConnectionStats.getSendType().nameToId("putAllSendsInProgress");

  private final int putDurationId = ConnectionStats.getType().nameToId("putTime");
  private final int putInProgressId = ConnectionStats.getType().nameToId("putsInProgress");
  private final int putSendDurationId = ConnectionStats.getSendType().nameToId("putSendTime");
  private final int putSendInProgressId =
      ConnectionStats.getSendType().nameToId("putSendsInProgress");

  private final int queryDurationId = ConnectionStats.getType().nameToId("queryTime");
  private final int queryInProgressId = ConnectionStats.getType().nameToId("querysInProgress");
  private final int querySendDurationId = ConnectionStats.getSendType().nameToId("querySendTime");
  private final int querySendInProgressId =
      ConnectionStats.getSendType().nameToId("querySendsInProgress");

  private final int readyForEventsDurationId =
      ConnectionStats.getType().nameToId("readyForEventsTime");
  private final int readyForEventsInProgressId =
      ConnectionStats.getType().nameToId("readyForEventsInProgress");
  private final int readyForEventsSendDurationId =
      ConnectionStats.getSendType().nameToId("readyForEventsSendTime");
  private final int readyForEventsSendInProgressId =
      ConnectionStats.getSendType().nameToId("readyForEventsSendsInProgress");

  private final int registerDataSerializersDurationId =
      ConnectionStats.getType().nameToId("registerDataSerializersTime");
  private final int registerDataSerializersInProgressId =
      ConnectionStats.getType().nameToId("registerDataSerializersInProgress");
  private final int registerDataSerializersSendDurationId =
      ConnectionStats.getSendType().nameToId("registerDataSerializersSendTime");
  private final int registerDataSerializersSendInProgressId =
      ConnectionStats.getSendType().nameToId("registerDataSerializersSendInProgress");

  private final int registerInstantiatorsDurationId =
      ConnectionStats.getType().nameToId("registerInstantiatorsTime");
  private final int registerInstantiatorsInProgressId =
      ConnectionStats.getType().nameToId("registerInstantiatorsInProgress");
  private final int registerInstantiatorsSendDurationId =
      ConnectionStats.getSendType().nameToId("registerInstantiatorsSendTime");
  private final int registerInstantiatorsSendInProgressId =
      ConnectionStats.getSendType().nameToId("registerInstantiatorsSendsInProgress");

  private final int registerInterestDurationId =
      ConnectionStats.getType().nameToId("registerInterestTime");
  private final int registerInterestInProgressId =
      ConnectionStats.getType().nameToId("registerInterestsInProgress");
  private final int registerInterestSendDurationId =
      ConnectionStats.getSendType().nameToId("registerInterestSendTime");
  private final int registerInterestSendInProgressId =
      ConnectionStats.getSendType().nameToId("registerInterestSendsInProgress");

  private final int removeAllDurationId = ConnectionStats.getType().nameToId("removeAllTime");
  private final int removeAllInProgressId =
      ConnectionStats.getType().nameToId("removeAllsInProgress");
  private final int removeAllSendDurationId =
      ConnectionStats.getSendType().nameToId("removeAllSendTime");
  private final int removeAllSendInProgressId =
      ConnectionStats.getSendType().nameToId("removeAllSendsInProgress");

  private final int rollbackDurationId = ConnectionStats.getType().nameToId("rollbackTime");
  private final int rollbackInProgressId =
      ConnectionStats.getType().nameToId("rollbacksInProgress");
  private final int rollbackSendDurationId =
      ConnectionStats.getSendType().nameToId("rollbackSendTime");
  private final int rollbackSendInProgressId =
      ConnectionStats.getSendType().nameToId("rollbackSendsInProgress");

  private final int sizeDurationId = ConnectionStats.getType().nameToId("sizeTime");
  private final int sizeInProgressId = ConnectionStats.getType().nameToId("sizesInProgress");
  private final int sizeSendDurationId = ConnectionStats.getSendType().nameToId("sizeSendTime");
  private final int sizeSendInProgressId =
      ConnectionStats.getSendType().nameToId("sizeSendsInProgress");

  private final int stopCQDurationId = ConnectionStats.getType().nameToId("stopCQTime");
  private final int stopCQInProgressId = ConnectionStats.getType().nameToId("stopCQsInProgress");
  private final int stopCQSendDurationId = ConnectionStats.getSendType().nameToId("stopCQSendTime");
  private final int stopCQSendInProgressId =
      ConnectionStats.getSendType().nameToId("stopCQSendsInProgress");

  private final int txFailoverDurationId = ConnectionStats.getType().nameToId("txFailoverTime");
  private final int txFailoverInProgressId =
      ConnectionStats.getType().nameToId("txFailoversInProgress");
  private final int txFailoverSendDurationId =
      ConnectionStats.getSendType().nameToId("txFailoverSendTime");
  private final int txFailoverSendInProgressId =
      ConnectionStats.getSendType().nameToId("txFailoverSendsInProgress");

  private final int unregisterInterestDurationId =
      ConnectionStats.getType().nameToId("unregisterInterestTime");
  private final int unregisterInterestInProgressId =
      ConnectionStats.getType().nameToId("unregisterInterestsInProgress");
  private final int unregisterInterestSendDurationId =
      ConnectionStats.getSendType().nameToId("unregisterInterestSendTime");
  private final int unregisterInterestSendInProgressId =
      ConnectionStats.getSendType().nameToId("unregisterInterestSendsInProgress");

  private StatisticsFactory createStatisticsFactory(Statistics sendStats) {
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    when(statisticsFactory.createAtomicStatistics(any(), eq("ClientStats-name")))
        .thenReturn(stats);
    when(statisticsFactory.createAtomicStatistics(any(), eq("ClientSendStats-name")))
        .thenReturn(sendStats);
    return statisticsFactory;
  }

  @Test
  public void close() {
    connectionStats.close();
    verify(stats).close();
    verify(sendStats).close();
  }

  @Test
  public void incMessagesBeingReceived() {
    int messagesStatId = ConnectionStats.getType().nameToId("messagesBeingReceived");
    int messagesBytesStatId = ConnectionStats.getType().nameToId("messageBytesBeingReceived");
    connectionStats.incMessagesBeingReceived(10);
    verify(stats).incInt(messagesStatId, 1);
    verify(stats).incLong(messagesBytesStatId, 10);
  }

  @Test
  public void incMessagesBeingReceived_messageSizeIsZero() {
    int messagesStatId = ConnectionStats.getType().nameToId("messagesBeingReceived");
    int messagesBytesStatId = ConnectionStats.getType().nameToId("messageBytesBeingReceived");
    connectionStats.incMessagesBeingReceived(0);
    verify(stats).incInt(messagesStatId, 1);
    verify(stats, never()).incLong(eq(messagesBytesStatId), anyInt());
  }

  @Test
  public void decMessagesBeingReceived() {
    int messagesStatId = ConnectionStats.getType().nameToId("messagesBeingReceived");
    int messagesBytesStatId = ConnectionStats.getType().nameToId("messageBytesBeingReceived");
    connectionStats.decMessagesBeingReceived(10);
    verify(stats).incInt(messagesStatId, -1);
    verify(stats).incLong(messagesBytesStatId, -10);
  }

  @Test
  public void decMessagesBeingReceived_messageSizeIsZero() {
    int messagesStatId = ConnectionStats.getType().nameToId("messagesBeingReceived");
    int messagesBytesStatId = ConnectionStats.getType().nameToId("messageBytesBeingReceived");
    connectionStats.decMessagesBeingReceived(0);
    verify(stats).incInt(messagesStatId, -1);
    verify(stats, never()).incLong(eq(messagesBytesStatId), anyInt());
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
  public void endMakePrimarySend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("makePrimarySendFailures");

    connectionStats.endMakePrimarySend(1, true);

    verify(sendStats).incInt(eq(makePrimarySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(makePrimarySendDurationId), anyLong());
  }

  @Test
  public void endMakePrimarySend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("makePrimarySends");

    connectionStats.endMakePrimarySend(1, false);

    verify(sendStats).incInt(eq(makePrimarySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(makePrimarySendDurationId), anyLong());
  }

  @Test
  public void endMakePrimary_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("makePrimaryTimeouts");

    connectionStats.endMakePrimary(1, true, true);

    verify(stats).incInt(eq(makePrimaryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(makePrimaryDurationId), anyLong());
  }

  @Test
  public void endMakePrimary_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("makePrimaryTimeouts");

    connectionStats.endMakePrimary(1, true, false);

    verify(stats).incInt(eq(makePrimaryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(makePrimaryDurationId), anyLong());
  }

  @Test
  public void endMakePrimary_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("makePrimaryFailures");

    connectionStats.endMakePrimary(1, false, true);

    verify(stats).incInt(eq(makePrimaryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(makePrimaryDurationId), anyLong());
  }

  @Test
  public void endMakePrimary_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("makePrimarys");

    connectionStats.endMakePrimary(1, false, false);

    verify(stats).incInt(eq(makePrimaryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(makePrimaryDurationId), anyLong());
  }

  @Test
  public void startMakePrimary() {
    int statId = ConnectionStats.getType().nameToId("makePrimarysInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("makePrimarySendsInProgress");

    connectionStats.startMakePrimary();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endPingSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("pingSendFailures");

    connectionStats.endPingSend(1, true);

    verify(sendStats).incInt(eq(pingSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(pingSendDurationId), anyLong());
  }

  @Test
  public void endPingSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("pingSends");

    connectionStats.endPingSend(1, false);

    verify(sendStats).incInt(eq(pingSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(pingSendDurationId), anyLong());
  }

  @Test
  public void endPing_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("pingTimeouts");

    connectionStats.endPing(1, true, true);

    verify(stats).incInt(eq(pingInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(pingDurationId), anyLong());
  }

  @Test
  public void endPing_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("pingTimeouts");

    connectionStats.endPing(1, true, false);

    verify(stats).incInt(eq(pingInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(pingDurationId), anyLong());
  }

  @Test
  public void endPing_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("pingFailures");

    connectionStats.endPing(1, false, true);

    verify(stats).incInt(eq(pingInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(pingDurationId), anyLong());
  }

  @Test
  public void endPing_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("pings");

    connectionStats.endPing(1, false, false);

    verify(stats).incInt(eq(pingInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(pingDurationId), anyLong());
  }

  @Test
  public void startPing() {
    int statId = ConnectionStats.getType().nameToId("pingsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("pingSendsInProgress");

    connectionStats.startPing();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endPrimaryAckSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("primaryAckSendFailures");

    connectionStats.endPrimaryAckSend(1, true);

    verify(sendStats).incInt(eq(primaryAckSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(primaryAckSendDurationId), anyLong());
  }

  @Test
  public void endPrimaryAckSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("primaryAckSends");

    connectionStats.endPrimaryAckSend(1, false);

    verify(sendStats).incInt(eq(primaryAckSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(primaryAckSendDurationId), anyLong());
  }

  @Test
  public void endPrimaryAck_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("primaryAckTimeouts");

    connectionStats.endPrimaryAck(1, true, true);

    verify(stats).incInt(eq(primaryAckInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(primaryAckDurationId), anyLong());
  }

  @Test
  public void endPrimaryAck_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("primaryAckTimeouts");

    connectionStats.endPrimaryAck(1, true, false);

    verify(stats).incInt(eq(primaryAckInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(primaryAckDurationId), anyLong());
  }

  @Test
  public void endPrimaryAck_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("primaryAckFailures");

    connectionStats.endPrimaryAck(1, false, true);

    verify(stats).incInt(eq(primaryAckInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(primaryAckDurationId), anyLong());
  }

  @Test
  public void endPrimaryAck_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("primaryAcks");

    connectionStats.endPrimaryAck(1, false, false);

    verify(stats).incInt(eq(primaryAckInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(primaryAckDurationId), anyLong());
  }

  @Test
  public void startPrimaryAck() {
    int statId = ConnectionStats.getType().nameToId("primaryAcksInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("primaryAckSendsInProgress");

    connectionStats.startPrimaryAck();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endPutAllSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("putAllSendFailures");

    connectionStats.endPutAllSend(1, true);

    verify(sendStats).incInt(eq(putAllSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(putAllSendDurationId), anyLong());
  }

  @Test
  public void endPutAllSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("putAllSends");

    connectionStats.endPutAllSend(1, false);

    verify(sendStats).incInt(eq(putAllSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(putAllSendDurationId), anyLong());
  }

  @Test
  public void endPutAll_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("putAllTimeouts");

    connectionStats.endPutAll(1, true, true);

    verify(stats).incInt(eq(putAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(putAllDurationId), anyLong());
  }

  @Test
  public void endPutAll_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("putAllTimeouts");

    connectionStats.endPutAll(1, true, false);

    verify(stats).incInt(eq(putAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(putAllDurationId), anyLong());
  }

  @Test
  public void endPutAll_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("putAllFailures");

    connectionStats.endPutAll(1, false, true);

    verify(stats).incInt(eq(putAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(putAllDurationId), anyLong());
  }

  @Test
  public void endPutAll_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("putAlls");

    connectionStats.endPutAll(1, false, false);

    verify(stats).incInt(eq(putAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(putAllDurationId), anyLong());
  }

  @Test
  public void startPutAll() {
    int statId = ConnectionStats.getType().nameToId("putAllsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("putAllSendsInProgress");

    connectionStats.startPutAll();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endPutSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("putSendFailures");

    connectionStats.endPutSend(1, true);

    verify(sendStats).incInt(eq(putSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(putSendDurationId), anyLong());
  }

  @Test
  public void endPutSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("putSends");

    connectionStats.endPutSend(1, false);

    verify(sendStats).incInt(eq(putSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(putSendDurationId), anyLong());
  }

  @Test
  public void endPut_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("putTimeouts");

    connectionStats.endPut(1, true, true);

    verify(stats).incInt(eq(putInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(putDurationId), anyLong());
  }

  @Test
  public void endPut_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("putTimeouts");

    connectionStats.endPut(1, true, false);

    verify(stats).incInt(eq(putInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(putDurationId), anyLong());
  }

  @Test
  public void endPut_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("putFailures");

    connectionStats.endPut(1, false, true);

    verify(stats).incInt(eq(putInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(putDurationId), anyLong());
  }

  @Test
  public void endPut_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("puts");

    connectionStats.endPut(1, false, false);

    verify(stats).incInt(eq(putInProgressId), eq(-1));
    verify(stats).incLong(statId, 1L);
    verify(stats).incLong(eq(putDurationId), anyLong());
  }

  @Test
  public void startPut() {
    int statId = ConnectionStats.getType().nameToId("putsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("putSendsInProgress");

    connectionStats.startPut();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endQuerySend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("querySendFailures");

    connectionStats.endQuerySend(1, true);

    verify(sendStats).incInt(eq(querySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(querySendDurationId), anyLong());
  }

  @Test
  public void endQuerySend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("querySends");

    connectionStats.endQuerySend(1, false);

    verify(sendStats).incInt(eq(querySendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(querySendDurationId), anyLong());
  }

  @Test
  public void endQuery_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("queryTimeouts");

    connectionStats.endQuery(1, true, true);

    verify(stats).incInt(eq(queryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(queryDurationId), anyLong());
  }

  @Test
  public void endQuery_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("queryTimeouts");

    connectionStats.endQuery(1, true, false);

    verify(stats).incInt(eq(queryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(queryDurationId), anyLong());
  }

  @Test
  public void endQuery_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("queryFailures");

    connectionStats.endQuery(1, false, true);

    verify(stats).incInt(eq(queryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(queryDurationId), anyLong());
  }

  @Test
  public void endQuery_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("querys");

    connectionStats.endQuery(1, false, false);

    verify(stats).incInt(eq(queryInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(queryDurationId), anyLong());
  }

  @Test
  public void startQuery() {
    int statId = ConnectionStats.getType().nameToId("querysInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("querySendsInProgress");

    connectionStats.startQuery();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endReadyForEventsSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("readyForEventsSendFailures");

    connectionStats.endReadyForEventsSend(1, true);

    verify(sendStats).incInt(eq(readyForEventsSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(readyForEventsSendDurationId), anyLong());
  }

  @Test
  public void endReadyForEventsSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("readyForEventsSends");

    connectionStats.endReadyForEventsSend(1, false);

    verify(sendStats).incInt(eq(readyForEventsSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(readyForEventsSendDurationId), anyLong());
  }

  @Test
  public void endReadyForEvents_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("readyForEventsTimeouts");

    connectionStats.endReadyForEvents(1, true, true);

    verify(stats).incInt(eq(readyForEventsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(readyForEventsDurationId), anyLong());
  }

  @Test
  public void endReadyForEvents_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("readyForEventsTimeouts");

    connectionStats.endReadyForEvents(1, true, false);

    verify(stats).incInt(eq(readyForEventsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(readyForEventsDurationId), anyLong());
  }

  @Test
  public void endReadyForEvents_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("readyForEventsFailures");

    connectionStats.endReadyForEvents(1, false, true);

    verify(stats).incInt(eq(readyForEventsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(readyForEventsDurationId), anyLong());
  }

  @Test
  public void endReadyForEvents_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("readyForEvents");

    connectionStats.endReadyForEvents(1, false, false);

    verify(stats).incInt(eq(readyForEventsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(readyForEventsDurationId), anyLong());
  }

  @Test
  public void startReadyForEvents() {
    int statId = ConnectionStats.getType().nameToId("readyForEventsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("readyForEventsSendsInProgress");

    connectionStats.startReadyForEvents();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endRegisterDataSerializersSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("registerDataSerializersSendFailures");

    connectionStats.endRegisterDataSerializersSend(1, true);

    verify(sendStats).incInt(eq(registerDataSerializersSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(registerDataSerializersSendDurationId), anyLong());
  }

  @Test
  public void endRegisterDataSerializersSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("registerDataSerializersSends");

    connectionStats.endRegisterDataSerializersSend(1, false);

    verify(sendStats).incInt(eq(registerDataSerializersSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(registerDataSerializersSendDurationId), anyLong());
  }

  @Test
  public void endRegisterDataSerializers_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("registerDataSerializersTimeouts");

    connectionStats.endRegisterDataSerializers(1, true, true);

    verify(stats).incInt(eq(registerDataSerializersInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerDataSerializersDurationId), anyLong());
  }

  @Test
  public void endRegisterDataSerializers_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("registerDataSerializersTimeouts");

    connectionStats.endRegisterDataSerializers(1, true, false);

    verify(stats).incInt(eq(registerDataSerializersInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerDataSerializersDurationId), anyLong());
  }

  @Test
  public void endRegisterDataSerializers_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("registerDataSerializersFailures");

    connectionStats.endRegisterDataSerializers(1, false, true);

    verify(stats).incInt(eq(registerDataSerializersInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerDataSerializersDurationId), anyLong());
  }

  @Test
  public void endRegisterDataSerializers_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("registerDataSerializers");

    connectionStats.endRegisterDataSerializers(1, false, false);

    verify(stats).incInt(eq(registerDataSerializersInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerDataSerializersDurationId), anyLong());
  }

  @Test
  public void startRegisterDataSerializers() {
    int statId = ConnectionStats.getType().nameToId("registerDataSerializersInProgress");
    int sendStatId =
        ConnectionStats.getSendType().nameToId("registerDataSerializersSendInProgress");

    connectionStats.startRegisterDataSerializers();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endRegisterInstantiatorsSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("registerInstantiatorsSendFailures");

    connectionStats.endRegisterInstantiatorsSend(1, true);

    verify(sendStats).incInt(eq(registerInstantiatorsSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(registerInstantiatorsSendDurationId), anyLong());
  }

  @Test
  public void endRegisterInstantiatorsSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("registerInstantiatorsSends");

    connectionStats.endRegisterInstantiatorsSend(1, false);

    verify(sendStats).incInt(eq(registerInstantiatorsSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(registerInstantiatorsSendDurationId), anyLong());
  }

  @Test
  public void endRegisterInstantiators_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("registerInstantiatorsTimeouts");

    connectionStats.endRegisterInstantiators(1, true, true);

    verify(stats).incInt(eq(registerInstantiatorsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerInstantiatorsDurationId), anyLong());
  }

  @Test
  public void endRegisterInstantiators_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("registerInstantiatorsTimeouts");

    connectionStats.endRegisterInstantiators(1, true, false);

    verify(stats).incInt(eq(registerInstantiatorsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerInstantiatorsDurationId), anyLong());
  }

  @Test
  public void endRegisterInstantiators_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("registerInstantiatorsFailures");

    connectionStats.endRegisterInstantiators(1, false, true);

    verify(stats).incInt(eq(registerInstantiatorsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerInstantiatorsDurationId), anyLong());
  }

  @Test
  public void endRegisterInstantiators_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("registerInstantiators");

    connectionStats.endRegisterInstantiators(1, false, false);

    verify(stats).incInt(eq(registerInstantiatorsInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerInstantiatorsDurationId), anyLong());
  }

  @Test
  public void startRegisterInstantiators() {
    int statId = ConnectionStats.getType().nameToId("registerInstantiatorsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("registerInstantiatorsSendsInProgress");

    connectionStats.startRegisterInstantiators();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endRegisterInterestSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("registerInterestSendFailures");

    connectionStats.endRegisterInterestSend(1, true);

    verify(sendStats).incInt(eq(registerInterestSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(registerInterestSendDurationId), anyLong());
  }

  @Test
  public void endRegisterInterestSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("registerInterestSends");

    connectionStats.endRegisterInterestSend(1, false);

    verify(sendStats).incInt(eq(registerInterestSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(registerInterestSendDurationId), anyLong());
  }

  @Test
  public void endRegisterInterest_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("registerInterestTimeouts");

    connectionStats.endRegisterInterest(1, true, true);

    verify(stats).incInt(eq(registerInterestInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerInterestDurationId), anyLong());
  }

  @Test
  public void endRegisterInterest_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("registerInterestTimeouts");

    connectionStats.endRegisterInterest(1, true, false);

    verify(stats).incInt(eq(registerInterestInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerInterestDurationId), anyLong());
  }

  @Test
  public void endRegisterInterest_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("registerInterestFailures");

    connectionStats.endRegisterInterest(1, false, true);

    verify(stats).incInt(eq(registerInterestInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerInterestDurationId), anyLong());
  }

  @Test
  public void endRegisterInterest_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("registerInterests");

    connectionStats.endRegisterInterest(1, false, false);

    verify(stats).incInt(eq(registerInterestInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(registerInterestDurationId), anyLong());
  }

  @Test
  public void startRegisterInterest() {
    int statId = ConnectionStats.getType().nameToId("registerInterestsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("registerInterestSendsInProgress");

    connectionStats.startRegisterInterest();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endRemoveAllSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("removeAllSendFailures");

    connectionStats.endRemoveAllSend(1, true);

    verify(sendStats).incInt(eq(removeAllSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(removeAllSendDurationId), anyLong());
  }

  @Test
  public void endRemoveAllSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("removeAllSends");

    connectionStats.endRemoveAllSend(1, false);

    verify(sendStats).incInt(eq(removeAllSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(removeAllSendDurationId), anyLong());
  }

  @Test
  public void endRemoveAll_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("removeAllTimeouts");

    connectionStats.endRemoveAll(1, true, true);

    verify(stats).incInt(eq(removeAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(removeAllDurationId), anyLong());
  }

  @Test
  public void endRemoveAll_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("removeAllTimeouts");

    connectionStats.endRemoveAll(1, true, false);

    verify(stats).incInt(eq(removeAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(removeAllDurationId), anyLong());
  }

  @Test
  public void endRemoveAll_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("removeAllFailures");

    connectionStats.endRemoveAll(1, false, true);

    verify(stats).incInt(eq(removeAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(removeAllDurationId), anyLong());
  }

  @Test
  public void endRemoveAll_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("removeAlls");

    connectionStats.endRemoveAll(1, false, false);

    verify(stats).incInt(eq(removeAllInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(removeAllDurationId), anyLong());
  }

  @Test
  public void startRemoveAll() {
    int statId = ConnectionStats.getType().nameToId("removeAllsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("removeAllSendsInProgress");

    connectionStats.startRemoveAll();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endRollbackSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("rollbackSendFailures");

    connectionStats.endRollbackSend(1, true);

    verify(sendStats).incInt(eq(rollbackSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(rollbackSendDurationId), anyLong());
  }

  @Test
  public void endRollbackSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("rollbackSends");

    connectionStats.endRollbackSend(1, false);

    verify(sendStats).incInt(eq(rollbackSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(rollbackSendDurationId), anyLong());
  }

  @Test
  public void endRollback_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("rollbackTimeouts");

    connectionStats.endRollback(1, true, true);

    verify(stats).incInt(eq(rollbackInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(rollbackDurationId), anyLong());
  }

  @Test
  public void endRollback_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("rollbackTimeouts");

    connectionStats.endRollback(1, true, false);

    verify(stats).incInt(eq(rollbackInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(rollbackDurationId), anyLong());
  }

  @Test
  public void endRollback_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("rollbackFailures");

    connectionStats.endRollback(1, false, true);

    verify(stats).incInt(eq(rollbackInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(rollbackDurationId), anyLong());
  }

  @Test
  public void endRollback_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("rollbacks");

    connectionStats.endRollback(1, false, false);

    verify(stats).incInt(eq(rollbackInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(rollbackDurationId), anyLong());
  }

  @Test
  public void startRollback() {
    int statId = ConnectionStats.getType().nameToId("rollbacksInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("rollbackSendsInProgress");

    connectionStats.startRollback();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endSizeSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("sizeSendFailures");

    connectionStats.endSizeSend(1, true);

    verify(sendStats).incInt(eq(sizeSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(sizeSendDurationId), anyLong());
  }

  @Test
  public void endSizeSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("sizeSends");

    connectionStats.endSizeSend(1, false);

    verify(sendStats).incInt(eq(sizeSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(sizeSendDurationId), anyLong());
  }

  @Test
  public void endSize_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("sizeTimeouts");

    connectionStats.endSize(1, true, true);

    verify(stats).incInt(eq(sizeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(sizeDurationId), anyLong());
  }

  @Test
  public void endSize_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("sizeTimeouts");

    connectionStats.endSize(1, true, false);

    verify(stats).incInt(eq(sizeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(sizeDurationId), anyLong());
  }

  @Test
  public void endSize_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("sizeFailures");

    connectionStats.endSize(1, false, true);

    verify(stats).incInt(eq(sizeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(sizeDurationId), anyLong());
  }

  @Test
  public void endSize_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("sizes");

    connectionStats.endSize(1, false, false);

    verify(stats).incInt(eq(sizeInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(sizeDurationId), anyLong());
  }

  @Test
  public void startSize() {
    int statId = ConnectionStats.getType().nameToId("sizesInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("sizeSendsInProgress");

    connectionStats.startSize();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endStopCQSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("stopCQSendFailures");

    connectionStats.endStopCQSend(1, true);

    verify(sendStats).incInt(eq(stopCQSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(stopCQSendDurationId), anyLong());
  }

  @Test
  public void endStopCQSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("stopCQSends");

    connectionStats.endStopCQSend(1, false);

    verify(sendStats).incInt(eq(stopCQSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(stopCQSendDurationId), anyLong());
  }

  @Test
  public void endStopCQ_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("stopCQTimeouts");

    connectionStats.endStopCQ(1, true, true);

    verify(stats).incInt(eq(stopCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(stopCQDurationId), anyLong());
  }

  @Test
  public void endStopCQ_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("stopCQTimeouts");

    connectionStats.endStopCQ(1, true, false);

    verify(stats).incInt(eq(stopCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(stopCQDurationId), anyLong());
  }

  @Test
  public void endStopCQ_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("stopCQFailures");

    connectionStats.endStopCQ(1, false, true);

    verify(stats).incInt(eq(stopCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(stopCQDurationId), anyLong());
  }

  @Test
  public void endStopCQ_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("stopCQs");

    connectionStats.endStopCQ(1, false, false);

    verify(stats).incInt(eq(stopCQInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(stopCQDurationId), anyLong());
  }

  @Test
  public void startStopCQ() {
    int statId = ConnectionStats.getType().nameToId("stopCQsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("stopCQSendsInProgress");

    connectionStats.startStopCQ();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endTxFailoverSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("txFailoverSendFailures");

    connectionStats.endTxFailoverSend(1, true);

    verify(sendStats).incInt(eq(txFailoverSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(txFailoverSendDurationId), anyLong());
  }

  @Test
  public void endTxFailoverSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("txFailoverSends");

    connectionStats.endTxFailoverSend(1, false);

    verify(sendStats).incInt(eq(txFailoverSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(txFailoverSendDurationId), anyLong());
  }

  @Test
  public void endTxFailover_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("txFailoverTimeouts");

    connectionStats.endTxFailover(1, true, true);

    verify(stats).incInt(eq(txFailoverInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(txFailoverDurationId), anyLong());
  }

  @Test
  public void endTxFailover_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("txFailoverTimeouts");

    connectionStats.endTxFailover(1, true, false);

    verify(stats).incInt(eq(txFailoverInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(txFailoverDurationId), anyLong());
  }

  @Test
  public void endTxFailover_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("txFailoverFailures");

    connectionStats.endTxFailover(1, false, true);

    verify(stats).incInt(eq(txFailoverInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(txFailoverDurationId), anyLong());
  }

  @Test
  public void endTxFailover_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("txFailovers");

    connectionStats.endTxFailover(1, false, false);

    verify(stats).incInt(eq(txFailoverInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(txFailoverDurationId), anyLong());
  }

  @Test
  public void startTxFailover() {
    int statId = ConnectionStats.getType().nameToId("txFailoversInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("txFailoverSendsInProgress");

    connectionStats.startTxFailover();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }

  @Test
  public void endUnregisterInterestSend_FailedOperation() {
    int statId = ConnectionStats.getSendType().nameToId("unregisterInterestSendFailures");

    connectionStats.endUnregisterInterestSend(1, true);

    verify(sendStats).incInt(eq(unregisterInterestSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(unregisterInterestSendDurationId), anyLong());
  }

  @Test
  public void endUnregisterInterestSend_SuccessfulOperation() {
    int statId = ConnectionStats.getSendType().nameToId("unregisterInterestSends");

    connectionStats.endUnregisterInterestSend(1, false);

    verify(sendStats).incInt(eq(unregisterInterestSendInProgressId), eq(-1));
    verify(sendStats).incInt(statId, 1);
    verify(sendStats).incLong(eq(unregisterInterestSendDurationId), anyLong());
  }

  @Test
  public void endUnregisterInterest_TimeoutOperation() {
    int statId = ConnectionStats.getType().nameToId("unregisterInterestTimeouts");

    connectionStats.endUnregisterInterest(1, true, true);

    verify(stats).incInt(eq(unregisterInterestInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(unregisterInterestDurationId), anyLong());
  }

  @Test
  public void endUnregisterInterest_TimeoutOperationAndNotFailed() {
    int statId = ConnectionStats.getType().nameToId("unregisterInterestTimeouts");

    connectionStats.endUnregisterInterest(1, true, false);

    verify(stats).incInt(eq(unregisterInterestInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(unregisterInterestDurationId), anyLong());
  }

  @Test
  public void endUnregisterInterest_FailedOperation() {
    int statId = ConnectionStats.getType().nameToId("unregisterInterestFailures");

    connectionStats.endUnregisterInterest(1, false, true);

    verify(stats).incInt(eq(unregisterInterestInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(unregisterInterestDurationId), anyLong());
  }

  @Test
  public void endUnregisterInterest_SuccessfulOperation() {
    int statId = ConnectionStats.getType().nameToId("unregisterInterests");

    connectionStats.endUnregisterInterest(1, false, false);

    verify(stats).incInt(eq(unregisterInterestInProgressId), eq(-1));
    verify(stats).incInt(statId, 1);
    verify(stats).incLong(eq(unregisterInterestDurationId), anyLong());
  }

  @Test
  public void startUnregisterInterest() {
    int statId = ConnectionStats.getType().nameToId("unregisterInterestsInProgress");
    int sendStatId = ConnectionStats.getSendType().nameToId("unregisterInterestSendsInProgress");

    connectionStats.startUnregisterInterest();

    verify(stats).incInt(statId, 1);
    verify(sendStats).incInt(sendStatId, 1);
  }
}
