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
  private final Statistics sendStats = mock(Statistics.class);
  private final StatisticsFactory statisticsFactory = createStatisticsFactory(sendStats);
  private final PoolStats poolStats = mock(PoolStats.class);
  private final ConnectionStats connectionStats =
      new ConnectionStats(statisticsFactory, "name", poolStats);

  private StatisticsFactory createStatisticsFactory(Statistics sendStats) {
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    when(statisticsFactory.createAtomicStatistics(any(), eq("ClientSendStats-name")))
        .thenReturn(sendStats);
    return statisticsFactory;
  }

  @Test
  public void endGetSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getSendTime");

    connectionStats.endGetSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endPutSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("putSendTime");

    connectionStats.endPutSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endDestroySendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("destroySendTime");

    connectionStats.endDestroySend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endDestroyRegionSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("destroyRegionSendTime");

    connectionStats.endDestroyRegionSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endClearSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("clearSendTime");

    connectionStats.endClearSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endContainsKeySendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("containsKeySendTime");

    connectionStats.endContainsKeySend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endKeySetSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("keySetSendTime");

    connectionStats.endKeySetSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRegisterInterestSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("registerInterestSendTime");

    connectionStats.endRegisterInterestSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endUnregisterInterestSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("unregisterInterestSendTime");

    connectionStats.endUnregisterInterestSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endQuerySendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("querySendTime");

    connectionStats.endQuerySend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endStopCQSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("stopCQSendTime");

    connectionStats.endStopCQSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endCloseCQSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("closeCQSendTime");

    connectionStats.endCloseCQSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetDurableCQsSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getDurableCQsSendTime");

    connectionStats.endGetDurableCQsSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGatewayBatchSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("gatewayBatchSendTime");

    connectionStats.endGatewayBatchSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endReadyForEventsSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("readyForEventsSendTime");

    connectionStats.endReadyForEventsSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endMakePrimarySendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("makePrimarySendTime");

    connectionStats.endMakePrimarySend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endCloseConSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("closeConSendTime");

    connectionStats.endCloseConSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endPrimaryAckSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("primaryAckSendTime");

    connectionStats.endPrimaryAckSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endPingSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("pingSendTime");

    connectionStats.endPingSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRegisterInstantiatorsSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("registerInstantiatorsSendTime");

    connectionStats.endRegisterInstantiatorsSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRegisterDataSerializersSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("registerDataSerializersSendTime");

    connectionStats.endRegisterDataSerializersSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endPutAllSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("putAllSendTime");

    connectionStats.endPutAllSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRemoveAllSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("removeAllSendTime");

    connectionStats.endRemoveAllSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetAllSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getAllSendTime");

    connectionStats.endGetAllSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endExecuteFunctionSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("executeFunctionSendTime");

    connectionStats.endExecuteFunctionSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetClientPRMetadataSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPRMetadataSendTime");

    connectionStats.endGetClientPRMetadataSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetClientPartitionAttributesSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getClientPartitionAttributesSendTime");

    connectionStats.endGetClientPartitionAttributesSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetPDXTypeByIdSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getPDXTypeByIdSendTime");

    connectionStats.endGetPDXTypeByIdSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
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
  public void endInvalidateSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("invalidateSendTime");

    connectionStats.endInvalidateSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endCommitSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("commitSendTime");

    connectionStats.endCommitSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endGetEntrySendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("getEntrySendTime");

    connectionStats.endGetEntrySend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endRollbackSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("rollbackSendTime");

    connectionStats.endRollbackSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endTxFailoverSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("txFailoverSendTime");

    connectionStats.endTxFailoverSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

  @Test
  public void endTxSynchronizationSendIncsStatIdOnSendStats() {
    int statId = ConnectionStats.getSendType().nameToId("jtaSynchronizationSendTime");

    connectionStats.endTxSynchronizationSend(1, false);

    verify(sendStats).incLong(eq(statId), anyLong());
  }

}
