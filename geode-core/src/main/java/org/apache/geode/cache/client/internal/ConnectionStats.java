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
package org.apache.geode.cache.client.internal;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.sockets.MessageStats;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * Stats for a client to server {@link ClientCacheConnection}
 *
 * @since GemFire 5.7
 */
public class ConnectionStats implements MessageStats {
  // static fields
  @Immutable
  private static final StatisticsType type;
  @Immutable
  private static final StatisticsType sendType;

  @VisibleForTesting
  static StatisticsType getType() {
    return type;
  }

  @VisibleForTesting
  static StatisticsType getSendType() {
    return sendType;
  }

  private static final int getInProgressId;
  private static final int getSendInProgressId;
  private static final int getSendFailedId;
  private static final int getSendId;
  private static final int getSendDurationId;
  private static final int getTimedOutId;
  private static final int getFailedId;
  private static final int getId;
  private static final int getDurationId;

  private static final int putInProgressId;
  private static final int putSendInProgressId;
  private static final int putSendFailedId;
  private static final int putSendId;
  private static final int putSendDurationId;
  private static final int putTimedOutId;
  private static final int putFailedId;
  private static final int putId;
  private static final int putDurationId;

  private static final int destroyInProgressId;
  private static final int destroySendInProgressId;
  private static final int destroySendFailedId;
  private static final int destroySendId;
  private static final int destroySendDurationId;
  private static final int destroyTimedOutId;
  private static final int destroyFailedId;
  private static final int destroyId;
  private static final int destroyDurationId;

  private static final int destroyRegionInProgressId;
  private static final int destroyRegionSendInProgressId;
  private static final int destroyRegionSendFailedId;
  private static final int destroyRegionSendId;
  private static final int destroyRegionSendDurationId;
  private static final int destroyRegionTimedOutId;
  private static final int destroyRegionFailedId;
  private static final int destroyRegionId;
  private static final int destroyRegionDurationId;

  private static final int clearInProgressId;
  private static final int clearSendInProgressId;
  private static final int clearSendFailedId;
  private static final int clearSendId;
  private static final int clearSendDurationId;
  private static final int clearTimedOutId;
  private static final int clearFailedId;
  private static final int clearId;
  private static final int clearDurationId;

  private static final int containsKeyInProgressId;
  private static final int containsKeySendInProgressId;
  private static final int containsKeySendFailedId;
  private static final int containsKeySendId;
  private static final int containsKeySendDurationId;
  private static final int containsKeyTimedOutId;
  private static final int containsKeyFailedId;
  private static final int containsKeyId;
  private static final int containsKeyDurationId;

  private static final int keySetInProgressId;
  private static final int keySetSendInProgressId;
  private static final int keySetSendFailedId;
  private static final int keySetSendId;
  private static final int keySetSendDurationId;
  private static final int keySetTimedOutId;
  private static final int keySetFailedId;
  private static final int keySetId;
  private static final int keySetDurationId;

  private static final int commitInProgressId;
  private static final int commitSendInProgressId;
  private static final int commitSendFailedId;
  private static final int commitSendId;
  private static final int commitSendDurationId;

  private static final int commitFailedId;
  private static final int commitTimedOutId;
  private static final int commitId;
  private static final int commitDurationId;

  private static final int rollbackInProgressId;
  private static final int rollbackSendInProgressId;
  private static final int rollbackSendFailedId;
  private static final int rollbackSendId;
  private static final int rollbackSendDurationId;

  private static final int rollbackFailedId;
  private static final int rollbackTimedOutId;
  private static final int rollbackId;
  private static final int rollbackDurationId;

  private static final int getEntryInProgressId;
  private static final int getEntrySendInProgressId;
  private static final int getEntrySendFailedId;
  private static final int getEntrySendId;
  private static final int getEntrySendDurationId;

  private static final int getEntryFailedId;
  private static final int getEntryTimedOutId;
  private static final int getEntryId;
  private static final int getEntryDurationId;

  private static final int txSynchronizationInProgressId;
  private static final int txSynchronizationSendInProgressId;
  private static final int txSynchronizationSendFailedId;
  private static final int txSynchronizationSendId;
  private static final int txSynchronizationSendDurationId;

  private static final int txSynchronizationFailedId;
  private static final int txSynchronizationTimedOutId;
  private static final int txSynchronizationId;
  private static final int txSynchronizationDurationId;

  private static final int txFailoverInProgressId;
  private static final int txFailoverSendInProgressId;
  private static final int txFailoverSendFailedId;
  private static final int txFailoverSendId;
  private static final int txFailoverSendDurationId;

  private static final int txFailoverFailedId;
  private static final int txFailoverTimedOutId;
  private static final int txFailoverId;
  private static final int txFailoverDurationId;

  private static final int sizeInProgressId;
  private static final int sizeSendInProgressId;
  private static final int sizeSendFailedId;
  private static final int sizeSendId;
  private static final int sizeSendDurationId;

  private static final int sizeFailedId;
  private static final int sizeTimedOutId;
  private static final int sizeId;
  private static final int sizeDurationId;

  private static final int invalidateInProgressId;
  private static final int invalidateSendInProgressId;
  private static final int invalidateSendFailedId;
  private static final int invalidateSendId;
  private static final int invalidateSendDurationId;

  private static final int invalidateFailedId;
  private static final int invalidateTimedOutId;
  private static final int invalidateId;
  private static final int invalidateDurationId;


  private static final int registerInterestInProgressId;
  private static final int registerInterestSendInProgressId;
  private static final int registerInterestSendFailedId;
  private static final int registerInterestSendId;
  private static final int registerInterestSendDurationId;
  private static final int registerInterestTimedOutId;
  private static final int registerInterestFailedId;
  private static final int registerInterestId;
  private static final int registerInterestDurationId;

  private static final int unregisterInterestInProgressId;
  private static final int unregisterInterestSendInProgressId;
  private static final int unregisterInterestSendFailedId;
  private static final int unregisterInterestSendId;
  private static final int unregisterInterestSendDurationId;
  private static final int unregisterInterestTimedOutId;
  private static final int unregisterInterestFailedId;
  private static final int unregisterInterestId;
  private static final int unregisterInterestDurationId;

  private static final int queryInProgressId;
  private static final int querySendInProgressId;
  private static final int querySendFailedId;
  private static final int querySendId;
  private static final int querySendDurationId;
  private static final int queryTimedOutId;
  private static final int queryFailedId;
  private static final int queryId;
  private static final int queryDurationId;

  private static final int createCQInProgressId;
  private static final int createCQSendInProgressId;
  private static final int createCQSendFailedId;
  private static final int createCQSendId;
  private static final int createCQSendDurationId;
  private static final int createCQTimedOutId;
  private static final int createCQFailedId;
  private static final int createCQId;
  private static final int createCQDurationId;
  private static final int stopCQInProgressId;
  private static final int stopCQSendInProgressId;
  private static final int stopCQSendFailedId;
  private static final int stopCQSendId;
  private static final int stopCQSendDurationId;
  private static final int stopCQTimedOutId;
  private static final int stopCQFailedId;
  private static final int stopCQId;
  private static final int stopCQDurationId;
  private static final int closeCQInProgressId;
  private static final int closeCQSendInProgressId;
  private static final int closeCQSendFailedId;
  private static final int closeCQSendId;
  private static final int closeCQSendDurationId;
  private static final int closeCQTimedOutId;
  private static final int closeCQFailedId;
  private static final int closeCQId;
  private static final int closeCQDurationId;
  private static final int gatewayBatchInProgressId;
  private static final int gatewayBatchSendInProgressId;
  private static final int gatewayBatchSendFailedId;
  private static final int gatewayBatchSendId;
  private static final int gatewayBatchSendDurationId;
  private static final int gatewayBatchTimedOutId;
  private static final int gatewayBatchFailedId;
  private static final int gatewayBatchId;
  private static final int gatewayBatchDurationId;
  private static final int getDurableCQsInProgressId;
  private static final int getDurableCQsSendsInProgressId;
  private static final int getDurableCQsSendFailedId;
  private static final int getDurableCQsSendId;
  private static final int getDurableCQsSendDurationId;
  private static final int getDurableCQsTimedOutId;
  private static final int getDurableCQsFailedId;
  private static final int getDurableCQsId;
  private static final int getDurableCQsDurationId;

  private static final int readyForEventsInProgressId;
  private static final int readyForEventsSendInProgressId;
  private static final int readyForEventsSendFailedId;
  private static final int readyForEventsSendId;
  private static final int readyForEventsSendDurationId;
  private static final int readyForEventsTimedOutId;
  private static final int readyForEventsFailedId;
  private static final int readyForEventsId;
  private static final int readyForEventsDurationId;

  private static final int makePrimaryInProgressId;
  private static final int makePrimarySendInProgressId;
  private static final int makePrimarySendFailedId;
  private static final int makePrimarySendId;
  private static final int makePrimarySendDurationId;
  private static final int makePrimaryTimedOutId;
  private static final int makePrimaryFailedId;
  private static final int makePrimaryId;
  private static final int makePrimaryDurationId;

  private static final int closeConInProgressId;
  private static final int closeConSendInProgressId;
  private static final int closeConSendFailedId;
  private static final int closeConSendId;
  private static final int closeConSendDurationId;
  private static final int closeConTimedOutId;
  private static final int closeConFailedId;
  private static final int closeConId;
  private static final int closeConDurationId;

  private static final int primaryAckInProgressId;
  private static final int primaryAckSendInProgressId;
  private static final int primaryAckSendFailedId;
  private static final int primaryAckSendId;
  private static final int primaryAckSendDurationId;
  private static final int primaryAckTimedOutId;
  private static final int primaryAckFailedId;
  private static final int primaryAckId;
  private static final int primaryAckDurationId;

  private static final int pingInProgressId;
  private static final int pingSendInProgressId;
  private static final int pingSendFailedId;
  private static final int pingSendId;
  private static final int pingSendDurationId;
  private static final int pingTimedOutId;
  private static final int pingFailedId;
  private static final int pingId;
  private static final int pingDurationId;

  private static final int registerInstantiatorsInProgressId;
  private static final int registerInstantiatorsSendInProgressId;
  private static final int registerInstantiatorsSendFailedId;
  private static final int registerInstantiatorsSendId;
  private static final int registerInstantiatorsSendDurationId;
  private static final int registerInstantiatorsTimedOutId;
  private static final int registerInstantiatorsFailedId;
  private static final int registerInstantiatorsId;
  private static final int registerInstantiatorsDurationId;

  private static final int registerDataSerializersInProgressId;
  private static final int registerDataSerializersSendInProgressId;
  private static final int registerDataSerializersSendFailedId;
  private static final int registerDataSerializersSendId;
  private static final int registerDataSerializersSendDurationId;
  private static final int registerDataSerializersTimedOutId;
  private static final int registerDataSerializersFailedId;
  private static final int registerDataSerializersId;
  private static final int registerDataSerializersDurationId;

  private static final int putAllInProgressId;
  private static final int putAllSendInProgressId;
  private static final int putAllSendFailedId;
  private static final int putAllSendId;
  private static final int putAllSendDurationId;
  private static final int putAllTimedOutId;
  private static final int putAllFailedId;
  private static final int putAllId;
  private static final int putAllDurationId;

  private static final int removeAllInProgressId;
  private static final int removeAllSendInProgressId;
  private static final int removeAllSendFailedId;
  private static final int removeAllSendId;
  private static final int removeAllSendDurationId;
  private static final int removeAllTimedOutId;
  private static final int removeAllFailedId;
  private static final int removeAllId;
  private static final int removeAllDurationId;

  private static final int getAllInProgressId;
  private static final int getAllSendInProgressId;
  private static final int getAllSendFailedId;
  private static final int getAllSendId;
  private static final int getAllSendDurationId;
  private static final int getAllTimedOutId;
  private static final int getAllFailedId;
  private static final int getAllId;
  private static final int getAllDurationId;

  private static final int connectionsId;
  private static final int connectsId;
  private static final int disconnectsId;
  private static final int messagesBeingReceivedId;
  private static final int messageBytesBeingReceivedId;
  private static final int receivedBytesId;
  private static final int sentBytesId;

  private static final int executeFunctionInProgressId;
  private static final int executeFunctionSendInProgressId;
  private static final int executeFunctionSendFailedId;
  private static final int executeFunctionSendId;
  private static final int executeFunctionSendDurationId;
  private static final int executeFunctionTimedOutId;
  private static final int executeFunctionFailedId;
  private static final int executeFunctionId;
  private static final int executeFunctionDurationId;

  private static final int getClientPRMetadataInProgressId;
  private static final int getClientPRMetadataSendInProgressId;
  private static final int getClientPRMetadataSendFailedId;
  private static final int getClientPRMetadataSendId;
  private static final int getClientPRMetadataSendDurationId;
  private static final int getClientPRMetadataTimedOutId;
  private static final int getClientPRMetadataFailedId;
  private static final int getClientPRMetadataId;
  private static final int getClientPRMetadataDurationId;

  private static final int getClientPartitionAttributesInProgressId;
  private static final int getClientPartitionAttributesSendInProgressId;
  private static final int getClientPartitionAttributesSendFailedId;
  private static final int getClientPartitionAttributesSendId;
  private static final int getClientPartitionAttributesSendDurationId;
  private static final int getClientPartitionAttributesTimedOutId;
  private static final int getClientPartitionAttributesFailedId;
  private static final int getClientPartitionAttributesId;
  private static final int getClientPartitionAttributesDurationId;

  private static final int getPDXIdForTypeInProgressId;
  private static final int getPDXIdForTypeSendInProgressId;
  private static final int getPDXIdForTypeSendFailedId;
  private static final int getPDXIdForTypeSendId;
  private static final int getPDXIdForTypeSendDurationId;
  private static final int getPDXIdForTypeTimedOutId;
  private static final int getPDXIdForTypeFailedId;
  private static final int getPDXIdForTypeId;
  private static final int getPDXIdForTypeDurationId;

  private static final int getPDXTypeByIdInProgressId;
  private static final int getPDXTypeByIdSendInProgressId;
  private static final int getPDXTypeByIdSendFailedId;
  private static final int getPDXTypeByIdSendId;
  private static final int getPDXTypeByIdSendDurationId;
  private static final int getPDXTypeByIdTimedOutId;
  private static final int getPDXTypeByIdFailedId;
  private static final int getPDXTypeByIdId;
  private static final int getPDXTypeByIdDurationId;

  private static final int addPdxTypeInProgressId;
  private static final int addPdxTypeSendInProgressId;
  private static final int addPdxTypeSendFailedId;
  private static final int addPdxTypeSendId;
  private static final int addPdxTypeSendDurationId;
  private static final int addPdxTypeTimedOutId;
  private static final int addPdxTypeFailedId;
  private static final int addPdxTypeId;
  private static final int addPdxTypeDurationId;


  // An array of all of the ids that represent operation statistics. This
  // is used by the getOps method to aggregate the individual stats
  // into a total value for all operations.
  @Immutable
  private static final int[] opIds;

  static {
    try {
      StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
      type = f.createType("ClientStats", "Statistics about client to server communication",
          new StatisticDescriptor[] {
              f.createIntGauge("addPdxTypeInProgress",
                  "Current number of addPdxType operations being executed", "addPdxType"),
              f.createIntCounter("addPdxTypeFailures",
                  "Total number of addPdxType operation attempts that have failed", "addPdxType"),
              f.createIntCounter("addPdxTypeSuccessful",
                  "Total number of addPdxType operations completed successfully", "addPdxType"),
              f.createIntCounter("addPdxTypeTimeouts",
                  "Total number of addPdxType operation attempts that have timed out",
                  "addPdxType"),
              f.createLongCounter("addPdxTypeTime",
                  "Total amount of time, in nanoseconds spent doing addPdxType successfully/unsuccessfully.",
                  "nanoseconds"),
              f.createLongCounter("clears", "Total number of clears completed successfully",
                  "clears"),
              f.createIntGauge("clearsInProgress", "Current number of clears being executed",
                  "clears"),
              f.createLongCounter("clearFailures",
                  "Total number of clear attempts that have failed",
                  "clears"),
              f.createLongCounter("clearTimeouts",
                  "Total number of clear attempts that have timed out", "clears"),
              f.createLongCounter("clearTime",
                  "Total amount of time, in nanoseconds spent doing clears", "nanoseconds"),
              f.createIntGauge("closeConsInProgress", "Current number of closeCons being executed",
                  "closeCons"),
              f.createIntCounter("closeCons", "Total number of closeCons completed successfully",
                  "closeCons"),
              f.createIntCounter("closeConFailures",
                  "Total number of closeCon attempts that have failed", "closeCons"),
              f.createIntCounter("closeConTimeouts",
                  "Total number of closeCon attempts that have timed out", "closeCons"),
              f.createLongCounter("closeConTime",
                  "Total amount of time, in nanoseconds spent doing closeCons", "nanoseconds"),
              f.createIntGauge("closeCQsInProgress", "Current number of closeCQs being executed",
                  "closeCQs"),
              f.createIntCounter("closeCQs", "Total number of closeCQs completed successfully",
                  "closeCQs"),
              f.createIntCounter("closeCQFailures",
                  "Total number of closeCQ attempts that have failed", "closeCQs"),
              f.createIntCounter("closeCQTimeouts",
                  "Total number of closeCQ attempts that have timed out", "closeCQs"),
              f.createLongCounter("closeCQTime",
                  "Total amount of time, in nanoseconds spent doing closeCQs", "nanoseconds"),
              f.createIntGauge("createCQsInProgress", "Current number of createCQs being executed",
                  "createCQs"),
              f.createIntCounter("createCQs", "Total number of createCQs completed successfully",
                  "createCQs"),
              f.createIntCounter("createCQFailures",
                  "Total number of createCQ attempts that have failed", "createCQs"),
              f.createIntCounter("createCQTimeouts",
                  "Total number of createCQ attempts that have timed out", "createCQs"),
              f.createLongCounter("createCQTime",
                  "Total amount of time, in nanoseconds spent doing createCQs", "nanoseconds"),
              f.createIntGauge("commitsInProgress", "Current number of commits being executed",
                  "commits"),
              f.createIntCounter("commits", "Total number of commits completed successfully",
                  "commits"),
              f.createIntCounter("commitFailures",
                  "Total number of commit attempts that have failed", "commits"),
              f.createIntCounter("commitTimeouts",
                  "Total number of commit attempts that have timed out", "commits"),
              f.createLongCounter("commitTime",
                  "Total amount of time, in nanoseconds spent doing commits", "nanoseconds"),
              f.createIntGauge("containsKeysInProgress",
                  "Current number of containsKeys being executed", "containsKeys"),
              f.createIntCounter("containsKeys",
                  "Total number of containsKeys completed successfully", "containsKeys"),
              f.createIntCounter("containsKeyFailures",
                  "Total number of containsKey attempts that have failed", "containsKeys"),
              f.createIntCounter("containsKeyTimeouts",
                  "Total number of containsKey attempts that have timed out", "containsKeys"),
              f.createLongCounter("containsKeyTime",
                  "Total amount of time, in nanoseconds spent doing containsKeys", "nanoseconds"),
              f.createIntGauge("destroyRegionsInProgress",
                  "Current number of destroyRegions being executed", "destroyRegions"),
              f.createIntCounter("destroyRegions",
                  "Total number of destroyRegions completed successfully", "destroyRegions"),
              f.createIntCounter("destroyRegionFailures",
                  "Total number of destroyRegion attempts that have failed", "destroyRegions"),
              f.createIntCounter("destroyRegionTimeouts",
                  "Total number of destroyRegion attempts that have timed out", "destroyRegions"),
              f.createLongCounter("destroyRegionTime",
                  "Total amount of time, in nanoseconds spent doing destroyRegions", "nanoseconds"),
              f.createIntGauge("destroysInProgress", "Current number of destroys being executed",
                  "destroys"),
              f.createLongCounter("destroys", "Total number of destroys completed successfully",
                  "destroys"),
              f.createLongCounter("destroyFailures",
                  "Total number of destroy attempts that have failed", "destroys"),
              f.createLongCounter("destroyTimeouts",
                  "Total number of destroy attempts that have timed out", "destroys"),
              f.createLongCounter("destroyTime",
                  "Total amount of time, in nanoseconds spent doing destroys", "nanoseconds"),
              f.createIntGauge("executeFunctionsInProgress",
                  "Current number of Functions being executed", "executeFunctions"),
              f.createIntCounter("executeFunctions",
                  "Total number of Functions completed successfully", "executeFunctions"),
              f.createIntCounter("executeFunctionFailures",
                  "Total number of Function attempts that have failed", "executeFunctions"),
              f.createIntCounter("executeFunctionTimeouts",
                  "Total number of Function attempts that have timed out", "executeFunctions"),
              f.createLongCounter("executeFunctionTime",
                  "Total amount of time, in nanoseconds spent doing Functions", "nanoseconds"),
              f.createIntGauge("gatewayBatchsInProgress",
                  "Current number of gatewayBatchs being executed", "gatewayBatchs"),
              f.createIntCounter("gatewayBatchs",
                  "Total number of gatewayBatchs completed successfully", "gatewayBatchs"),
              f.createIntCounter("gatewayBatchFailures",
                  "Total number of gatewayBatch attempts that have failed", "gatewayBatchs"),
              f.createIntCounter("gatewayBatchTimeouts",
                  "Total number of gatewayBatch attempts that have timed out", "gatewayBatchs"),
              f.createLongCounter("gatewayBatchTime",
                  "Total amount of time, in nanoseconds spent doing gatewayBatchs", "nanoseconds"),
              f.createIntGauge("getAllsInProgress", "Current number of getAlls being executed",
                  "getAlls"),
              f.createIntCounter("getAlls", "Total number of getAlls completed successfully",
                  "getAlls"),
              f.createIntCounter("getAllFailures",
                  "Total number of getAll attempts that have failed", "getAlls"),
              f.createIntCounter("getAllTimeouts",
                  "Total number of getAll attempts that have timed out", "getAlls"),
              f.createLongCounter("getAllTime",
                  "Total amount of time, in nanoseconds spent doing getAlls", "nanoseconds"),
              f.createIntGauge("getClientPartitionAttributesInProgress",
                  "Current number of getClientPartitionAttributes operations being executed",
                  "getClientPartitionAttributes"),
              f.createIntCounter("getClientPartitionAttributesFailures",
                  "Total number of getClientPartitionAttributes operation attempts that have failed",
                  "getClientPartitionAttributes"),
              f.createIntCounter("getClientPartitionAttributesSuccessful",
                  "Total number of getClientPartitionAttributes operations completed successfully",
                  "getClientPartitionAttributes"),
              f.createIntCounter("getClientPartitionAttributesTimeouts",
                  "Total number of getClientPartitionAttributes operation attempts that have timed out",
                  "getClientPartitionAttributes"),
              f.createLongCounter("getClientPartitionAttributesTime",
                  "Total amount of time, in nanoseconds spent doing getClientPartitionAttributes successfully/unsuccessfully.",
                  "nanoseconds"),
              f.createIntGauge("getClientPRMetadataInProgress",
                  "Current number of getClientPRMetadata operations being executed",
                  "getClientPRMetadata"),
              f.createIntCounter("getClientPRMetadataFailures",
                  "Total number of getClientPRMetadata operation attempts that have failed",
                  "getClientPRMetadata"),
              f.createIntCounter("getClientPRMetadataSuccessful",
                  "Total number of getClientPRMetadata operations completed successfully",
                  "getClientPRMetadata"),
              f.createIntCounter("getClientPRMetadataTimeouts",
                  "Total number of getClientPRMetadata operation attempts that have timed out",
                  "getClientPRMetadata"),
              f.createLongCounter("getClientPRMetadataTime",
                  "Total amount of time, in nanoseconds spent doing getClientPRMetadata successfully/unsuccessfully",
                  "nanoseconds"),
              f.createIntGauge("getPDXIdForTypeInProgress",
                  "Current number of getPDXIdForType operations being executed", "getPDXIdForType"),
              f.createIntCounter("getPDXIdForTypeFailures",
                  "Total number of getPDXIdForType operation attempts that have failed",
                  "getPDXIdForType"),
              f.createIntCounter("getPDXIdForTypeSuccessful",
                  "Total number of getPDXIdForType operations completed successfully",
                  "getPDXIdForType"),
              f.createIntCounter("getPDXIdForTypeTimeouts",
                  "Total number of getPDXIdForType operation attempts that have timed out",
                  "getPDXIdForType"),
              f.createLongCounter("getPDXIdForTypeTime",
                  "Total amount of time, in nanoseconds spent doing getPDXIdForType successfully/unsuccessfully.",
                  "nanoseconds"),
              f.createIntGauge("getPDXTypeByIdInProgress",
                  "Current number of getPDXTypeById operations being executed", "getPDXTypeById"),
              f.createIntCounter("getPDXTypeByIdFailures",
                  "Total number of getPDXTypeById operation attempts that have failed",
                  "getPDXTypeById"),
              f.createIntCounter("getPDXTypeByIdSuccessful",
                  "Total number of getPDXTypeById operations completed successfully",
                  "getPDXTypeById"),
              f.createIntCounter("getPDXTypeByIdTimeouts",
                  "Total number of getPDXTypeById operation attempts that have timed out",
                  "getPDXTypeById"),
              f.createLongCounter("getPDXTypeByIdTime",
                  "Total amount of time, in nanoseconds spent doing getPDXTypeById successfully/unsuccessfully.",
                  "nanoseconds"),
              f.createIntGauge("getEntrysInProgress",
                  "Current number of getEntry messages being executed", "messages"),
              f.createIntCounter("getEntrys",
                  "Total number of getEntry messages completed successfully", "messages"),
              f.createIntCounter("getEntryFailures",
                  "Total number of getEntry attempts that have failed", "attempts"),
              f.createIntCounter("getEntryTimeouts",
                  "Total number of getEntry attempts that have timed out", "attempts"),
              f.createLongCounter("getEntryTime",
                  "Total amount of time, in nanoseconds spent doing getEntry processings",
                  "nanoseconds"),
              f.createIntGauge("getDurableCQsInProgress",
                  "Current number of getDurableCQs being executed", "getDurableCQs"),
              f.createIntCounter("getDurableCQs",
                  "Total number of getDurableCQs completed successfully", "getDurableCQs"),
              f.createIntCounter("getDurableCQsFailures",
                  "Total number of getDurableCQs attempts that have failed", "getDurableCQs"),
              f.createIntCounter("getDurableCQsTimeouts",
                  "Total number of getDurableCQs attempts that have timed out", "getDurableCQs"),
              f.createLongCounter("getDurableCQsTime",
                  "Total amount of time, in nanoseconds spent doing getDurableCQs", "nanoseconds"),
              f.createIntGauge("getsInProgress", "Current number of gets being executed", "gets"),
              f.createLongCounter("gets", "Total number of gets completed successfully", "gets"),
              f.createLongCounter("getFailures", "Total number of get attempts that have failed",
                  "gets"),
              f.createLongCounter("getTimeouts", "Total number of get attempts that have timed out",
                  "gets"),
              f.createLongCounter("getTime",
                  "Total amount of time, in nanoseconds spent doing gets", "nanoseconds"),
              f.createIntGauge("invalidatesInProgress",
                  "Current number of invalidates being executed", "invalidates"),
              f.createLongCounter("invalidates",
                  "Total number of invalidates completed successfully", "invalidates"),
              f.createLongCounter("invalidateFailures",
                  "Total number of invalidate attempts that have failed", "invalidates"),
              f.createLongCounter("invalidateTimeouts",
                  "Total number of invalidate attempts that have timed out", "invalidates"),
              f.createLongCounter("invalidateTime",
                  "Total amount of time, in nanoseconds spent doing invalidates", "nanoseconds"),
              f.createIntGauge("jtaSynchronizationsInProgress",
                  "Current number of jtaSynchronizations being executed", "sizes"),
              f.createIntCounter("jtaSynchronizations",
                  "Total number of jtaSynchronizations completed successfully",
                  "jtaSynchronizations"),
              f.createIntCounter("jtaSynchronizationFailures",
                  "Total number of jtaSynchronization attempts that have failed",
                  "jtaSynchronizations"),
              f.createIntCounter("jtaSynchronizationTimeouts",
                  "Total number of jtaSynchronization attempts that have timed out",
                  "jtaSynchronizations"),
              f.createLongCounter("jtaSynchronizationTime",
                  "Total amount of time, in nanoseconds spent doing jtaSynchronizations",
                  "nanoseconds"),
              f.createIntGauge("keySetsInProgress", "Current number of keySets being executed",
                  "keySets"),
              f.createIntCounter("keySets", "Total number of keySets completed successfully",
                  "keySets"),
              f.createIntCounter("keySetFailures",
                  "Total number of keySet attempts that have failed", "keySets"),
              f.createIntCounter("keySetTimeouts",
                  "Total number of keySet attempts that have timed out", "keySets"),
              f.createLongCounter("keySetTime",
                  "Total amount of time, in nanoseconds spent doing keySets", "nanoseconds"),
              f.createIntGauge("makePrimarysInProgress",
                  "Current number of makePrimarys being executed", "makePrimarys"),
              f.createIntCounter("makePrimarys",
                  "Total number of makePrimarys completed successfully", "makePrimarys"),
              f.createIntCounter("makePrimaryFailures",
                  "Total number of makePrimary attempts that have failed", "makePrimarys"),
              f.createIntCounter("makePrimaryTimeouts",
                  "Total number of makePrimary attempts that have timed out", "makePrimarys"),
              f.createLongCounter("makePrimaryTime",
                  "Total amount of time, in nanoseconds spent doing makePrimarys", "nanoseconds"),
              f.createIntGauge("pingsInProgress", "Current number of pings being executed",
                  "pings"),
              f.createIntCounter("pings", "Total number of pings completed successfully", "pings"),
              f.createIntCounter("pingFailures", "Total number of ping attempts that have failed",
                  "pings"),
              f.createIntCounter("pingTimeouts",
                  "Total number of ping attempts that have timed out", "pings"),
              f.createLongCounter("pingTime",
                  "Total amount of time, in nanoseconds spent doing pings", "nanoseconds"),
              f.createIntGauge("primaryAcksInProgress",
                  "Current number of primaryAcks being executed", "primaryAcks"),
              f.createIntCounter("primaryAcks",
                  "Total number of primaryAcks completed successfully", "primaryAcks"),
              f.createIntCounter("primaryAckFailures",
                  "Total number of primaryAck attempts that have failed", "primaryAcks"),
              f.createIntCounter("primaryAckTimeouts",
                  "Total number of primaryAck attempts that have timed out", "primaryAcks"),
              f.createLongCounter("primaryAckTime",
                  "Total amount of time, in nanoseconds spent doing primaryAcks", "nanoseconds"),
              f.createIntGauge("putAllsInProgress", "Current number of putAlls being executed",
                  "putAlls"),
              f.createIntCounter("putAlls", "Total number of putAlls completed successfully",
                  "putAlls"),
              f.createIntCounter("putAllFailures",
                  "Total number of putAll attempts that have failed", "putAlls"),
              f.createIntCounter("putAllTimeouts",
                  "Total number of putAll attempts that have timed out", "putAlls"),
              f.createLongCounter("putAllTime",
                  "Total amount of time, in nanoseconds spent doing putAlls", "nanoseconds"),
              f.createIntGauge("putsInProgress", "Current number of puts being executed", "puts"),
              f.createLongCounter("puts", "Total number of puts completed successfully", "puts"),
              f.createLongCounter("putFailures", "Total number of put attempts that have failed",
                  "puts"),
              f.createLongCounter("putTimeouts", "Total number of put attempts that have timed out",
                  "puts"),
              f.createLongCounter("putTime",
                  "Total amount of time, in nanoseconds spent doing puts", "nanoseconds"),
              f.createIntGauge("querysInProgress", "Current number of querys being executed",
                  "querys"),
              f.createIntCounter("querys", "Total number of querys completed successfully",
                  "querys"),
              f.createIntCounter("queryFailures", "Total number of query attempts that have failed",
                  "querys"),
              f.createIntCounter("queryTimeouts",
                  "Total number of query attempts that have timed out", "querys"),
              f.createLongCounter("queryTime",
                  "Total amount of time, in nanoseconds spent doing querys", "nanoseconds"),
              f.createIntGauge("readyForEventsInProgress",
                  "Current number of readyForEvents being executed", "readyForEvents"),
              f.createIntCounter("readyForEvents",
                  "Total number of readyForEvents completed successfully", "readyForEvents"),
              f.createIntCounter("readyForEventsFailures",
                  "Total number of readyForEvents attempts that have failed", "readyForEvents"),
              f.createIntCounter("readyForEventsTimeouts",
                  "Total number of readyForEvents attempts that have timed out", "readyForEvents"),
              f.createLongCounter("readyForEventsTime",
                  "Total amount of time, in nanoseconds spent doing readyForEvents", "nanoseconds"),
              f.createIntGauge("registerDataSerializersInProgress",
                  "Current number of registerDataSerializers being executed",
                  "registerDataSerializers"),
              f.createIntCounter("registerDataSerializers",
                  "Total number of registerDataSerializers completed successfully",
                  "registerDataSerializers"),
              f.createIntCounter("registerDataSerializersFailures",
                  "Total number of registerDataSerializers attempts that have failed",
                  "registerDataSerializers"),
              f.createIntCounter("registerDataSerializersTimeouts",
                  "Total number of registerDataSerializers attempts that have timed out",
                  "registerDataSerializers"),
              f.createLongCounter("registerDataSerializersTime",
                  "Total amount of time, in nanoseconds spent doing registerDataSerializers",
                  "nanoseconds"),
              f.createIntGauge("registerInstantiatorsInProgress",
                  "Current number of registerInstantiators being executed",
                  "registerInstantiators"),
              f.createIntCounter("registerInstantiators",
                  "Total number of registerInstantiators completed successfully",
                  "registerInstantiators"),
              f.createIntCounter("registerInstantiatorsFailures",
                  "Total number of registerInstantiators attempts that have failed",
                  "registerInstantiators"),
              f.createIntCounter("registerInstantiatorsTimeouts",
                  "Total number of registerInstantiators attempts that have timed out",
                  "registerInstantiators"),
              f.createLongCounter("registerInstantiatorsTime",
                  "Total amount of time, in nanoseconds spent doing registerInstantiators",
                  "nanoseconds"),
              f.createIntGauge("registerInterestsInProgress",
                  "Current number of registerInterests being executed", "registerInterests"),
              f.createIntCounter("registerInterests",
                  "Total number of registerInterests completed successfully", "registerInterests"),
              f.createIntCounter("registerInterestFailures",
                  "Total number of registerInterest attempts that have failed",
                  "registerInterests"),
              f.createIntCounter("registerInterestTimeouts",
                  "Total number of registerInterest attempts that have timed out",
                  "registerInterests"),
              f.createLongCounter("registerInterestTime",
                  "Total amount of time, in nanoseconds spent doing registerInterests",
                  "nanoseconds"),
              f.createIntGauge("removeAllsInProgress",
                  "Current number of removeAlls being executed", "removeAlls"),
              f.createIntCounter("removeAlls", "Total number of removeAlls completed successfully",
                  "removeAlls"),
              f.createIntCounter("removeAllFailures",
                  "Total number of removeAll attempts that have failed", "removeAlls"),
              f.createIntCounter("removeAllTimeouts",
                  "Total number of removeAll attempts that have timed out", "removeAlls"),
              f.createLongCounter("removeAllTime",
                  "Total amount of time, in nanoseconds spent doing removeAlls", "nanoseconds"),
              f.createIntGauge("rollbacksInProgress", "Current number of rollbacks being executed",
                  "rollbacks"),
              f.createIntCounter("rollbacks", "Total number of rollbacks completed successfully",
                  "rollbacks"),
              f.createIntCounter("rollbackFailures",
                  "Total number of rollback attempts that have failed", "rollbacks"),
              f.createIntCounter("rollbackTimeouts",
                  "Total number of rollback attempts that have timed out", "rollbacks"),
              f.createLongCounter("rollbackTime",
                  "Total amount of time, in nanoseconds spent doing rollbacks", "nanoseconds"),
              f.createIntGauge("sizesInProgress", "Current number of sizes being executed",
                  "sizes"),
              f.createIntCounter("sizes", "Total number of sizes completed successfully", "sizes"),
              f.createIntCounter("sizeFailures", "Total number of size attempts that have failed",
                  "sizes"),
              f.createIntCounter("sizeTimeouts",
                  "Total number of size attempts that have timed out", "sizes"),
              f.createLongCounter("sizeTime",
                  "Total amount of time, in nanoseconds spent doing sizes", "nanoseconds"),
              f.createIntGauge("stopCQsInProgress", "Current number of stopCQs being executed",
                  "stopCQs"),
              f.createIntCounter("stopCQs", "Total number of stopCQs completed successfully",
                  "stopCQs"),
              f.createIntCounter("stopCQFailures",
                  "Total number of stopCQ attempts that have failed", "stopCQs"),
              f.createIntCounter("stopCQTimeouts",
                  "Total number of stopCQ attempts that have timed out", "stopCQs"),
              f.createLongCounter("stopCQTime",
                  "Total amount of time, in nanoseconds spent doing stopCQs", "nanoseconds"),
              f.createIntGauge("txFailoversInProgress",
                  "Current number of txFailovers being executed", "txFailovers"),
              f.createIntCounter("txFailovers",
                  "Total number of txFailovers completed successfully", "txFailovers"),
              f.createIntCounter("txFailoverFailures",
                  "Total number of txFailover attempts that have failed", "txFailovers"),
              f.createIntCounter("txFailoverTimeouts",
                  "Total number of txFailover attempts that have timed out", "sizes"),
              f.createLongCounter("txFailoverTime",
                  "Total amount of time, in nanoseconds spent doing txFailovers", "nanoseconds"),
              f.createIntGauge("unregisterInterestsInProgress",
                  "Current number of unregisterInterests being executed", "unregisterInterests"),
              f.createIntCounter("unregisterInterests",
                  "Total number of unregisterInterests completed successfully",
                  "unregisterInterests"),
              f.createIntCounter("unregisterInterestFailures",
                  "Total number of unregisterInterest attempts that have failed",
                  "unregisterInterests"),
              f.createIntCounter("unregisterInterestTimeouts",
                  "Total number of unregisterInterest attempts that have timed out",
                  "unregisterInterests"),
              f.createLongCounter("unregisterInterestTime",
                  "Total amount of time, in nanoseconds spent doing unregisterInterests",
                  "nanoseconds"),

              f.createIntGauge("connections", "Current number of connections", "connections"),
              f.createIntCounter("connects", "Total number of times a connection has been created.",
                  "connects"),
              f.createIntCounter("disconnects",
                  "Total number of times a connection has been destroyed.", "disconnects"),
              f.createLongCounter("receivedBytes",
                  "Total number of bytes received (as responses) from server over a client-to-server connection.",
                  "bytes"),
              f.createLongCounter("sentBytes",
                  "Total number of bytes sent to server over a client-to-server connection.",
                  "bytes"),
              f.createIntGauge("messagesBeingReceived",
                  "Current number of message being received off the network or being processed after reception over a client-to-server connection.",
                  "messages"),
              f.createLongGauge("messageBytesBeingReceived",
                  "Current number of bytes consumed by messages being received or processed over a client-to-server connection.",
                  "bytes"),
          });

      sendType = f.createType("ClientSendStats", "Statistics about client to server communication",
          new StatisticDescriptor[] {
              f.createIntCounter("addPdxTypeSendFailures",
                  "Total number of addPdxType operation's request messages not sent successfully from the client to server",
                  "sends"),
              f.createIntCounter("addPdxTypeSendsSuccessful",
                  "Total number of addPdxType operation's request messages sent successfully from the client to server",
                  "sends"),
              f.createIntGauge("addPdxTypeSendsInProgress",
                  "Current number of addPdxType operation's request messages being send from the client to server",
                  "sends"),
              f.createLongCounter("addPdxTypeSendTime",
                  "Total amount of time, in nanoseconds spent sending addPdxType operation's request messages successfully/unsuccessfully from the client to server",
                  "nanoseconds"),
              f.createIntCounter("clearSendFailures",
                  "Total number of clear sends that have failed", "sends"),
              f.createIntCounter("clearSends",
                  "Total number of clear sends that have completed successfully", "sends"),
              f.createIntGauge("clearSendsInProgress",
                  "Current number of clear sends being executed", "sends"),
              f.createLongCounter("clearSendTime",
                  "Total amount of time, in nanoseconds spent doing clear sends", "nanoseconds"),
              f.createIntCounter("closeConSendFailures",
                  "Total number of closeCon sends that have failed", "sends"),
              f.createIntCounter("closeConSends",
                  "Total number of closeCon sends that have completed successfully", "sends"),
              f.createIntGauge("closeConSendsInProgress",
                  "Current number of closeCon sends being executed", "sends"),
              f.createLongCounter("closeConSendTime",
                  "Total amount of time, in nanoseconds spent doing closeCon sends", "nanoseconds"),
              f.createIntCounter("closeCQSendFailures",
                  "Total number of closeCQ sends that have failed", "sends"),
              f.createIntCounter("closeCQSends",
                  "Total number of closeCQ sends that have completed successfully", "sends"),
              f.createIntGauge("closeCQSendsInProgress",
                  "Current number of closeCQ sends being executed", "sends"),
              f.createLongCounter("closeCQSendTime",
                  "Total amount of time, in nanoseconds spent doing closeCQ sends", "nanoseconds"),
              f.createIntCounter("createCQSendFailures",
                  "Total number of createCQ sends that have failed", "sends"),
              f.createIntCounter("createCQSends",
                  "Total number of createCQ sends that have completed successfully", "sends"),
              f.createIntGauge("createCQSendsInProgress",
                  "Current number of createCQ sends being executed", "sends"),
              f.createLongCounter("createCQSendTime",
                  "Total amount of time, in nanoseconds spent doing createCQ sends", "nanoseconds"),
              f.createIntCounter("commitSendFailures",
                  "Total number of commit sends that have failed", "sends"),
              f.createIntCounter("commitSends",
                  "Total number of commit sends that have completed successfully",
                  "sends"),
              f.createIntGauge("commitSendsInProgress",
                  "Current number of commit sends being executed", "sends"),
              f.createLongCounter("commitSendTime",
                  "Total amount of time, in nanoseconds spent doing commits", "nanoseconds"),
              f.createIntCounter("containsKeySendFailures",
                  "Total number of containsKey sends that have failed", "sends"),
              f.createIntCounter("containsKeySends",
                  "Total number of containsKey sends that have completed successfully", "sends"),
              f.createIntGauge("containsKeySendsInProgress",
                  "Current number of containsKey sends being executed", "sends"),
              f.createLongCounter("containsKeySendTime",
                  "Total amount of time, in nanoseconds spent doing containsKey sends",
                  "nanoseconds"),
              f.createIntCounter("destroyRegionSendFailures",
                  "Total number of destroyRegion sends that have failed", "sends"),
              f.createIntCounter("destroyRegionSends",
                  "Total number of destroyRegion sends that have completed successfully", "sends"),
              f.createIntGauge("destroyRegionSendsInProgress",
                  "Current number of destroyRegion sends being executed", "sends"),
              f.createLongCounter("destroyRegionSendTime",
                  "Total amount of time, in nanoseconds spent doing destroyRegion sends",
                  "nanoseconds"),
              f.createIntCounter("destroySendFailures",
                  "Total number of destroy sends that have failed", "sends"),
              f.createIntCounter("destroySends",
                  "Total number of destroy sends that have completed successfully", "sends"),
              f.createIntGauge("destroySendsInProgress",
                  "Current number of destroy sends being executed", "sends"),
              f.createLongCounter("destroySendTime",
                  "Total amount of time, in nanoseconds spent doing destroy sends", "nanoseconds"),
              f.createIntCounter("executeFunctionSendFailures",
                  "Total number of Function sends that have failed", "sends"),
              f.createIntCounter("executeFunctionSends",
                  "Total number of Function sends that have completed successfully", "sends"),
              f.createIntGauge("executeFunctionSendsInProgress",
                  "Current number of Function sends being executed", "sends"),
              f.createLongCounter("executeFunctionSendTime",
                  "Total amount of time, in nanoseconds spent doing Function sends", "nanoseconds"),
              f.createIntCounter("gatewayBatchSendFailures",
                  "Total number of gatewayBatch sends that have failed", "sends"),
              f.createIntCounter("gatewayBatchSends",
                  "Total number of gatewayBatch sends that have completed successfully", "sends"),
              f.createIntGauge("gatewayBatchSendsInProgress",
                  "Current number of gatewayBatch sends being executed", "sends"),
              f.createLongCounter("gatewayBatchSendTime",
                  "Total amount of time, in nanoseconds spent doing gatewayBatch sends",
                  "nanoseconds"),
              f.createIntCounter("getAllSendFailures",
                  "Total number of getAll sends that have failed", "sends"),
              f.createIntCounter("getAllSends",
                  "Total number of getAll sends that have completed successfully", "sends"),
              f.createIntGauge("getAllSendsInProgress",
                  "Current number of getAll sends being executed", "sends"),
              f.createLongCounter("getAllSendTime",
                  "Total amount of time, in nanoseconds spent doing getAll sends", "nanoseconds"),
              f.createIntCounter("getClientPartitionAttributesSendFailures",
                  "Total number of getClientPartitionAttributes operation's request messages not sent successfully from the client to server",
                  "sends"),
              f.createIntGauge("getClientPartitionAttributesSendsInProgress",
                  "Current number of getClientPartitionAttributes operation's request messages being send from the client to server",
                  "sends"),
              f.createIntCounter("getClientPartitionAttributesSendsSuccessful",
                  "Total number of getClientPartitionAttributes operation's request messages sent successfully from the client to server",
                  "sends"),
              f.createLongCounter("getClientPartitionAttributesSendTime",
                  "Total amount of time, in nanoseconds spent sending getClientPartitionAttributes operation's request messages successfully/unsuccessfully from the client to server",
                  "nanoseconds"),
              f.createIntCounter("getClientPRMetadataSendFailures",
                  "Total number of getClientPRMetadata operation's request messages not sent successfully from the client to server",
                  "sends"),
              f.createIntGauge("getClientPRMetadataSendsInProgress",
                  "Current number of getClientPRMetadata operation's request messages being send from the client to server",
                  "sends"),
              f.createIntCounter("getClientPRMetadataSendsSuccessful",
                  "Total number of getClientPRMetadata operation's request messages sent successfully from the client to server",
                  "sends"),
              f.createLongCounter("getClientPRMetadataSendTime",
                  "Total amount of time, in nanoseconds spent sending getClientPRMetadata operation's request messages successfully/unsuccessfully from the client to server",
                  "nanoseconds"),
              f.createIntCounter("getPDXIdForTypeSendFailures",
                  "Total number of getPDXIdForType operation's request messages not sent successfully from the client to server",
                  "sends"),
              f.createIntGauge("getPDXIdForTypeSendsInProgress",
                  "Current number of getPDXIdForType operation's request messages being send from the client to server",
                  "sends"),
              f.createIntCounter("getPDXIdForTypeSendsSuccessful",
                  "Total number of getPDXIdForType operation's request messages sent successfully from the client to server",
                  "sends"),
              f.createLongCounter("getPDXIdForTypeSendTime",
                  "Total amount of time, in nanoseconds spent sending getPDXIdForType operation's request messages successfully/unsuccessfully from the client to server",
                  "nanoseconds"),
              f.createIntCounter("getPDXTypeByIdSendFailures",
                  "Total number of getPDXTypeById operation's request messages not sent successfully from the client to server",
                  "sends"),
              f.createIntGauge("getPDXTypeByIdSendsInProgress",
                  "Current number of getPDXTypeById operation's request messages being send from the client to server",
                  "sends"),
              f.createIntCounter("getPDXTypeByIdSendsSuccessful",
                  "Total number of getPDXTypeById operation's request messages sent successfully from the client to server",
                  "sends"),
              f.createLongCounter("getPDXTypeByIdSendTime",
                  "Total amount of time, in nanoseconds spent sending getPDXTypeById operation's request messages successfully/unsuccessfully from the client to server",
                  "nanoseconds"),
              f.createIntCounter("getEntrySendFailures",
                  "Total number of getEntry sends that have failed", "sends"),
              f.createIntCounter("getEntrySends",
                  "Total number of getEntry sends that have completed successfully", "sends"),
              f.createIntGauge("getEntrySendsInProgress",
                  "Current number of getEntry sends being executed", "sends"),
              f.createLongCounter("getEntrySendTime",
                  "Total amount of time, in nanoseconds spent sending getEntry messages",
                  "nanoseconds"),
              f.createIntCounter("getDurableCQsSendFailures",
                  "Total number of getDurableCQs sends that have failed", "sends"),
              f.createIntCounter("getDurableCQsSends",
                  "Total number of getDurableCQs sends that have completed successfully", "sends"),
              f.createIntGauge("getDurableCQsSendsInProgress",
                  "Current number of getDurableCQs sends being executed", "sends"),
              f.createLongCounter("getDurableCQsSendTime",
                  "Total amount of time, in nanoseconds spent doing getDurableCQs sends",
                  "nanoseconds"),
              f.createIntCounter("getSendFailures", "Total number of get sends that have failed",
                  "sends"),
              f.createIntCounter("getSends",
                  "Total number of get sends that have completed successfully", "sends"),
              f.createIntGauge("getSendsInProgress", "Current number of get sends being executed",
                  "sends"),
              f.createLongCounter("getSendTime",
                  "Total amount of time, in nanoseconds spent doing get sends", "nanoseconds"),
              f.createIntCounter("invalidateSendFailures",
                  "Total number of invalidate sends that have failed", "sends"),
              f.createIntCounter("invalidateSends",
                  "Total number of invalidate sends that have completed successfully", "sends"),
              f.createIntGauge("invalidateSendsInProgress",
                  "Current number of invalidate sends being executed", "sends"),
              f.createLongCounter("invalidateSendTime",
                  "Total amount of time, in nanoseconds spent doing invalidates", "nanoseconds"),
              f.createIntCounter("jtaSynchronizationSendFailures",
                  "Total number of jtaSynchronization sends that have failed", "sends"),
              f.createIntCounter("jtaSynchronizationSends",
                  "Total number of jtaSynchronization sends that have completed successfully",
                  "sends"),
              f.createIntGauge("jtaSynchronizationSendsInProgress",
                  "Current number of jtaSynchronization sends being executed", "sends"),
              f.createLongCounter("jtaSynchronizationSendTime",
                  "Total amount of time, in nanoseconds spent doing jtaSynchronizations",
                  "nanoseconds"),
              f.createIntCounter("keySetSendFailures",
                  "Total number of keySet sends that have failed", "sends"),
              f.createIntCounter("keySetSends",
                  "Total number of keySet sends that have completed successfully", "sends"),
              f.createIntGauge("keySetSendsInProgress",
                  "Current number of keySet sends being executed", "sends"),
              f.createLongCounter("keySetSendTime",
                  "Total amount of time, in nanoseconds spent doing keySet sends", "nanoseconds"),
              f.createIntCounter("makePrimarySendFailures",
                  "Total number of makePrimary sends that have failed", "sends"),
              f.createIntCounter("makePrimarySends",
                  "Total number of makePrimary sends that have completed successfully", "sends"),
              f.createIntGauge("makePrimarySendsInProgress",
                  "Current number of makePrimary sends being executed", "sends"),
              f.createLongCounter("makePrimarySendTime",
                  "Total amount of time, in nanoseconds spent doing makePrimary sends",
                  "nanoseconds"),
              f.createIntCounter("pingSendFailures", "Total number of ping sends that have failed",
                  "sends"),
              f.createIntCounter("pingSends",
                  "Total number of ping sends that have completed successfully", "sends"),
              f.createIntGauge("pingSendsInProgress", "Current number of ping sends being executed",
                  "sends"),
              f.createLongCounter("pingSendTime",
                  "Total amount of time, in nanoseconds spent doing ping sends", "nanoseconds"),
              f.createIntCounter("primaryAckSendFailures",
                  "Total number of primaryAck sends that have failed", "sends"),
              f.createIntCounter("primaryAckSends",
                  "Total number of primaryAck sends that have completed successfully", "sends"),
              f.createIntGauge("primaryAckSendsInProgress",
                  "Current number of primaryAck sends being executed", "sends"),
              f.createLongCounter("primaryAckSendTime",
                  "Total amount of time, in nanoseconds spent doing primaryAck sends",
                  "nanoseconds"),
              f.createIntCounter("putAllSendFailures",
                  "Total number of putAll sends that have failed", "sends"),
              f.createIntCounter("putAllSends",
                  "Total number of putAll sends that have completed successfully", "sends"),
              f.createIntGauge("putAllSendsInProgress",
                  "Current number of putAll sends being executed", "sends"),
              f.createLongCounter("putAllSendTime",
                  "Total amount of time, in nanoseconds spent doing putAll sends", "nanoseconds"),
              f.createIntCounter("putSendFailures", "Total number of put sends that have failed",
                  "sends"),
              f.createIntCounter("putSends",
                  "Total number of put sends that have completed successfully", "sends"),
              f.createIntGauge("putSendsInProgress", "Current number of put sends being executed",
                  "sends"),
              f.createLongCounter("putSendTime",
                  "Total amount of time, in nanoseconds spent doing put sends", "nanoseconds"),
              f.createIntCounter("querySendFailures",
                  "Total number of query sends that have failed", "sends"),
              f.createIntCounter("querySends",
                  "Total number of query sends that have completed successfully", "sends"),
              f.createIntGauge("querySendsInProgress",
                  "Current number of query sends being executed", "sends"),
              f.createLongCounter("querySendTime",
                  "Total amount of time, in nanoseconds spent doing query sends", "nanoseconds"),
              f.createIntCounter("readyForEventsSendFailures",
                  "Total number of readyForEvents sends that have failed", "sends"),
              f.createIntCounter("readyForEventsSends",
                  "Total number of readyForEvents sends that have completed successfully", "sends"),
              f.createIntGauge("readyForEventsSendsInProgress",
                  "Current number of readyForEvents sends being executed", "sends"),
              f.createLongCounter("readyForEventsSendTime",
                  "Total amount of time, in nanoseconds spent doing readyForEvents sends",
                  "nanoseconds"),
              f.createIntCounter("registerDataSerializersSendFailures",
                  "Total number of registerDataSerializers sends that have failed", "sends"),
              f.createIntCounter("registerDataSerializersSends",
                  "Total number of registerDataSerializers sends that have completed successfully",
                  "sends"),
              f.createIntGauge("registerDataSerializersSendInProgress",
                  "Current number of registerDataSerializers sends being executed", "sends"),
              f.createLongCounter("registerDataSerializersSendTime",
                  "Total amount of time, in nanoseconds spent doing registerDataSerializers sends",
                  "nanoseconds"),
              f.createIntCounter("registerInstantiatorsSendFailures",
                  "Total number of registerInstantiators sends that have failed", "sends"),
              f.createIntCounter("registerInstantiatorsSends",
                  "Total number of registerInstantiators sends that have completed successfully",
                  "sends"),
              f.createIntGauge("registerInstantiatorsSendsInProgress",
                  "Current number of registerInstantiators sends being executed", "sends"),
              f.createLongCounter("registerInstantiatorsSendTime",
                  "Total amount of time, in nanoseconds spent doing registerInstantiators sends",
                  "nanoseconds"),
              f.createIntCounter("registerInterestSendFailures",
                  "Total number of registerInterest sends that have failed", "sends"),
              f.createIntCounter("registerInterestSends",
                  "Total number of registerInterest sends that have completed successfully",
                  "sends"),
              f.createIntGauge("registerInterestSendsInProgress",
                  "Current number of registerInterest sends being executed", "sends"),
              f.createLongCounter("registerInterestSendTime",
                  "Total amount of time, in nanoseconds spent doing registerInterest sends",
                  "nanoseconds"),
              f.createIntCounter("removeAllSendFailures",
                  "Total number of removeAll sends that have failed", "sends"),
              f.createIntCounter("removeAllSends",
                  "Total number of removeAll sends that have completed successfully", "sends"),
              f.createIntGauge("removeAllSendsInProgress",
                  "Current number of removeAll sends being executed", "sends"),
              f.createLongCounter("removeAllSendTime",
                  "Total amount of time, in nanoseconds spent doing removeAll sends",
                  "nanoseconds"),
              f.createIntCounter("rollbackSendFailures",
                  "Total number of rollback sends that have failed", "sends"),
              f.createIntCounter("rollbackSends",
                  "Total number of rollback sends that have completed successfully", "sends"),
              f.createIntGauge("rollbackSendsInProgress",
                  "Current number of rollback sends being executed", "sends"),
              f.createLongCounter("rollbackSendTime",
                  "Total amount of time, in nanoseconds spent doing rollbacks", "nanoseconds"),
              f.createIntCounter("sizeSendFailures", "Total number of size sends that have failed",
                  "sends"),
              f.createIntCounter("sizeSends",
                  "Total number of size sends that have completed successfully",
                  "sends"),
              f.createIntGauge("sizeSendsInProgress", "Current number of size sends being executed",
                  "sends"),
              f.createLongCounter("sizeSendTime",
                  "Total amount of time, in nanoseconds spent doing sizes", "nanoseconds"),
              f.createIntCounter("stopCQSendFailures",
                  "Total number of stopCQ sends that have failed", "sends"),
              f.createIntCounter("stopCQSends",
                  "Total number of stopCQ sends that have completed successfully", "sends"),
              f.createIntGauge("stopCQSendsInProgress",
                  "Current number of stopCQ sends being executed", "sends"),
              f.createLongCounter("stopCQSendTime",
                  "Total amount of time, in nanoseconds spent doing stopCQ sends", "nanoseconds"),
              f.createIntCounter("txFailoverSendFailures",
                  "Total number of txFailover sends that have failed", "sends"),
              f.createIntCounter("txFailoverSends",
                  "Total number of txFailover sends that have completed successfully", "sends"),
              f.createIntGauge("txFailoverSendsInProgress",
                  "Current number of txFailover sends being executed", "sends"),
              f.createLongCounter("txFailoverSendTime",
                  "Total amount of time, in nanoseconds spent doing txFailovers", "nanoseconds"),
              f.createIntCounter("unregisterInterestSendFailures",
                  "Total number of unregisterInterest sends that have failed", "sends"),
              f.createIntCounter("unregisterInterestSends",
                  "Total number of unregisterInterest sends that have completed successfully",
                  "sends"),
              f.createIntGauge("unregisterInterestSendsInProgress",
                  "Current number of unregisterInterest sends being executed", "sends"),
              f.createLongCounter("unregisterInterestSendTime",
                  "Total amount of time, in nanoseconds spent doing unregisterInterest sends",
                  "nanoseconds"),
          });

      getInProgressId = type.nameToId("getsInProgress");
      getSendInProgressId = sendType.nameToId("getSendsInProgress");
      getSendFailedId = sendType.nameToId("getSendFailures");
      getSendId = sendType.nameToId("getSends");
      getSendDurationId = sendType.nameToId("getSendTime");
      getTimedOutId = type.nameToId("getTimeouts");
      getFailedId = type.nameToId("getFailures");
      getId = type.nameToId("gets");
      getDurationId = type.nameToId("getTime");
      putInProgressId = type.nameToId("putsInProgress");
      putSendInProgressId = sendType.nameToId("putSendsInProgress");
      putSendFailedId = sendType.nameToId("putSendFailures");
      putSendId = sendType.nameToId("putSends");
      putSendDurationId = sendType.nameToId("putSendTime");
      putTimedOutId = type.nameToId("putTimeouts");
      putFailedId = type.nameToId("putFailures");
      putId = type.nameToId("puts");
      putDurationId = type.nameToId("putTime");
      destroyInProgressId = type.nameToId("destroysInProgress");
      destroySendInProgressId = sendType.nameToId("destroySendsInProgress");
      destroySendFailedId = sendType.nameToId("destroySendFailures");
      destroySendId = sendType.nameToId("destroySends");
      destroySendDurationId = sendType.nameToId("destroySendTime");
      destroyTimedOutId = type.nameToId("destroyTimeouts");
      destroyFailedId = type.nameToId("destroyFailures");
      destroyId = type.nameToId("destroys");
      destroyDurationId = type.nameToId("destroyTime");
      destroyRegionInProgressId = type.nameToId("destroyRegionsInProgress");
      destroyRegionSendInProgressId = sendType.nameToId("destroyRegionSendsInProgress");
      destroyRegionSendFailedId = sendType.nameToId("destroyRegionSendFailures");
      destroyRegionSendId = sendType.nameToId("destroyRegionSends");
      destroyRegionSendDurationId = sendType.nameToId("destroyRegionSendTime");
      destroyRegionTimedOutId = type.nameToId("destroyRegionTimeouts");
      destroyRegionFailedId = type.nameToId("destroyRegionFailures");
      destroyRegionId = type.nameToId("destroyRegions");
      destroyRegionDurationId = type.nameToId("destroyRegionTime");
      clearInProgressId = type.nameToId("clearsInProgress");
      clearSendInProgressId = sendType.nameToId("clearSendsInProgress");
      clearSendFailedId = sendType.nameToId("clearSendFailures");
      clearSendId = sendType.nameToId("clearSends");
      clearSendDurationId = sendType.nameToId("clearSendTime");
      clearTimedOutId = type.nameToId("clearTimeouts");
      clearFailedId = type.nameToId("clearFailures");
      clearId = type.nameToId("clears");
      clearDurationId = type.nameToId("clearTime");
      containsKeyInProgressId = type.nameToId("containsKeysInProgress");
      containsKeySendInProgressId = sendType.nameToId("containsKeySendsInProgress");
      containsKeySendFailedId = sendType.nameToId("containsKeySendFailures");
      containsKeySendId = sendType.nameToId("containsKeySends");
      containsKeySendDurationId = sendType.nameToId("containsKeySendTime");
      containsKeyTimedOutId = type.nameToId("containsKeyTimeouts");
      containsKeyFailedId = type.nameToId("containsKeyFailures");
      containsKeyId = type.nameToId("containsKeys");
      containsKeyDurationId = type.nameToId("containsKeyTime");

      keySetInProgressId = type.nameToId("keySetsInProgress");
      keySetSendInProgressId = sendType.nameToId("keySetSendsInProgress");
      keySetSendFailedId = sendType.nameToId("keySetSendFailures");
      keySetSendId = sendType.nameToId("keySetSends");
      keySetSendDurationId = sendType.nameToId("keySetSendTime");
      keySetTimedOutId = type.nameToId("keySetTimeouts");
      keySetFailedId = type.nameToId("keySetFailures");
      keySetId = type.nameToId("keySets");
      keySetDurationId = type.nameToId("keySetTime");

      commitInProgressId = type.nameToId("commitsInProgress");
      commitSendInProgressId = sendType.nameToId("commitSendsInProgress");
      commitSendFailedId = sendType.nameToId("commitSendFailures");
      commitSendId = sendType.nameToId("commitSends");
      commitSendDurationId = sendType.nameToId("commitSendTime");
      commitDurationId = type.nameToId("commitTime");
      commitTimedOutId = type.nameToId("commitTimeouts");
      commitFailedId = type.nameToId("commitFailures");
      commitId = type.nameToId("commits");

      rollbackInProgressId = type.nameToId("rollbacksInProgress");
      rollbackSendInProgressId = sendType.nameToId("rollbackSendsInProgress");
      rollbackSendFailedId = sendType.nameToId("rollbackSendFailures");
      rollbackSendId = sendType.nameToId("rollbackSends");
      rollbackSendDurationId = sendType.nameToId("rollbackSendTime");
      rollbackDurationId = type.nameToId("rollbackTime");
      rollbackTimedOutId = type.nameToId("rollbackTimeouts");
      rollbackFailedId = type.nameToId("rollbackFailures");
      rollbackId = type.nameToId("rollbacks");

      getEntryInProgressId = type.nameToId("getEntrysInProgress");
      getEntrySendInProgressId = sendType.nameToId("getEntrySendsInProgress");
      getEntrySendFailedId = sendType.nameToId("getEntrySendFailures");
      getEntrySendId = sendType.nameToId("getEntrySends");
      getEntrySendDurationId = sendType.nameToId("getEntrySendTime");
      getEntryDurationId = type.nameToId("getEntryTime");
      getEntryTimedOutId = type.nameToId("getEntryTimeouts");
      getEntryFailedId = type.nameToId("getEntryFailures");
      getEntryId = type.nameToId("getEntrys");

      txSynchronizationInProgressId = type.nameToId("jtaSynchronizationsInProgress");
      txSynchronizationSendInProgressId = sendType.nameToId("jtaSynchronizationSendsInProgress");
      txSynchronizationSendFailedId = sendType.nameToId("jtaSynchronizationSendFailures");
      txSynchronizationSendId = sendType.nameToId("jtaSynchronizationSends");
      txSynchronizationSendDurationId = sendType.nameToId("jtaSynchronizationSendTime");
      txSynchronizationDurationId = type.nameToId("jtaSynchronizationTime");
      txSynchronizationTimedOutId = type.nameToId("jtaSynchronizationTimeouts");
      txSynchronizationFailedId = type.nameToId("jtaSynchronizationFailures");
      txSynchronizationId = type.nameToId("jtaSynchronizations");

      txFailoverInProgressId = type.nameToId("txFailoversInProgress");
      txFailoverSendInProgressId = sendType.nameToId("txFailoverSendsInProgress");
      txFailoverSendFailedId = sendType.nameToId("txFailoverSendFailures");
      txFailoverSendId = sendType.nameToId("txFailoverSends");
      txFailoverSendDurationId = sendType.nameToId("txFailoverSendTime");
      txFailoverDurationId = type.nameToId("txFailoverTime");
      txFailoverTimedOutId = type.nameToId("txFailoverTimeouts");
      txFailoverFailedId = type.nameToId("txFailoverFailures");
      txFailoverId = type.nameToId("txFailovers");

      sizeInProgressId = type.nameToId("sizesInProgress");
      sizeSendInProgressId = sendType.nameToId("sizeSendsInProgress");
      sizeSendFailedId = sendType.nameToId("sizeSendFailures");
      sizeSendId = sendType.nameToId("sizeSends");
      sizeSendDurationId = sendType.nameToId("sizeSendTime");
      sizeDurationId = type.nameToId("sizeTime");
      sizeTimedOutId = type.nameToId("sizeTimeouts");
      sizeFailedId = type.nameToId("sizeFailures");
      sizeId = type.nameToId("sizes");


      invalidateInProgressId = type.nameToId("invalidatesInProgress");
      invalidateSendInProgressId = sendType.nameToId("invalidateSendsInProgress");
      invalidateSendFailedId = sendType.nameToId("invalidateSendFailures");
      invalidateSendId = sendType.nameToId("invalidateSends");
      invalidateSendDurationId = sendType.nameToId("invalidateSendTime");
      invalidateDurationId = type.nameToId("invalidateTime");
      invalidateTimedOutId = type.nameToId("invalidateTimeouts");
      invalidateFailedId = type.nameToId("invalidateFailures");
      invalidateId = type.nameToId("invalidates");


      registerInterestInProgressId = type.nameToId("registerInterestsInProgress");
      registerInterestSendInProgressId = sendType.nameToId("registerInterestSendsInProgress");
      registerInterestSendFailedId = sendType.nameToId("registerInterestSendFailures");
      registerInterestSendId = sendType.nameToId("registerInterestSends");
      registerInterestSendDurationId = sendType.nameToId("registerInterestSendTime");
      registerInterestTimedOutId = type.nameToId("registerInterestTimeouts");
      registerInterestFailedId = type.nameToId("registerInterestFailures");
      registerInterestId = type.nameToId("registerInterests");
      registerInterestDurationId = type.nameToId("registerInterestTime");
      unregisterInterestInProgressId = type.nameToId("unregisterInterestsInProgress");
      unregisterInterestSendInProgressId = sendType.nameToId("unregisterInterestSendsInProgress");
      unregisterInterestSendFailedId = sendType.nameToId("unregisterInterestSendFailures");
      unregisterInterestSendId = sendType.nameToId("unregisterInterestSends");
      unregisterInterestSendDurationId = sendType.nameToId("unregisterInterestSendTime");
      unregisterInterestTimedOutId = type.nameToId("unregisterInterestTimeouts");
      unregisterInterestFailedId = type.nameToId("unregisterInterestFailures");
      unregisterInterestId = type.nameToId("unregisterInterests");
      unregisterInterestDurationId = type.nameToId("unregisterInterestTime");
      queryInProgressId = type.nameToId("querysInProgress");
      querySendInProgressId = sendType.nameToId("querySendsInProgress");
      querySendFailedId = sendType.nameToId("querySendFailures");
      querySendId = sendType.nameToId("querySends");
      querySendDurationId = sendType.nameToId("querySendTime");
      queryTimedOutId = type.nameToId("queryTimeouts");
      queryFailedId = type.nameToId("queryFailures");
      queryId = type.nameToId("querys");
      queryDurationId = type.nameToId("queryTime");
      createCQInProgressId = type.nameToId("createCQsInProgress");
      createCQSendInProgressId = sendType.nameToId("createCQSendsInProgress");
      createCQSendFailedId = sendType.nameToId("createCQSendFailures");
      createCQSendId = sendType.nameToId("createCQSends");
      createCQSendDurationId = sendType.nameToId("createCQSendTime");
      createCQTimedOutId = type.nameToId("createCQTimeouts");
      createCQFailedId = type.nameToId("createCQFailures");
      createCQId = type.nameToId("createCQs");
      createCQDurationId = type.nameToId("createCQTime");
      stopCQInProgressId = type.nameToId("stopCQsInProgress");
      stopCQSendInProgressId = sendType.nameToId("stopCQSendsInProgress");
      stopCQSendFailedId = sendType.nameToId("stopCQSendFailures");
      stopCQSendId = sendType.nameToId("stopCQSends");
      stopCQSendDurationId = sendType.nameToId("stopCQSendTime");
      stopCQTimedOutId = type.nameToId("stopCQTimeouts");
      stopCQFailedId = type.nameToId("stopCQFailures");
      stopCQId = type.nameToId("stopCQs");
      stopCQDurationId = type.nameToId("stopCQTime");
      closeCQInProgressId = type.nameToId("closeCQsInProgress");
      closeCQSendInProgressId = sendType.nameToId("closeCQSendsInProgress");
      closeCQSendFailedId = sendType.nameToId("closeCQSendFailures");
      closeCQSendId = sendType.nameToId("closeCQSends");
      closeCQSendDurationId = sendType.nameToId("closeCQSendTime");
      closeCQTimedOutId = type.nameToId("closeCQTimeouts");
      closeCQFailedId = type.nameToId("closeCQFailures");
      closeCQId = type.nameToId("closeCQs");
      closeCQDurationId = type.nameToId("closeCQTime");
      gatewayBatchInProgressId = type.nameToId("gatewayBatchsInProgress");
      gatewayBatchSendInProgressId = sendType.nameToId("gatewayBatchSendsInProgress");
      gatewayBatchSendFailedId = sendType.nameToId("gatewayBatchSendFailures");
      gatewayBatchSendId = sendType.nameToId("gatewayBatchSends");
      gatewayBatchSendDurationId = sendType.nameToId("gatewayBatchSendTime");
      gatewayBatchTimedOutId = type.nameToId("gatewayBatchTimeouts");
      gatewayBatchFailedId = type.nameToId("gatewayBatchFailures");
      gatewayBatchId = type.nameToId("gatewayBatchs");
      gatewayBatchDurationId = type.nameToId("gatewayBatchTime");
      getDurableCQsInProgressId = type.nameToId("getDurableCQsInProgress");
      getDurableCQsSendsInProgressId = sendType.nameToId("getDurableCQsSendsInProgress");
      getDurableCQsSendFailedId = sendType.nameToId("getDurableCQsSendFailures");
      getDurableCQsSendId = sendType.nameToId("getDurableCQsSends");
      getDurableCQsSendDurationId = sendType.nameToId("getDurableCQsSendTime");
      getDurableCQsTimedOutId = type.nameToId("getDurableCQsTimeouts");
      getDurableCQsFailedId = type.nameToId("getDurableCQsFailures");
      getDurableCQsId = type.nameToId("getDurableCQs");
      getDurableCQsDurationId = type.nameToId("getDurableCQsTime");
      readyForEventsInProgressId = type.nameToId("readyForEventsInProgress");
      readyForEventsSendInProgressId = sendType.nameToId("readyForEventsSendsInProgress");
      readyForEventsSendFailedId = sendType.nameToId("readyForEventsSendFailures");
      readyForEventsSendId = sendType.nameToId("readyForEventsSends");
      readyForEventsSendDurationId = sendType.nameToId("readyForEventsSendTime");
      readyForEventsTimedOutId = type.nameToId("readyForEventsTimeouts");
      readyForEventsFailedId = type.nameToId("readyForEventsFailures");
      readyForEventsId = type.nameToId("readyForEvents");
      readyForEventsDurationId = type.nameToId("readyForEventsTime");
      makePrimaryInProgressId = type.nameToId("makePrimarysInProgress");
      makePrimarySendInProgressId = sendType.nameToId("makePrimarySendsInProgress");
      makePrimarySendFailedId = sendType.nameToId("makePrimarySendFailures");
      makePrimarySendId = sendType.nameToId("makePrimarySends");
      makePrimarySendDurationId = sendType.nameToId("makePrimarySendTime");
      makePrimaryTimedOutId = type.nameToId("makePrimaryTimeouts");
      makePrimaryFailedId = type.nameToId("makePrimaryFailures");
      makePrimaryId = type.nameToId("makePrimarys");
      makePrimaryDurationId = type.nameToId("makePrimaryTime");

      closeConInProgressId = type.nameToId("closeConsInProgress");
      closeConSendInProgressId = sendType.nameToId("closeConSendsInProgress");
      closeConSendFailedId = sendType.nameToId("closeConSendFailures");
      closeConSendId = sendType.nameToId("closeConSends");
      closeConSendDurationId = sendType.nameToId("closeConSendTime");
      closeConTimedOutId = type.nameToId("closeConTimeouts");
      closeConFailedId = type.nameToId("closeConFailures");
      closeConId = type.nameToId("closeCons");
      closeConDurationId = type.nameToId("closeConTime");

      primaryAckInProgressId = type.nameToId("primaryAcksInProgress");
      primaryAckSendInProgressId = sendType.nameToId("primaryAckSendsInProgress");
      primaryAckSendFailedId = sendType.nameToId("primaryAckSendFailures");
      primaryAckSendId = sendType.nameToId("primaryAckSends");
      primaryAckSendDurationId = sendType.nameToId("primaryAckSendTime");
      primaryAckTimedOutId = type.nameToId("primaryAckTimeouts");
      primaryAckFailedId = type.nameToId("primaryAckFailures");
      primaryAckId = type.nameToId("primaryAcks");
      primaryAckDurationId = type.nameToId("primaryAckTime");

      pingInProgressId = type.nameToId("pingsInProgress");
      pingSendInProgressId = sendType.nameToId("pingSendsInProgress");
      pingSendFailedId = sendType.nameToId("pingSendFailures");
      pingSendId = sendType.nameToId("pingSends");
      pingSendDurationId = sendType.nameToId("pingSendTime");
      pingTimedOutId = type.nameToId("pingTimeouts");
      pingFailedId = type.nameToId("pingFailures");
      pingId = type.nameToId("pings");
      pingDurationId = type.nameToId("pingTime");

      registerInstantiatorsInProgressId = type.nameToId("registerInstantiatorsInProgress");
      registerInstantiatorsSendInProgressId =
          sendType.nameToId("registerInstantiatorsSendsInProgress");
      registerInstantiatorsSendFailedId = sendType.nameToId("registerInstantiatorsSendFailures");
      registerInstantiatorsSendId = sendType.nameToId("registerInstantiatorsSends");
      registerInstantiatorsSendDurationId = sendType.nameToId("registerInstantiatorsSendTime");
      registerInstantiatorsTimedOutId = type.nameToId("registerInstantiatorsTimeouts");
      registerInstantiatorsFailedId = type.nameToId("registerInstantiatorsFailures");
      registerInstantiatorsId = type.nameToId("registerInstantiators");
      registerInstantiatorsDurationId = type.nameToId("registerInstantiatorsTime");

      registerDataSerializersInProgressId = type.nameToId("registerDataSerializersInProgress");
      registerDataSerializersSendInProgressId =
          sendType.nameToId("registerDataSerializersSendInProgress");
      registerDataSerializersSendFailedId =
          sendType.nameToId("registerDataSerializersSendFailures");
      registerDataSerializersSendId = sendType.nameToId("registerDataSerializersSends");
      registerDataSerializersSendDurationId = sendType.nameToId("registerDataSerializersSendTime");
      registerDataSerializersTimedOutId = type.nameToId("registerDataSerializersTimeouts");
      registerDataSerializersFailedId = type.nameToId("registerDataSerializersFailures");
      registerDataSerializersId = type.nameToId("registerDataSerializers");
      registerDataSerializersDurationId = type.nameToId("registerDataSerializersTime");

      putAllInProgressId = type.nameToId("putAllsInProgress");
      putAllSendInProgressId = sendType.nameToId("putAllSendsInProgress");
      putAllSendFailedId = sendType.nameToId("putAllSendFailures");
      putAllSendId = sendType.nameToId("putAllSends");
      putAllSendDurationId = sendType.nameToId("putAllSendTime");
      putAllTimedOutId = type.nameToId("putAllTimeouts");
      putAllFailedId = type.nameToId("putAllFailures");
      putAllId = type.nameToId("putAlls");
      putAllDurationId = type.nameToId("putAllTime");

      removeAllInProgressId = type.nameToId("removeAllsInProgress");
      removeAllSendInProgressId = sendType.nameToId("removeAllSendsInProgress");
      removeAllSendFailedId = sendType.nameToId("removeAllSendFailures");
      removeAllSendId = sendType.nameToId("removeAllSends");
      removeAllSendDurationId = sendType.nameToId("removeAllSendTime");
      removeAllTimedOutId = type.nameToId("removeAllTimeouts");
      removeAllFailedId = type.nameToId("removeAllFailures");
      removeAllId = type.nameToId("removeAlls");
      removeAllDurationId = type.nameToId("removeAllTime");

      getAllInProgressId = type.nameToId("getAllsInProgress");
      getAllSendInProgressId = sendType.nameToId("getAllSendsInProgress");
      getAllSendFailedId = sendType.nameToId("getAllSendFailures");
      getAllSendId = sendType.nameToId("getAllSends");
      getAllSendDurationId = sendType.nameToId("getAllSendTime");
      getAllTimedOutId = type.nameToId("getAllTimeouts");
      getAllFailedId = type.nameToId("getAllFailures");
      getAllId = type.nameToId("getAlls");
      getAllDurationId = type.nameToId("getAllTime");

      connectionsId = type.nameToId("connections");
      connectsId = type.nameToId("connects");
      disconnectsId = type.nameToId("disconnects");

      receivedBytesId = type.nameToId("receivedBytes");
      sentBytesId = type.nameToId("sentBytes");
      messagesBeingReceivedId = type.nameToId("messagesBeingReceived");
      messageBytesBeingReceivedId = type.nameToId("messageBytesBeingReceived");

      executeFunctionInProgressId = type.nameToId("executeFunctionsInProgress");
      executeFunctionSendInProgressId = sendType.nameToId("executeFunctionSendsInProgress");
      executeFunctionSendFailedId = sendType.nameToId("executeFunctionSendFailures");
      executeFunctionSendId = sendType.nameToId("executeFunctionSends");
      executeFunctionSendDurationId = sendType.nameToId("executeFunctionSendTime");
      executeFunctionTimedOutId = type.nameToId("executeFunctionTimeouts");
      executeFunctionFailedId = type.nameToId("executeFunctionFailures");
      executeFunctionId = type.nameToId("executeFunctions");
      executeFunctionDurationId = type.nameToId("executeFunctionTime");

      getClientPRMetadataInProgressId = type.nameToId("getClientPRMetadataInProgress");
      getClientPRMetadataSendInProgressId = sendType.nameToId("getClientPRMetadataSendsInProgress");
      getClientPRMetadataSendFailedId = sendType.nameToId("getClientPRMetadataSendFailures");
      getClientPRMetadataSendId = sendType.nameToId("getClientPRMetadataSendsSuccessful");
      getClientPRMetadataSendDurationId = sendType.nameToId("getClientPRMetadataSendTime");
      getClientPRMetadataTimedOutId = type.nameToId("getClientPRMetadataTimeouts");
      getClientPRMetadataFailedId = type.nameToId("getClientPRMetadataFailures");
      getClientPRMetadataId = type.nameToId("getClientPRMetadataSuccessful");
      getClientPRMetadataDurationId = type.nameToId("getClientPRMetadataTime");

      getClientPartitionAttributesInProgressId =
          type.nameToId("getClientPartitionAttributesInProgress");
      getClientPartitionAttributesSendInProgressId =
          sendType.nameToId("getClientPartitionAttributesSendsInProgress");
      getClientPartitionAttributesSendFailedId =
          sendType.nameToId("getClientPartitionAttributesSendFailures");
      getClientPartitionAttributesSendId =
          sendType.nameToId("getClientPartitionAttributesSendsSuccessful");
      getClientPartitionAttributesSendDurationId =
          sendType.nameToId("getClientPartitionAttributesSendTime");
      getClientPartitionAttributesTimedOutId =
          type.nameToId("getClientPartitionAttributesTimeouts");
      getClientPartitionAttributesFailedId = type.nameToId("getClientPartitionAttributesFailures");
      getClientPartitionAttributesId = type.nameToId("getClientPartitionAttributesSuccessful");
      getClientPartitionAttributesDurationId = type.nameToId("getClientPartitionAttributesTime");

      getPDXTypeByIdInProgressId = type.nameToId("getPDXTypeByIdInProgress");
      getPDXTypeByIdSendInProgressId = sendType.nameToId("getPDXTypeByIdSendsInProgress");
      getPDXTypeByIdSendFailedId = sendType.nameToId("getPDXTypeByIdSendFailures");
      getPDXTypeByIdSendId = sendType.nameToId("getPDXTypeByIdSendsSuccessful");
      getPDXTypeByIdSendDurationId = sendType.nameToId("getPDXTypeByIdSendTime");
      getPDXTypeByIdTimedOutId = type.nameToId("getPDXTypeByIdTimeouts");
      getPDXTypeByIdFailedId = type.nameToId("getPDXTypeByIdFailures");
      getPDXTypeByIdId = type.nameToId("getPDXTypeByIdSuccessful");
      getPDXTypeByIdDurationId = type.nameToId("getPDXTypeByIdTime");

      getPDXIdForTypeInProgressId = type.nameToId("getPDXIdForTypeInProgress");
      getPDXIdForTypeSendInProgressId = sendType.nameToId("getPDXIdForTypeSendsInProgress");
      getPDXIdForTypeSendFailedId = sendType.nameToId("getPDXIdForTypeSendFailures");
      getPDXIdForTypeSendId = sendType.nameToId("getPDXIdForTypeSendsSuccessful");
      getPDXIdForTypeSendDurationId = sendType.nameToId("getPDXIdForTypeSendTime");
      getPDXIdForTypeTimedOutId = type.nameToId("getPDXIdForTypeTimeouts");
      getPDXIdForTypeFailedId = type.nameToId("getPDXIdForTypeFailures");
      getPDXIdForTypeId = type.nameToId("getPDXIdForTypeSuccessful");
      getPDXIdForTypeDurationId = type.nameToId("getPDXIdForTypeTime");

      addPdxTypeInProgressId = type.nameToId("addPdxTypeInProgress");
      addPdxTypeSendInProgressId = sendType.nameToId("addPdxTypeSendsInProgress");
      addPdxTypeSendFailedId = sendType.nameToId("addPdxTypeSendFailures");
      addPdxTypeSendId = sendType.nameToId("addPdxTypeSendsSuccessful");
      addPdxTypeSendDurationId = sendType.nameToId("addPdxTypeSendTime");
      addPdxTypeTimedOutId = type.nameToId("addPdxTypeTimeouts");
      addPdxTypeFailedId = type.nameToId("addPdxTypeFailures");
      addPdxTypeId = type.nameToId("addPdxTypeSuccessful");
      addPdxTypeDurationId = type.nameToId("addPdxTypeTime");


      opIds = new int[] {getId, putId, destroyId, destroyRegionId, clearId, containsKeyId, keySetId,
          registerInterestId, unregisterInterestId, queryId, createCQId, stopCQId, closeCQId,
          gatewayBatchId, readyForEventsId, makePrimaryId, closeConId, primaryAckId, pingId,
          putAllId, removeAllId, getAllId, registerInstantiatorsId, executeFunctionId,
          getClientPRMetadataId, getClientPartitionAttributesId, getPDXTypeByIdId,
          getPDXIdForTypeId, addPdxTypeId};
    } catch (RuntimeException t) {
      t.printStackTrace();
      throw t;
    }
  }

  private static long getStatTime() {
    return DistributionStats.getStatTime();
  }

  // instance fields
  private final Statistics stats;
  private final Statistics sendStats;
  private final PoolStats poolStats;

  public ConnectionStats(StatisticsFactory factory, String prefix, String name,
      PoolStats poolStats) {
    this.stats = factory.createAtomicStatistics(type, prefix + "Stats-" + name);
    this.sendStats = factory.createAtomicStatistics(sendType, prefix + "SendStats-" + name);
    this.poolStats = poolStats;
  }

  /**
   * Records that the specified get is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endGetSend} and
   * {@link #endGet}.
   *
   * @return the start time of this get
   */
  public long startGet() {
    this.stats.incInt(getInProgressId, 1);
    this.sendStats.incInt(getSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the get has completed
   *
   * @param startTime the value returned by {@link #startGet}.
   * @param failed true if the send of the get failed
   */
  public void endGetSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(getSendInProgressId, -1);
    int endGetSendId;
    if (failed) {
      endGetSendId = getSendFailedId;
    } else {
      endGetSendId = getSendId;
    }
    this.sendStats.incInt(endGetSendId, 1);
    this.sendStats.incLong(getSendDurationId, duration);
  }

  /**
   * Records that the specified get has ended
   *
   * @param startTime the value returned by {@link #startGet}.
   * @param timedOut true if get timed out
   * @param failed true if get failed
   */
  public void endGet(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(getInProgressId, -1);
    int endGetId;
    if (timedOut) {
      endGetId = getTimedOutId;
    } else if (failed) {
      endGetId = getFailedId;
    } else {
      endGetId = getId;
    }
    this.stats.incLong(endGetId, 1L);
    this.stats.incLong(getDurationId, duration);
  }

  public int getGets() {
    return this.stats.getInt(getId);
  }

  public long getGetDuration() {
    return this.stats.getLong(getDurationId);
  }

  /**
   * Records that the specified put is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endPutSend} and
   * {@link #endPut}.
   *
   * @return the start time of this put
   */
  public long startPut() {
    this.stats.incInt(putInProgressId, 1);
    this.sendStats.incInt(putSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the put has completed
   *
   * @param startTime the value returned by {@link #startPut}.
   * @param failed true if the send of the put failed
   */
  public void endPutSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(putSendInProgressId, -1);
    int endPutSendId;
    if (failed) {
      endPutSendId = putSendFailedId;
    } else {
      endPutSendId = putSendId;
    }
    this.sendStats.incInt(endPutSendId, 1);
    this.sendStats.incLong(putSendDurationId, duration);
  }

  /**
   * Records that the specified put has ended
   *
   * @param startTime the value returned by {@link #startPut}.
   * @param timedOut true if put timed out
   * @param failed true if put failed
   */
  public void endPut(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(putInProgressId, -1);
    int endPutId;
    if (timedOut) {
      endPutId = putTimedOutId;
    } else if (failed) {
      endPutId = putFailedId;
    } else {
      endPutId = putId;
    }
    this.stats.incLong(endPutId, 1L);
    this.stats.incLong(putDurationId, duration);
  }

  public int getPuts() {
    return this.stats.getInt(putId);
  }

  public long getPutDuration() {
    return this.stats.getLong(putDurationId);
  }

  /**
   * Records that the specified destroy is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endDestroySend} and
   * {@link #endDestroy}.
   *
   * @return the start time of this destroy
   */
  public long startDestroy() {
    this.stats.incInt(destroyInProgressId, 1);
    this.sendStats.incInt(destroySendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the destroy has completed
   *
   * @param startTime the value returned by {@link #startDestroy}.
   * @param failed true if the send of the destroy failed
   */
  public void endDestroySend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(destroySendInProgressId, -1);
    int endDestroySendId;
    if (failed) {
      endDestroySendId = destroySendFailedId;
    } else {
      endDestroySendId = destroySendId;
    }
    this.sendStats.incInt(endDestroySendId, 1);
    this.sendStats.incLong(destroySendDurationId, duration);
  }

  /**
   * Records that the specified destroy has ended
   *
   * @param startTime the value returned by {@link #startDestroy}.
   * @param timedOut true if destroy timed out
   * @param failed true if destroy failed
   */
  public void endDestroy(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(destroyInProgressId, -1);
    int endDestroyId;
    if (timedOut) {
      endDestroyId = destroyTimedOutId;
    } else if (failed) {
      endDestroyId = destroyFailedId;
    } else {
      endDestroyId = destroyId;
    }
    this.stats.incLong(endDestroyId, 1);
    this.stats.incLong(destroyDurationId, duration);
  }

  /**
   * Records that the specified destroyRegion is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endDestroyRegionSend} and
   * {@link #endDestroyRegion}.
   *
   * @return the start time of this destroyRegion
   */
  public long startDestroyRegion() {
    this.stats.incInt(destroyRegionInProgressId, 1);
    this.sendStats.incInt(destroyRegionSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the destroyRegion has completed
   *
   * @param startTime the value returned by {@link #startDestroyRegion}.
   * @param failed true if the send of the destroyRegion failed
   */
  public void endDestroyRegionSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(destroyRegionSendInProgressId, -1);
    int endDestroyRegionSendId;
    if (failed) {
      endDestroyRegionSendId = destroyRegionSendFailedId;
    } else {
      endDestroyRegionSendId = destroyRegionSendId;
    }
    this.sendStats.incInt(endDestroyRegionSendId, 1);
    this.sendStats.incLong(destroyRegionSendDurationId, duration);
  }

  /**
   * Records that the specified destroyRegion has ended
   *
   * @param startTime the value returned by {@link #startDestroyRegion}.
   * @param timedOut true if destroyRegion timed out
   * @param failed true if destroyRegion failed
   */
  public void endDestroyRegion(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(destroyRegionInProgressId, -1);
    int endDestroyRegionId;
    if (timedOut) {
      endDestroyRegionId = destroyRegionTimedOutId;
    } else if (failed) {
      endDestroyRegionId = destroyRegionFailedId;
    } else {
      endDestroyRegionId = destroyRegionId;
    }
    this.stats.incInt(endDestroyRegionId, 1);
    this.stats.incLong(destroyRegionDurationId, duration);
  }

  /**
   * Records that the specified clear is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endClearSend} and
   * {@link #endClear}.
   *
   * @return the start time of this clear
   */
  public long startClear() {
    this.stats.incInt(clearInProgressId, 1);
    this.sendStats.incInt(clearSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the clear has completed
   *
   * @param startTime the value returned by {@link #startClear}.
   * @param failed true if the send of the clear failed
   */
  public void endClearSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(clearSendInProgressId, -1);
    int endClearSendId;
    if (failed) {
      endClearSendId = clearSendFailedId;
    } else {
      endClearSendId = clearSendId;
    }
    this.sendStats.incInt(endClearSendId, 1);
    this.sendStats.incLong(clearSendDurationId, duration);
  }

  /**
   * Records that the specified clear has ended
   *
   * @param startTime the value returned by {@link #startClear}.
   * @param timedOut true if clear timed out
   * @param failed true if clear failed
   */
  public void endClear(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(clearInProgressId, -1);
    int endClearId;
    if (timedOut) {
      endClearId = clearTimedOutId;
    } else if (failed) {
      endClearId = clearFailedId;
    } else {
      endClearId = clearId;
    }
    this.stats.incLong(endClearId, 1L);
    this.stats.incLong(clearDurationId, duration);
  }

  /**
   * Records that the specified containsKey is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endContainsKeySend} and
   * {@link #endContainsKey}.
   *
   * @return the start time of this containsKey
   */
  public long startContainsKey() {
    this.stats.incInt(containsKeyInProgressId, 1);
    this.sendStats.incInt(containsKeySendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the containsKey has completed
   *
   * @param startTime the value returned by {@link #startContainsKey}.
   * @param failed true if the send of the containsKey failed
   */
  public void endContainsKeySend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(containsKeySendInProgressId, -1);
    int endContainsKeySendId;
    if (failed) {
      endContainsKeySendId = containsKeySendFailedId;
    } else {
      endContainsKeySendId = containsKeySendId;
    }
    this.sendStats.incInt(endContainsKeySendId, 1);
    this.sendStats.incLong(containsKeySendDurationId, duration);
  }

  /**
   * Records that the specified containsKey has ended
   *
   * @param startTime the value returned by {@link #startContainsKey}.
   * @param timedOut true if containsKey timed out
   * @param failed true if containsKey failed
   */
  public void endContainsKey(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(containsKeyInProgressId, -1);
    int endContainsKeyId;
    if (timedOut) {
      endContainsKeyId = containsKeyTimedOutId;
    } else if (failed) {
      endContainsKeyId = containsKeyFailedId;
    } else {
      endContainsKeyId = containsKeyId;
    }
    this.stats.incInt(endContainsKeyId, 1);
    this.stats.incLong(containsKeyDurationId, duration);
  }

  /**
   * Records that the specified keySet is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endKeySetSend} and
   * {@link #endKeySet}.
   *
   * @return the start time of this keySet
   */
  public long startKeySet() {
    this.stats.incInt(keySetInProgressId, 1);
    this.sendStats.incInt(keySetSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the keySet has completed
   *
   * @param startTime the value returned by {@link #startKeySet}.
   * @param failed true if the send of the keySet failed
   */
  public void endKeySetSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(keySetSendInProgressId, -1);
    int endKeySetSendId;
    if (failed) {
      endKeySetSendId = keySetSendFailedId;
    } else {
      endKeySetSendId = keySetSendId;
    }
    this.sendStats.incInt(endKeySetSendId, 1);
    this.sendStats.incLong(keySetSendDurationId, duration);
  }

  /**
   * Records that the specified keySet has ended
   *
   * @param startTime the value returned by {@link #startKeySet}.
   * @param timedOut true if keySet timed out
   * @param failed true if keySet failed
   */
  public void endKeySet(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(keySetInProgressId, -1);
    int endKeySetId;
    if (timedOut) {
      endKeySetId = keySetTimedOutId;
    } else if (failed) {
      endKeySetId = keySetFailedId;
    } else {
      endKeySetId = keySetId;
    }
    this.stats.incInt(endKeySetId, 1);
    this.stats.incLong(keySetDurationId, duration);
  }

  /**
   * Records that the specified registerInterest is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endRegisterInterestSend}
   * and {@link #endRegisterInterest}.
   *
   * @return the start time of this registerInterest
   */
  public long startRegisterInterest() {
    this.stats.incInt(registerInterestInProgressId, 1);
    this.sendStats.incInt(registerInterestSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the registerInterest has completed
   *
   * @param startTime the value returned by {@link #startRegisterInterest}.
   * @param failed true if the send of the registerInterest failed
   */
  public void endRegisterInterestSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(registerInterestSendInProgressId, -1);
    int endRegisterInterestSendId;
    if (failed) {
      endRegisterInterestSendId = registerInterestSendFailedId;
    } else {
      endRegisterInterestSendId = registerInterestSendId;
    }
    this.sendStats.incInt(endRegisterInterestSendId, 1);
    this.sendStats.incLong(registerInterestSendDurationId, duration);
  }

  /**
   * Records that the specified registerInterest has ended
   *
   * @param startTime the value returned by {@link #startRegisterInterest}.
   * @param timedOut true if registerInterest timed out
   * @param failed true if registerInterest failed
   */
  public void endRegisterInterest(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(registerInterestInProgressId, -1);
    int endRegisterInterestId;
    if (timedOut) {
      endRegisterInterestId = registerInterestTimedOutId;
    } else if (failed) {
      endRegisterInterestId = registerInterestFailedId;
    } else {
      endRegisterInterestId = registerInterestId;
    }
    this.stats.incInt(endRegisterInterestId, 1);
    this.stats.incLong(registerInterestDurationId, duration);
  }

  /**
   * Records that the specified unregisterInterest is starting
   * <p>
   * Note: for every call of this method the caller must also call
   * {@link #endUnregisterInterestSend} and {@link #endUnregisterInterest}.
   *
   * @return the start time of this unregisterInterest
   */
  public long startUnregisterInterest() {
    this.stats.incInt(unregisterInterestInProgressId, 1);
    this.sendStats.incInt(unregisterInterestSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the unregisterInterest has completed
   *
   * @param startTime the value returned by {@link #startUnregisterInterest}.
   * @param failed true if the send of the unregisterInterest failed
   */
  public void endUnregisterInterestSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(unregisterInterestSendInProgressId, -1);
    int endUnregisterInterestSendId;
    if (failed) {
      endUnregisterInterestSendId = unregisterInterestSendFailedId;
    } else {
      endUnregisterInterestSendId = unregisterInterestSendId;
    }
    this.sendStats.incInt(endUnregisterInterestSendId, 1);
    this.sendStats.incLong(unregisterInterestSendDurationId, duration);
  }

  /**
   * Records that the specified unregisterInterest has ended
   *
   * @param startTime the value returned by {@link #startUnregisterInterest}.
   * @param timedOut true if unregisterInterest timed out
   * @param failed true if unregisterInterest failed
   */
  public void endUnregisterInterest(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(unregisterInterestInProgressId, -1);
    int endUnregisterInterestId;
    if (timedOut) {
      endUnregisterInterestId = unregisterInterestTimedOutId;
    } else if (failed) {
      endUnregisterInterestId = unregisterInterestFailedId;
    } else {
      endUnregisterInterestId = unregisterInterestId;
    }
    this.stats.incInt(endUnregisterInterestId, 1);
    this.stats.incLong(unregisterInterestDurationId, duration);
  }

  /**
   * Records that the specified query is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endQuerySend} and
   * {@link #endQuery}.
   *
   * @return the start time of this query
   */
  public long startQuery() {
    this.stats.incInt(queryInProgressId, 1);
    this.sendStats.incInt(querySendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the query has completed
   *
   * @param startTime the value returned by {@link #startQuery}.
   * @param failed true if the send of the query failed
   */
  public void endQuerySend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(querySendInProgressId, -1);
    int endQuerySendId;
    if (failed) {
      endQuerySendId = querySendFailedId;
    } else {
      endQuerySendId = querySendId;
    }
    this.sendStats.incInt(endQuerySendId, 1);
    this.sendStats.incLong(querySendDurationId, duration);
  }

  /**
   * Records that the specified query has ended
   *
   * @param startTime the value returned by {@link #startQuery}.
   * @param timedOut true if query timed out
   * @param failed true if query failed
   */
  public void endQuery(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(queryInProgressId, -1);
    int endQueryId;
    if (timedOut) {
      endQueryId = queryTimedOutId;
    } else if (failed) {
      endQueryId = queryFailedId;
    } else {
      endQueryId = queryId;
    }
    this.stats.incInt(endQueryId, 1);
    this.stats.incLong(queryDurationId, duration);
  }

  /**
   * Records that the specified createCQ is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endCreateCQSend} and
   * {@link #endCreateCQ}.
   *
   * @return the start time of this createCQ
   */
  public long startCreateCQ() {
    this.stats.incInt(createCQInProgressId, 1);
    this.sendStats.incInt(createCQSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the createCQ has completed
   *
   * @param startTime the value returned by {@link #startCreateCQ}.
   * @param failed true if the send of the createCQ failed
   */
  public void endCreateCQSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(createCQSendInProgressId, -1);
    int endCreateCQSendId;
    if (failed) {
      endCreateCQSendId = createCQSendFailedId;
    } else {
      endCreateCQSendId = createCQSendId;
    }
    this.sendStats.incInt(endCreateCQSendId, 1);
    this.sendStats.incLong(createCQSendDurationId, duration);
  }

  /**
   * Records that the specified createCQ has ended
   *
   * @param startTime the value returned by {@link #startCreateCQ}.
   * @param timedOut true if createCQ timed out
   * @param failed true if createCQ failed
   */
  public void endCreateCQ(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(createCQInProgressId, -1);
    int endCreateCQId;
    if (timedOut) {
      endCreateCQId = createCQTimedOutId;
    } else if (failed) {
      endCreateCQId = createCQFailedId;
    } else {
      endCreateCQId = createCQId;
    }
    this.stats.incInt(endCreateCQId, 1);
    this.stats.incLong(createCQDurationId, duration);
  }

  /**
   * Records that the specified stopCQ is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endStopCQSend} and
   * {@link #endStopCQ}.
   *
   * @return the start time of this stopCQ
   */
  public long startStopCQ() {
    this.stats.incInt(stopCQInProgressId, 1);
    this.sendStats.incInt(stopCQSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the stopCQ has completed
   *
   * @param startTime the value returned by {@link #startStopCQ}.
   * @param failed true if the send of the stopCQ failed
   */
  public void endStopCQSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(stopCQSendInProgressId, -1);
    int endStopCQSendId;
    if (failed) {
      endStopCQSendId = stopCQSendFailedId;
    } else {
      endStopCQSendId = stopCQSendId;
    }
    this.sendStats.incInt(endStopCQSendId, 1);
    this.sendStats.incLong(stopCQSendDurationId, duration);
  }

  /**
   * Records that the specified stopCQ has ended
   *
   * @param startTime the value returned by {@link #startStopCQ}.
   * @param timedOut true if stopCQ timed out
   * @param failed true if stopCQ failed
   */
  public void endStopCQ(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(stopCQInProgressId, -1);
    int endStopCQId;
    if (timedOut) {
      endStopCQId = stopCQTimedOutId;
    } else if (failed) {
      endStopCQId = stopCQFailedId;
    } else {
      endStopCQId = stopCQId;
    }
    this.stats.incInt(endStopCQId, 1);
    this.stats.incLong(stopCQDurationId, duration);
  }

  /**
   * Records that the specified closeCQ is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endCloseCQSend} and
   * {@link #endCloseCQ}.
   *
   * @return the start time of this closeCQ
   */
  public long startCloseCQ() {
    this.stats.incInt(closeCQInProgressId, 1);
    this.sendStats.incInt(closeCQSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the closeCQ has completed
   *
   * @param startTime the value returned by {@link #startCloseCQ}.
   * @param failed true if the send of the closeCQ failed
   */
  public void endCloseCQSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(closeCQSendInProgressId, -1);
    int endCloseCQSendId;
    if (failed) {
      endCloseCQSendId = closeCQSendFailedId;
    } else {
      endCloseCQSendId = closeCQSendId;
    }
    this.sendStats.incInt(endCloseCQSendId, 1);
    this.sendStats.incLong(closeCQSendDurationId, duration);
  }

  /**
   * Records that the specified closeCQ has ended
   *
   * @param startTime the value returned by {@link #startCloseCQ}.
   * @param timedOut true if closeCQ timed out
   * @param failed true if closeCQ failed
   */
  public void endCloseCQ(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(closeCQInProgressId, -1);
    int endCloseCQId;
    if (timedOut) {
      endCloseCQId = closeCQTimedOutId;
    } else if (failed) {
      endCloseCQId = closeCQFailedId;
    } else {
      endCloseCQId = closeCQId;
    }
    this.stats.incInt(endCloseCQId, 1);
    this.stats.incLong(closeCQDurationId, duration);
  }

  /**
   * Records that the specified stopCQ is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endStopCQSend} and
   * {@link #endStopCQ}.
   *
   * @return the start time of this stopCQ
   */
  public long startGetDurableCQs() {
    this.stats.incInt(getDurableCQsInProgressId, 1);
    this.sendStats.incInt(getDurableCQsSendsInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the stopCQ has completed
   *
   * @param startTime the value returned by {@link #startStopCQ}.
   * @param failed true if the send of the stopCQ failed
   */
  public void endGetDurableCQsSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(getDurableCQsSendsInProgressId, -1);
    int endGetDurableCQsSendId;
    if (failed) {
      endGetDurableCQsSendId = getDurableCQsSendFailedId;
    } else {
      endGetDurableCQsSendId = getDurableCQsSendId;
    }
    this.sendStats.incInt(endGetDurableCQsSendId, 1);
    this.sendStats.incLong(getDurableCQsSendDurationId, duration);
  }

  /**
   * Records that the specified stopCQ has ended
   *
   * @param startTime the value returned by {@link #startStopCQ}.
   * @param timedOut true if stopCQ timed out
   * @param failed true if stopCQ failed
   */
  public void endGetDurableCQs(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(getDurableCQsInProgressId, -1);
    int endGetDurableCQsId;
    if (timedOut) {
      endGetDurableCQsId = getDurableCQsTimedOutId;
    } else if (failed) {
      endGetDurableCQsId = getDurableCQsFailedId;
    } else {
      endGetDurableCQsId = getDurableCQsId;
    }
    this.stats.incInt(endGetDurableCQsId, 1);
    this.stats.incLong(getDurableCQsDurationId, duration);
  }

  /**
   * Records that the specified gatewayBatch is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endGatewayBatchSend} and
   * {@link #endGatewayBatch}.
   *
   * @return the start time of this gatewayBatch
   */
  public long startGatewayBatch() {
    this.stats.incInt(gatewayBatchInProgressId, 1);
    this.sendStats.incInt(gatewayBatchSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the gatewayBatch has completed
   *
   * @param startTime the value returned by {@link #startGatewayBatch}.
   * @param failed true if the send of the gatewayBatch failed
   */
  public void endGatewayBatchSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(gatewayBatchSendInProgressId, -1);
    int endGatewayBatchSendId;
    if (failed) {
      endGatewayBatchSendId = gatewayBatchSendFailedId;
    } else {
      endGatewayBatchSendId = gatewayBatchSendId;
    }
    this.sendStats.incInt(endGatewayBatchSendId, 1);
    this.sendStats.incLong(gatewayBatchSendDurationId, duration);
  }

  /**
   * Records that the specified gatewayBatch has ended
   *
   * @param startTime the value returned by {@link #startGatewayBatch}.
   * @param timedOut true if gatewayBatch timed out
   * @param failed true if gatewayBatch failed
   */
  public void endGatewayBatch(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(gatewayBatchInProgressId, -1);
    int endGatewayBatchId;
    if (timedOut) {
      endGatewayBatchId = gatewayBatchTimedOutId;
    } else if (failed) {
      endGatewayBatchId = gatewayBatchFailedId;
    } else {
      endGatewayBatchId = gatewayBatchId;
    }
    this.stats.incInt(endGatewayBatchId, 1);
    this.stats.incLong(gatewayBatchDurationId, duration);
  }

  /**
   * Records that the specified readyForEvents is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endReadyForEventsSend}
   * and {@link #endReadyForEvents}.
   *
   * @return the start time of this readyForEvents
   */
  public long startReadyForEvents() {
    this.stats.incInt(readyForEventsInProgressId, 1);
    this.sendStats.incInt(readyForEventsSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the readyForEvents has completed
   *
   * @param startTime the value returned by {@link #startReadyForEvents}.
   * @param failed true if the send of the readyForEvents failed
   */
  public void endReadyForEventsSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(readyForEventsSendInProgressId, -1);
    int endReadyForEventsSendId;
    if (failed) {
      endReadyForEventsSendId = readyForEventsSendFailedId;
    } else {
      endReadyForEventsSendId = readyForEventsSendId;
    }
    this.sendStats.incInt(endReadyForEventsSendId, 1);
    this.sendStats.incLong(readyForEventsSendDurationId, duration);
  }

  /**
   * Records that the specified readyForEvents has ended
   *
   * @param startTime the value returned by {@link #startReadyForEvents}.
   * @param timedOut true if readyForEvents timed out
   * @param failed true if readyForEvents failed
   */
  public void endReadyForEvents(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(readyForEventsInProgressId, -1);
    int endReadyForEventsId;
    if (timedOut) {
      endReadyForEventsId = readyForEventsTimedOutId;
    } else if (failed) {
      endReadyForEventsId = readyForEventsFailedId;
    } else {
      endReadyForEventsId = readyForEventsId;
    }
    this.stats.incInt(endReadyForEventsId, 1);
    this.stats.incLong(readyForEventsDurationId, duration);
  }

  /**
   * Records that the specified makePrimary is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endMakePrimarySend} and
   * {@link #endMakePrimary}.
   *
   * @return the start time of this makePrimary
   */
  public long startMakePrimary() {
    this.stats.incInt(makePrimaryInProgressId, 1);
    this.sendStats.incInt(makePrimarySendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the makePrimary has completed
   *
   * @param startTime the value returned by {@link #startMakePrimary}.
   * @param failed true if the send of the makePrimary failed
   */
  public void endMakePrimarySend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(makePrimarySendInProgressId, -1);
    int endMakePrimarySendId;
    if (failed) {
      endMakePrimarySendId = makePrimarySendFailedId;
    } else {
      endMakePrimarySendId = makePrimarySendId;
    }
    this.sendStats.incInt(endMakePrimarySendId, 1);
    this.sendStats.incLong(makePrimarySendDurationId, duration);
  }

  /**
   * Records that the specified makePrimary has ended
   *
   * @param startTime the value returned by {@link #startMakePrimary}.
   * @param timedOut true if makePrimary timed out
   * @param failed true if makePrimary failed
   */
  public void endMakePrimary(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(makePrimaryInProgressId, -1);
    int endMakePrimaryId;
    if (timedOut) {
      endMakePrimaryId = makePrimaryTimedOutId;
    } else if (failed) {
      endMakePrimaryId = makePrimaryFailedId;
    } else {
      endMakePrimaryId = makePrimaryId;
    }
    this.stats.incInt(endMakePrimaryId, 1);
    this.stats.incLong(makePrimaryDurationId, duration);
  }

  /**
   * Records that the specified closeCon is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endCloseConSend} and
   * {@link #endCloseCon}.
   *
   * @return the start time of this closeCon
   */
  public long startCloseCon() {
    this.stats.incInt(closeConInProgressId, 1);
    this.sendStats.incInt(closeConSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the closeCon has completed
   *
   * @param startTime the value returned by {@link #startCloseCon}.
   * @param failed true if the send of the closeCon failed
   */
  public void endCloseConSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(closeConSendInProgressId, -1);
    int endCloseConSendId;
    if (failed) {
      endCloseConSendId = closeConSendFailedId;
    } else {
      endCloseConSendId = closeConSendId;
    }
    this.sendStats.incInt(endCloseConSendId, 1);
    this.sendStats.incLong(closeConSendDurationId, duration);
  }

  /**
   * Records that the specified closeCon has ended
   *
   * @param startTime the value returned by {@link #startCloseCon}.
   * @param timedOut true if closeCon timed out
   * @param failed true if closeCon failed
   */
  public void endCloseCon(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(closeConInProgressId, -1);
    int endCloseConId;
    if (timedOut) {
      endCloseConId = closeConTimedOutId;
    } else if (failed) {
      endCloseConId = closeConFailedId;
    } else {
      endCloseConId = closeConId;
    }
    this.stats.incInt(endCloseConId, 1);
    this.stats.incLong(closeConDurationId, duration);
  }

  /**
   * Records that the specified primaryAck is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endPrimaryAckSend} and
   * {@link #endPrimaryAck}.
   *
   * @return the start time of this primaryAck
   */
  public long startPrimaryAck() {
    this.stats.incInt(primaryAckInProgressId, 1);
    this.sendStats.incInt(primaryAckSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the primaryAck has completed
   *
   * @param startTime the value returned by {@link #startPrimaryAck}.
   * @param failed true if the send of the primaryAck failed
   */
  public void endPrimaryAckSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(primaryAckSendInProgressId, -1);
    int endPrimaryAckSendId;
    if (failed) {
      endPrimaryAckSendId = primaryAckSendFailedId;
    } else {
      endPrimaryAckSendId = primaryAckSendId;
    }
    this.sendStats.incInt(endPrimaryAckSendId, 1);
    this.sendStats.incLong(primaryAckSendDurationId, duration);
  }

  /**
   * Records that the specified primaryAck has ended
   *
   * @param startTime the value returned by {@link #startPrimaryAck}.
   * @param timedOut true if primaryAck timed out
   * @param failed true if primaryAck failed
   */
  public void endPrimaryAck(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(primaryAckInProgressId, -1);
    int endPrimaryAckId;
    if (timedOut) {
      endPrimaryAckId = primaryAckTimedOutId;
    } else if (failed) {
      endPrimaryAckId = primaryAckFailedId;
    } else {
      endPrimaryAckId = primaryAckId;
    }
    this.stats.incInt(endPrimaryAckId, 1);
    this.stats.incLong(primaryAckDurationId, duration);
  }

  /**
   * Records that the specified ping is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endPingSend} and
   * {@link #endPing}.
   *
   * @return the start time of this ping
   */
  public long startPing() {
    this.stats.incInt(pingInProgressId, 1);
    this.sendStats.incInt(pingSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the ping has completed
   *
   * @param startTime the value returned by {@link #startPing}.
   * @param failed true if the send of the ping failed
   */
  public void endPingSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(pingSendInProgressId, -1);
    int endPingSendId;
    if (failed) {
      endPingSendId = pingSendFailedId;
    } else {
      endPingSendId = pingSendId;
    }
    this.sendStats.incInt(endPingSendId, 1);
    this.sendStats.incLong(pingSendDurationId, duration);
  }

  /**
   * Records that the specified ping has ended
   *
   * @param startTime the value returned by {@link #startPing}.
   * @param timedOut true if ping timed out
   * @param failed true if ping failed
   */
  public void endPing(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(pingInProgressId, -1);
    int endPingId;
    if (timedOut) {
      endPingId = pingTimedOutId;
    } else if (failed) {
      endPingId = pingFailedId;
    } else {
      endPingId = pingId;
    }
    this.stats.incInt(endPingId, 1);
    this.stats.incLong(pingDurationId, duration);
  }

  /**
   * Records that the specified registerInstantiators is starting
   * <p>
   * Note: for every call of this method the caller must also call
   * {@link #endRegisterInstantiatorsSend} and {@link #endRegisterInstantiators}.
   *
   * @return the start time of this registerInstantiators
   */
  public long startRegisterInstantiators() {
    this.stats.incInt(registerInstantiatorsInProgressId, 1);
    this.sendStats.incInt(registerInstantiatorsSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public long startRegisterDataSerializers() {
    this.stats.incInt(registerDataSerializersInProgressId, 1);
    this.sendStats.incInt(registerDataSerializersSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the registerInstantiators has completed
   *
   * @param startTime the value returned by {@link #startRegisterInstantiators}.
   * @param failed true if the send of the registerInstantiators failed
   */
  public void endRegisterInstantiatorsSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(registerInstantiatorsSendInProgressId, -1);
    int endRegisterInstantiatorsSendId;
    if (failed) {
      endRegisterInstantiatorsSendId = registerInstantiatorsSendFailedId;
    } else {
      endRegisterInstantiatorsSendId = registerInstantiatorsSendId;
    }
    this.sendStats.incInt(endRegisterInstantiatorsSendId, 1);
    this.sendStats.incLong(registerInstantiatorsSendDurationId, duration);
  }

  public void endRegisterDataSerializersSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(registerDataSerializersSendInProgressId, -1);
    int endDataSerializersSendId;
    if (failed) {
      endDataSerializersSendId = registerDataSerializersSendFailedId;
    } else {
      endDataSerializersSendId = registerDataSerializersSendId;
    }
    this.sendStats.incInt(endDataSerializersSendId, 1);
    this.sendStats.incLong(registerDataSerializersSendDurationId, duration);
  }

  /**
   * Records that the specified registerInstantiators has ended
   *
   * @param startTime the value returned by {@link #startRegisterInstantiators}.
   * @param timedOut true if registerInstantiators timed out
   * @param failed true if registerInstantiators failed
   */
  public void endRegisterInstantiators(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(registerInstantiatorsInProgressId, -1);
    int endRegisterInstantiatorsId;
    if (timedOut) {
      endRegisterInstantiatorsId = registerInstantiatorsTimedOutId;
    } else if (failed) {
      endRegisterInstantiatorsId = registerInstantiatorsFailedId;
    } else {
      endRegisterInstantiatorsId = registerInstantiatorsId;
    }
    this.stats.incInt(endRegisterInstantiatorsId, 1);
    this.stats.incLong(registerInstantiatorsDurationId, duration);
  }

  public void endRegisterDataSerializers(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(registerDataSerializersInProgressId, -1);
    int endRegisterDataSerializersId;
    if (timedOut) {
      endRegisterDataSerializersId = registerDataSerializersTimedOutId;
    } else if (failed) {
      endRegisterDataSerializersId = registerDataSerializersFailedId;
    } else {
      endRegisterDataSerializersId = registerDataSerializersId;
    }
    this.stats.incInt(endRegisterDataSerializersId, 1);
    this.stats.incLong(registerDataSerializersDurationId, duration);
  }

  /**
   * Records that the specified putAll is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endPutAllSend} and
   * {@link #endPutAll}.
   *
   * @return the start time of this putAll
   */
  public long startPutAll() {
    this.stats.incInt(putAllInProgressId, 1);
    this.sendStats.incInt(putAllSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the putAll has completed
   *
   * @param startTime the value returned by {@link #startPutAll}.
   * @param failed true if the send of the putAll failed
   */
  public void endPutAllSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(putAllSendInProgressId, -1);
    int endPutAllSendId;
    if (failed) {
      endPutAllSendId = putAllSendFailedId;
    } else {
      endPutAllSendId = putAllSendId;
    }
    this.sendStats.incInt(endPutAllSendId, 1);
    this.sendStats.incLong(putAllSendDurationId, duration);
  }

  /**
   * Records that the specified putAll has ended
   *
   * @param startTime the value returned by {@link #startPutAll}.
   * @param timedOut true if putAll timed out
   * @param failed true if putAll failed
   */
  public void endPutAll(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(putAllInProgressId, -1);
    int endPutAllId;
    if (timedOut) {
      endPutAllId = putAllTimedOutId;
    } else if (failed) {
      endPutAllId = putAllFailedId;
    } else {
      endPutAllId = putAllId;
    }
    this.stats.incInt(endPutAllId, 1);
    this.stats.incLong(putAllDurationId, duration);
  }

  /**
   * Records that the specified removeAll is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endRemoveAllSend} and
   * {@link #endRemoveAll}.
   *
   * @return the start time of this removeAll
   */
  public long startRemoveAll() {
    this.stats.incInt(removeAllInProgressId, 1);
    this.sendStats.incInt(removeAllSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the removeAll has completed
   *
   * @param startTime the value returned by {@link #startRemoveAll}.
   * @param failed true if the send of the removeAll failed
   */
  public void endRemoveAllSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(removeAllSendInProgressId, -1);
    int endRemoveAllSendId;
    if (failed) {
      endRemoveAllSendId = removeAllSendFailedId;
    } else {
      endRemoveAllSendId = removeAllSendId;
    }
    this.sendStats.incInt(endRemoveAllSendId, 1);
    this.sendStats.incLong(removeAllSendDurationId, duration);
  }

  /**
   * Records that the specified removeAll has ended
   *
   * @param startTime the value returned by {@link #startRemoveAll}.
   * @param timedOut true if removeAll timed out
   * @param failed true if removeAll failed
   */
  public void endRemoveAll(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(removeAllInProgressId, -1);
    int endRemoveAllId;
    if (timedOut) {
      endRemoveAllId = removeAllTimedOutId;
    } else if (failed) {
      endRemoveAllId = removeAllFailedId;
    } else {
      endRemoveAllId = removeAllId;
    }
    this.stats.incInt(endRemoveAllId, 1);
    this.stats.incLong(removeAllDurationId, duration);
  }

  /**
   * Records that the specified getAll is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endGetAllSend} and
   * {@link #endGetAll}.
   *
   * @return the start time of this getAll
   */
  public long startGetAll() {
    this.stats.incInt(getAllInProgressId, 1);
    this.sendStats.incInt(getAllSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the getAll has completed
   *
   * @param startTime the value returned by {@link #startGetAll}.
   * @param failed true if the send of the getAll failed
   */
  public void endGetAllSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(getAllSendInProgressId, -1);
    int endGetAllSendId;
    if (failed) {
      endGetAllSendId = getAllSendFailedId;
    } else {
      endGetAllSendId = getAllSendId;
    }
    this.sendStats.incInt(endGetAllSendId, 1);
    this.sendStats.incLong(getAllSendDurationId, duration);
  }

  /**
   * Records that the specified getAll has ended
   *
   * @param startTime the value returned by {@link #startGetAll}.
   * @param timedOut true if getAll timed out
   * @param failed true if getAll failed
   */
  public void endGetAll(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(getAllInProgressId, -1);
    int endGetAllId;
    if (timedOut) {
      endGetAllId = getAllTimedOutId;
    } else if (failed) {
      endGetAllId = getAllFailedId;
    } else {
      endGetAllId = getAllId;
    }
    this.stats.incInt(endGetAllId, 1);
    this.stats.incLong(getAllDurationId, duration);
  }

  public int getConnections() {
    return this.stats.getInt(connectionsId);
  }

  public int getOps() {
    int ops = 0;
    for (int i = 0; i < opIds.length; i++) {
      ops += this.stats.getInt(i);
    }
    return ops;
  }

  public void incConnections(int delta) {
    this.stats.incInt(connectionsId, delta);
    if (delta > 0) {
      this.stats.incInt(connectsId, delta);
    } else if (delta < 0) {
      this.stats.incInt(disconnectsId, -delta);
    }
    this.poolStats.incConnections(delta);
  }

  private void startClientOp() {
    this.poolStats.startClientOp();
  }

  private void endClientOpSend(long duration, boolean failed) {
    this.poolStats.endClientOpSend(duration, failed);
  }

  private void endClientOp(long duration, boolean timedOut, boolean failed) {
    this.poolStats.endClientOp(duration, timedOut, failed);
  }

  public void close() {
    this.stats.close();
    this.sendStats.close();
  }

  @Override
  public void incReceivedBytes(long v) {
    this.stats.incLong(receivedBytesId, v);
  }

  @Override
  public void incSentBytes(long v) {
    this.stats.incLong(sentBytesId, v);
  }

  @Override
  public void incMessagesBeingReceived(int bytes) {
    stats.incInt(messagesBeingReceivedId, 1);
    if (bytes > 0) {
      stats.incLong(messageBytesBeingReceivedId, bytes);
    }
  }

  @Override
  public void decMessagesBeingReceived(int bytes) {
    stats.incInt(messagesBeingReceivedId, -1);
    if (bytes > 0) {
      stats.incLong(messageBytesBeingReceivedId, -bytes);
    }
  }

  /**
   * Records that the specified execute Function is starting
   * <p>
   * Note: for every call of this method the caller must also call {@link #endExecuteFunctionSend}
   * and {@link #endExecuteFunction}.
   *
   * @return the start time of this ExecuteFunction
   */
  public long startExecuteFunction() {
    this.stats.incInt(executeFunctionInProgressId, 1);
    this.sendStats.incInt(executeFunctionSendInProgressId, 1);
    return getStatTime();
  }

  /**
   * Records that the send part of the executeFunction has completed
   *
   * @param startTime the value returned by {@link #startExecuteFunction}.
   * @param failed true if the send of the executeFunction failed
   */
  public void endExecuteFunctionSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    this.sendStats.incInt(executeFunctionSendInProgressId, -1);
    int endExecuteFunctionSendId;
    if (failed) {
      endExecuteFunctionSendId = executeFunctionSendFailedId;
    } else {
      endExecuteFunctionSendId = executeFunctionSendId;
    }
    this.sendStats.incInt(endExecuteFunctionSendId, 1);
    this.sendStats.incLong(executeFunctionSendDurationId, duration);
  }

  /**
   * Records that the specified executeFunction has ended
   *
   * @param startTime the value returned by {@link #startExecuteFunction}.
   * @param timedOut true if executeFunction timed out
   * @param failed true if executeFunction failed
   */
  public void endExecuteFunction(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    this.stats.incInt(executeFunctionInProgressId, -1);
    int endExecuteFunctionId;
    if (timedOut) {
      endExecuteFunctionId = executeFunctionTimedOutId;
    } else if (failed) {
      endExecuteFunctionId = executeFunctionFailedId;
    } else {
      endExecuteFunctionId = executeFunctionId;
    }
    this.stats.incInt(endExecuteFunctionId, 1);
    this.stats.incLong(executeFunctionDurationId, duration);
  }

  public int getExecuteFunctions() {
    return this.stats.getInt(executeFunctionId);
  }

  public long getExecuteFunctionDuration() {
    return this.stats.getLong(executeFunctionDurationId);
  }

  public int getGetDurableCqs() {
    return this.stats.getInt(getDurableCQsId);
  }

  /**
   * Records that the specified GetClientPRMetadata operation is starting
   * <p>
   * Note: for every call of this method the caller must also call
   * {@link #endGetClientPRMetadataSend} and {@link #endGetClientPRMetadata}.
   *
   * @return the start time of this ExecuteFunction
   */
  public long startGetClientPRMetadata() {
    this.stats.incInt(getClientPRMetadataInProgressId, 1);
    this.sendStats.incInt(getClientPRMetadataSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the GetClientPRMetadata has completed
   *
   * @param startTime the value returned by {@link #startGetClientPRMetadata}.
   * @param failed true if the send of the GetClientPRMetadata failed
   */
  public void endGetClientPRMetadataSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(getClientPRMetadataSendInProgressId, -1);
    int endGetClientPRMetadataSendId;
    if (failed) {
      endGetClientPRMetadataSendId = getClientPRMetadataSendFailedId;
    } else {
      endGetClientPRMetadataSendId = getClientPRMetadataSendId;
    }
    this.sendStats.incInt(endGetClientPRMetadataSendId, 1);
    this.sendStats.incLong(getClientPRMetadataSendDurationId, duration);
  }

  /**
   * Records that the specified GetClientPRMetadata has ended
   *
   * @param startTime the value returned by {@link #startGetClientPRMetadata}.
   * @param timedOut true if GetClientPRMetadata timed out
   * @param failed true if GetClientPRMetadata failed
   */
  public void endGetClientPRMetadata(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(getClientPRMetadataInProgressId, -1);
    int endGetClientPRMetadataId;
    if (timedOut) {
      endGetClientPRMetadataId = getClientPRMetadataTimedOutId;
    } else if (failed) {
      endGetClientPRMetadataId = getClientPRMetadataFailedId;
    } else {
      endGetClientPRMetadataId = getClientPRMetadataId;
    }
    this.stats.incInt(endGetClientPRMetadataId, 1);
    this.stats.incLong(getClientPRMetadataDurationId, duration);
  }

  /**
   * Records that the specified GetClientPartitionAttributes operation is starting
   * <p>
   * Note: for every call of this method the caller must also call
   * {@link #endGetClientPartitionAttributesSend} and {@link #endGetClientPartitionAttributes}.
   *
   * @return the start time of this GetClientPartitionAttributes
   */
  public long startGetClientPartitionAttributes() {
    this.stats.incInt(getClientPartitionAttributesInProgressId, 1);
    this.sendStats.incInt(getClientPartitionAttributesSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  /**
   * Records that the send part of the GetClientPartitionAttributes operation has completed
   *
   * @param startTime the value returned by {@link #startGetClientPartitionAttributes}.
   * @param failed true if the send of the GetClientPartitionAttributes failed
   */
  public void endGetClientPartitionAttributesSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(getClientPartitionAttributesSendInProgressId, -1);
    int endGetClientPartitionAttributesSendId;
    if (failed) {
      endGetClientPartitionAttributesSendId = getClientPartitionAttributesSendFailedId;
    } else {
      endGetClientPartitionAttributesSendId = getClientPartitionAttributesSendId;
    }
    this.sendStats.incInt(endGetClientPartitionAttributesSendId, 1);
    this.sendStats.incLong(getClientPartitionAttributesSendDurationId, duration);
  }

  /**
   * Records that the specified GetClientPartitionAttributes has ended
   *
   * @param startTime the value returned by {@link #startGetClientPartitionAttributes}.
   * @param timedOut true if GetClientPartitionAttributes timed out
   * @param failed true if GetClientPartitionAttributes failed
   */
  public void endGetClientPartitionAttributes(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(getClientPartitionAttributesInProgressId, -1);
    int endGetClientPartitionAttributesId;
    if (timedOut) {
      endGetClientPartitionAttributesId = getClientPartitionAttributesTimedOutId;
    } else if (failed) {
      endGetClientPartitionAttributesId = getClientPartitionAttributesFailedId;
    } else {
      endGetClientPartitionAttributesId = getClientPartitionAttributesId;
    }
    this.stats.incInt(endGetClientPartitionAttributesId, 1);
    this.stats.incLong(getClientPartitionAttributesDurationId, duration);
  }

  public long startGetPDXTypeById() {
    this.stats.incInt(getPDXTypeByIdInProgressId, 1);
    this.sendStats.incInt(getPDXTypeByIdSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public long startGetPDXIdForType() {
    this.stats.incInt(getPDXIdForTypeInProgressId, 1);
    this.sendStats.incInt(getPDXIdForTypeSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public void endGetPDXTypeByIdSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(getPDXTypeByIdSendInProgressId, -1);
    int endGetPDXTypeByIdSendId;
    if (failed) {
      endGetPDXTypeByIdSendId = getPDXTypeByIdSendFailedId;
    } else {
      endGetPDXTypeByIdSendId = getPDXTypeByIdSendId;
    }
    this.sendStats.incInt(endGetPDXTypeByIdSendId, 1);
    this.sendStats.incLong(getPDXTypeByIdSendDurationId, duration);
  }

  public void endGetPDXIdForTypeSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(getPDXIdForTypeSendInProgressId, -1);
    int endGetPDXIdForTypeSendId;
    if (failed) {
      endGetPDXIdForTypeSendId = getPDXIdForTypeSendFailedId;
    } else {
      endGetPDXIdForTypeSendId = getPDXIdForTypeSendId;
    }
    this.sendStats.incInt(endGetPDXIdForTypeSendId, 1);
    this.sendStats.incLong(getPDXIdForTypeSendDurationId, duration);
  }

  public void endGetPDXTypeById(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(getPDXTypeByIdInProgressId, -1);
    int statId;
    if (timedOut) {
      statId = getPDXTypeByIdTimedOutId;
    } else if (failed) {
      statId = getPDXTypeByIdFailedId;
    } else {
      statId = getPDXTypeByIdId;
    }
    this.stats.incInt(statId, 1);
    this.stats.incLong(getPDXTypeByIdDurationId, duration);
  }

  public void endGetPDXIdForType(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(getPDXIdForTypeInProgressId, -1);
    int statId;
    if (timedOut) {
      statId = getPDXIdForTypeTimedOutId;
    } else if (failed) {
      statId = getPDXIdForTypeFailedId;
    } else {
      statId = getPDXIdForTypeId;
    }
    this.stats.incInt(statId, 1);
    this.stats.incLong(getPDXIdForTypeDurationId, duration);
  }

  public long startAddPdxType() {
    this.stats.incInt(addPdxTypeInProgressId, 1);
    this.sendStats.incInt(addPdxTypeSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public void endAddPdxTypeSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(addPdxTypeSendInProgressId, -1);
    int endAddPdxTypeSendId;
    if (failed) {
      endAddPdxTypeSendId = addPdxTypeSendFailedId;
    } else {
      endAddPdxTypeSendId = addPdxTypeSendId;
    }
    this.sendStats.incInt(endAddPdxTypeSendId, 1);
    this.sendStats.incLong(addPdxTypeSendDurationId, duration);
  }

  public void endAddPdxType(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(addPdxTypeInProgressId, -1);
    int statId;
    if (timedOut) {
      statId = addPdxTypeTimedOutId;
    } else if (failed) {
      statId = addPdxTypeFailedId;
    } else {
      statId = addPdxTypeId;
    }
    this.stats.incInt(statId, 1);
    this.stats.incLong(addPdxTypeDurationId, duration);
  }

  public long startSize() {
    this.stats.incInt(sizeInProgressId, 1);
    this.sendStats.incInt(sizeSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public void endSizeSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(sizeSendInProgressId, -1);
    int endSizeSendId;
    if (failed) {
      endSizeSendId = sizeSendFailedId;
    } else {
      endSizeSendId = sizeSendId;
    }
    this.sendStats.incInt(endSizeSendId, 1);
    this.sendStats.incLong(sizeSendDurationId, duration);

  }

  public void endSize(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(sizeInProgressId, -1);
    int endSizeId;
    if (timedOut) {
      endSizeId = sizeTimedOutId;
    } else if (failed) {
      endSizeId = sizeFailedId;
    } else {
      endSizeId = sizeId;
    }
    this.stats.incInt(endSizeId, 1);
    this.stats.incLong(sizeDurationId, duration);
  }



  public long startInvalidate() {
    this.stats.incInt(invalidateInProgressId, 1);
    this.sendStats.incInt(invalidateSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public void endInvalidateSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(invalidateSendInProgressId, -1);
    int endInvalidateSendId;
    if (failed) {
      endInvalidateSendId = invalidateSendFailedId;
    } else {
      endInvalidateSendId = invalidateSendId;
    }
    this.sendStats.incInt(endInvalidateSendId, 1);
    this.sendStats.incLong(invalidateSendDurationId, duration);
  }

  public void endInvalidate(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(invalidateInProgressId, -1);
    int endInvalidateId;
    if (timedOut) {
      endInvalidateId = invalidateTimedOutId;
    } else if (failed) {
      endInvalidateId = invalidateFailedId;
    } else {
      endInvalidateId = invalidateId;
    }
    this.stats.incLong(endInvalidateId, 1L);
    this.stats.incLong(invalidateDurationId, duration);
  }

  public long startCommit() {
    this.stats.incInt(commitInProgressId, 1);
    this.sendStats.incInt(commitSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public void endCommitSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(commitSendInProgressId, -1);
    int endcommitSendId;
    if (failed) {
      endcommitSendId = commitSendFailedId;
    } else {
      endcommitSendId = commitSendId;
    }
    this.sendStats.incInt(endcommitSendId, 1);
    this.sendStats.incLong(commitSendDurationId, duration);
  }

  public void endCommit(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(commitInProgressId, -1);
    int endcommitId;
    if (timedOut) {
      endcommitId = commitTimedOutId;
    } else if (failed) {
      endcommitId = commitFailedId;
    } else {
      endcommitId = commitId;
    }
    this.stats.incInt(endcommitId, 1);
    this.stats.incLong(commitDurationId, duration);
  }


  public long startGetEntry() {
    this.stats.incInt(getEntryInProgressId, 1);
    this.sendStats.incInt(getEntrySendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public void endGetEntrySend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(getEntrySendInProgressId, -1);
    int endGetEntrySendId;
    if (failed) {
      endGetEntrySendId = getEntrySendFailedId;
    } else {
      endGetEntrySendId = getEntrySendId;
    }
    this.sendStats.incInt(endGetEntrySendId, 1);
    this.sendStats.incLong(getEntrySendDurationId, duration);
  }

  public void endGetEntry(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(getEntryInProgressId, -1);
    int endGetEntryId;
    if (timedOut) {
      endGetEntryId = getEntryTimedOutId;
    } else if (failed) {
      endGetEntryId = getEntryFailedId;
    } else {
      endGetEntryId = getEntryId;
    }
    this.stats.incInt(endGetEntryId, 1);
    this.stats.incLong(getEntryDurationId, duration);
  }


  public long startRollback() {
    this.stats.incInt(rollbackInProgressId, 1);
    this.sendStats.incInt(rollbackSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public void endRollbackSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(rollbackSendInProgressId, -1);
    int endRollbackSendId;
    if (failed) {
      endRollbackSendId = rollbackSendFailedId;
    } else {
      endRollbackSendId = rollbackSendId;
    }
    this.sendStats.incInt(endRollbackSendId, 1);
    this.sendStats.incLong(rollbackSendDurationId, duration);
  }

  public void endRollback(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(rollbackInProgressId, -1);
    int endRollbackId;
    if (timedOut) {
      endRollbackId = rollbackTimedOutId;
    } else if (failed) {
      endRollbackId = rollbackFailedId;
    } else {
      endRollbackId = rollbackId;
    }
    this.stats.incInt(endRollbackId, 1);
    this.stats.incLong(rollbackDurationId, duration);
  }



  public long startTxFailover() {
    this.stats.incInt(txFailoverInProgressId, 1);
    this.sendStats.incInt(txFailoverSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public void endTxFailoverSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(txFailoverSendInProgressId, -1);
    int endTxFailoverSendId;
    if (failed) {
      endTxFailoverSendId = txFailoverSendFailedId;
    } else {
      endTxFailoverSendId = txFailoverSendId;
    }
    this.sendStats.incInt(endTxFailoverSendId, 1);
    this.sendStats.incLong(txFailoverSendDurationId, duration);
  }

  public void endTxFailover(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(txFailoverInProgressId, -1);
    int endTxFailoverId;
    if (timedOut) {
      endTxFailoverId = txFailoverTimedOutId;
    } else if (failed) {
      endTxFailoverId = txFailoverFailedId;
    } else {
      endTxFailoverId = txFailoverId;
    }
    this.stats.incInt(endTxFailoverId, 1);
    this.stats.incLong(txFailoverDurationId, duration);
  }


  public long startTxSynchronization() {
    this.stats.incInt(txSynchronizationInProgressId, 1);
    this.sendStats.incInt(txSynchronizationSendInProgressId, 1);
    startClientOp();
    return getStatTime();
  }

  public void endTxSynchronizationSend(long startTime, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOpSend(duration, failed);
    this.sendStats.incInt(txSynchronizationSendInProgressId, -1);
    int endTxSynchronizationSendId;
    if (failed) {
      endTxSynchronizationSendId = txSynchronizationSendFailedId;
    } else {
      endTxSynchronizationSendId = txSynchronizationSendId;
    }
    this.sendStats.incInt(endTxSynchronizationSendId, 1);
    this.sendStats.incLong(txSynchronizationSendDurationId, duration);
  }

  public void endTxSynchronization(long startTime, boolean timedOut, boolean failed) {
    long duration = getStatTime() - startTime;
    endClientOp(duration, timedOut, failed);
    this.stats.incInt(txSynchronizationInProgressId, -1);
    int endTxSynchronizationId;
    if (timedOut) {
      endTxSynchronizationId = txSynchronizationTimedOutId;
    } else if (failed) {
      endTxSynchronizationId = txSynchronizationFailedId;
    } else {
      endTxSynchronizationId = txSynchronizationId;
    }
    this.stats.incInt(endTxSynchronizationId, 1);
    this.stats.incLong(txSynchronizationDurationId, duration);
  }
}
