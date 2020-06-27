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
package org.apache.geode.internal.cache;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Implementation of {@link TxCallbackEventFactory}
 */
public class TxCallbackEventFactoryImpl implements TxCallbackEventFactory {
  private static final Logger logger = LogService.getLogger();

  /** create a callback event for applying a transactional change to the local cache */
  @Override
  @Retained
  public EntryEventImpl createCallbackEvent(final InternalRegion internalRegion,
      Operation op, Object key, Object newValue,
      TransactionId txId, TXRmtEvent txEvent,
      EventID eventId, Object aCallbackArgument,
      FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext,
      TXEntryState txEntryState, VersionTag versionTag,
      long tailKey) {
    DistributedMember originator = null;
    // txId should not be null even on localOrigin
    Assert.assertTrue(txId != null);
    originator = txId.getMemberId();

    InternalRegion eventRegion = internalRegion;
    if (eventRegion.isUsedForPartitionedRegionBucket()) {
      eventRegion = internalRegion.getPartitionedRegion();
    }

    @Retained
    EntryEventImpl retVal = EntryEventImpl.create(internalRegion, op, key, newValue,
        aCallbackArgument, txEntryState == null, originator);
    boolean returnedRetVal = false;
    try {


      if (bridgeContext != null) {
        retVal.setContext(bridgeContext);
      }

      if (eventRegion.generateEventID()) {
        retVal.setEventId(eventId);
      }

      if (versionTag != null) {
        retVal.setVersionTag(versionTag);
      }

      retVal.setTailKey(tailKey);

      FilterRoutingInfo.FilterInfo localRouting = null;
      boolean computeFilterInfo = false;
      if (filterRoutingInfo == null) {
        computeFilterInfo = true;
      } else {
        localRouting = filterRoutingInfo.getLocalFilterInfo();
        if (localRouting != null) {
          // routing was computed in this VM but may need to perform local interest processing
          computeFilterInfo = !filterRoutingInfo.hasLocalInterestBeenComputed()
              && !localRouting.filterProcessedLocally;
        } else {
          // routing was computed elsewhere and is in the "remote" routing table
          localRouting = filterRoutingInfo.getFilterInfo(internalRegion.getMyId());
        }
        if (localRouting != null) {
          if (!computeFilterInfo) {
            retVal.setLocalFilterInfo(localRouting);
          }
        } else {
          computeFilterInfo = true;
        }
      }
      if (TxCallbackEventFactoryImpl.logger.isTraceEnabled()) {
        TxCallbackEventFactoryImpl.logger.trace(
            "createCBEvent filterRouting={} computeFilterInfo={} local routing={}",
            filterRoutingInfo, computeFilterInfo, localRouting);
      }

      if (internalRegion.isUsedForPartitionedRegionBucket()) {
        BucketRegion bucket = (BucketRegion) internalRegion;
        if (BucketRegion.FORCE_LOCAL_LISTENERS_INVOCATION
            || bucket.getBucketAdvisor().isPrimary()) {
          retVal.setInvokePRCallbacks(true);
        } else {
          retVal.setInvokePRCallbacks(false);
        }

        if (computeFilterInfo) {
          if (bucket.getBucketAdvisor().isPrimary()) {
            if (TxCallbackEventFactoryImpl.logger.isTraceEnabled()) {
              TxCallbackEventFactoryImpl.logger
                  .trace("createCBEvent computing routing for primary bucket");
            }
            FilterProfile fp =
                ((BucketRegion) internalRegion).getPartitionedRegion().getFilterProfile();
            if (fp != null) {
              FilterRoutingInfo fri = fp.getFilterRoutingInfoPart2(filterRoutingInfo, retVal);
              if (fri != null) {
                retVal.setLocalFilterInfo(fri.getLocalFilterInfo());
              }
            }
          }
        }
      } else if (computeFilterInfo) { // not a bucket
        if (TxCallbackEventFactoryImpl.logger.isTraceEnabled()) {
          TxCallbackEventFactoryImpl.logger.trace("createCBEvent computing routing for non-bucket");
        }
        FilterProfile fp = internalRegion.getFilterProfile();
        if (fp != null) {
          retVal.setLocalFilterInfo(fp.getLocalFilterRouting(retVal));
        }
      }
      retVal.setTransactionId(txId);
      returnedRetVal = true;
      return retVal;
    } finally {
      if (!returnedRetVal) {
        retVal.release();
      }
    }
  }
}
