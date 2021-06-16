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
package org.apache.geode.internal.cache.partitioned;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.PartitionedRegionDistributionException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionException;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.partitioned.PutMessage.PutResponse;
import org.apache.geode.internal.cache.partitioned.PutMessage.PutResult;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class DefaultPartitionMessageDistribution implements PartitionMessageDistribution {
  private static final Logger LOGGER = LogService.getLogger();

  @Override
  public boolean createRemotely(PartitionedRegion region, PartitionedRegionStats stats,
      DistributedMember recipient, EntryEventImpl event, boolean requireOldValue)
      throws ForceReattemptException {
    long eventTime = event.getEventTime(0L);
    PutResponse response = (PutResponse) PutMessage
        .send(recipient, region, event, eventTime, true, false, null, requireOldValue);
    if (response == null) {
      return false;
    }
    stats.incPartitionMessagesSent();
    try {
      PutResult result = response.waitForResult();
      event.setOperation(result.op);
      event.setVersionTag(result.versionTag);
      if (requireOldValue) {
        event.setOldValue(result.oldValue, true);
      }
      return result.returnValue;
    } catch (EntryExistsException ignore) {
    } catch (TransactionDataNotColocatedException | TransactionDataRebalancedException e) {
      throw e;
    } catch (CacheException e) {
      throw new PartitionedRegionException(
          String.format("Create of entry on %s failed", recipient), e);
    } catch (RegionDestroyedException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("createRemotely: caught exception", e);
      }
      throw new RegionDestroyedException(region.toString(), region.getFullPath());
    }
    return false;
  }

  @Override
  public boolean putRemotely(PartitionedRegion partitionedRegion,
      PartitionedRegionStats prStats, DistributedMember recipient,
      EntryEventImpl event, boolean ifNew, boolean ifOld, Object expectedOldValue,
      boolean requireOldValue) throws ForceReattemptException {
    long eventTime = event.getEventTime(0L);
    PutMessage.PutResponse response = (PutMessage.PutResponse) PutMessage.send(recipient,
        partitionedRegion, event, eventTime, ifNew, ifOld, expectedOldValue, requireOldValue);
    if (response == null) {
      return true; // ???:ezoerner:20080728 why return true if response was null?
    }
    prStats.incPartitionMessagesSent();
    try {
      PutResult pr = response.waitForResult();
      event.setOperation(pr.op);
      event.setVersionTag(pr.versionTag);
      if (requireOldValue) {
        event.setOldValue(pr.oldValue, true);
      }
      return pr.returnValue;
    } catch (RegionDestroyedException rde) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("putRemotely: caught RegionDestroyedException", rde);
      }
      throw new RegionDestroyedException(toString(), partitionedRegion.getFullPath());
    } catch (TransactionException te) {
      throw te;
    } catch (CacheException ce) {
      // Fix for bug 36014
      throw new PartitionedRegionDistributionException(
          String.format("Putting entry on %s failed",
              recipient),
          ce);
    }
  }
}
