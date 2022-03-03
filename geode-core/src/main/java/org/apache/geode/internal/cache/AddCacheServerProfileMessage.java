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

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.BEFORE_INITIAL_IMAGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * OperationMessage synchronously propagates a change in the profile to another member. It is a
 * serial message so that there is no chance of out-of-order execution.
 */
public class AddCacheServerProfileMessage extends SerialDistributionMessage
    implements MessageWithReply {

  private static final Logger logger = LogService.getLogger();

  int processorId;

  @Override
  protected void process(ClusterDistributionManager dm) {
    final InitializationLevel oldLevel =
        LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
    try {
      InternalCache cache = dm.getCache();
      // will be null if not initialized
      if (cache != null && !cache.isClosed()) {
        operateOnCache(cache);
      }
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
      ReplyMessage reply = new ReplyMessage();
      reply.setProcessorId(processorId);
      reply.setRecipient(getSender());
      try {
        dm.putOutgoing(reply);
      } catch (CancelException ignore) {
        // can't send a reply, so ignore the exception
      }
    }
  }

  private void operateOnCache(InternalCache cache) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    for (DistributedRegion r : getDistributedRegions(cache)) {
      CacheDistributionAdvisor cda = r.getDistributionAdvisor();
      CacheDistributionAdvisor.CacheProfile cp =
          (CacheDistributionAdvisor.CacheProfile) cda.getProfile(getSender());
      if (cp != null) {
        if (isDebugEnabled) {
          logger.debug("Setting hasCacheServer flag on region \"{}\" for {}", r.getFullPath(),
              getSender());
        }
        cp.hasCacheServer = true;
      }
    }
    for (PartitionedRegion r : getPartitionedRegions(cache)) {
      CacheDistributionAdvisor cda = (CacheDistributionAdvisor) r.getDistributionAdvisor();
      CacheDistributionAdvisor.CacheProfile cp =
          (CacheDistributionAdvisor.CacheProfile) cda.getProfile(getSender());
      if (cp != null) {
        if (isDebugEnabled) {
          logger.debug("Setting hasCacheServer flag on region \"{}\" for {}", r.getFullPath(),
              getSender());
        }
        cp.hasCacheServer = true;
      }
    }
  }

  /** set the hasCacheServer flags for all regions in this cache */
  public void operateOnLocalCache(InternalCache cache) {
    final InitializationLevel oldLevel =
        LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
    try {
      for (InternalRegion r : getAllRegions(cache)) {
        FilterProfile fp = r.getFilterProfile();
        if (fp != null) {
          fp.getLocalProfile().hasCacheServer = true;
        }
      }
      for (PartitionedRegion r : getPartitionedRegions(cache)) {
        FilterProfile fp = r.getFilterProfile();
        if (fp != null) {
          fp.getLocalProfile().hasCacheServer = true;
        }
      }
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
  }


  private Set<InternalRegion> getAllRegions(InternalCache internalCache) {
    return internalCache.getAllRegions();
  }

  private Set<DistributedRegion> getDistributedRegions(InternalCache internalCache) {
    Set<DistributedRegion> result = new HashSet<>();
    for (InternalRegion r : internalCache.getAllRegions()) {
      if (r instanceof DistributedRegion) {
        result.add((DistributedRegion) r);
      }
    }
    return result;
  }

  private Set<PartitionedRegion> getPartitionedRegions(InternalCache internalCache) {
    return (Set<PartitionedRegion>) new HashSet(internalCache.getPartitionedRegions());
  }

  /** for deserialization only */
  public AddCacheServerProfileMessage() {}

  @Override
  public int getDSFID() {
    return ADD_CACHESERVER_PROFILE_UPDATE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(processorId);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    processorId = in.readInt();
  }

  @Override
  public String toString() {
    return getShortClassName() + "(processorId=" + processorId + ")";
  }
}
