/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.SerialDistributionMessage;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * OperationMessage synchronously propagates a change in the profile to
 * another member.  It is a serial message so that there is no chance
 * of out-of-order execution.
 */
public class AddCacheServerProfileMessage extends SerialDistributionMessage implements MessageWithReply {

  private static final Logger logger = LogService.getLogger();
  
  int processorId;

  @Override
  protected void process(DistributionManager dm) {
    int oldLevel =
      LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
    try {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache != null && !cache.isClosed()) {  // will be null if not initialized
        operateOnCache(cache);
      }
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
      ReplyMessage reply = new ReplyMessage();
      reply.setProcessorId(this.processorId);
      reply.setRecipient(getSender());
      try {
        dm.putOutgoing(reply);
      } catch (CancelException e) {
        // can't send a reply, so ignore the exception
      }
    }
  }
  
  private void operateOnCache(GemFireCacheImpl cache) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    for (DistributedRegion r: this.getDistributedRegions(cache)) {
      CacheDistributionAdvisor cda = (CacheDistributionAdvisor)r.getDistributionAdvisor();
      CacheDistributionAdvisor.CacheProfile cp =
        (CacheDistributionAdvisor.CacheProfile)cda.getProfile(getSender());
      if (cp != null){
        if (isDebugEnabled) {
          logger.debug("Setting hasCacheServer flag on region \"{}\" for {}", r.getFullPath(), getSender());
        }
        cp.hasCacheServer = true;
      }
    }
    for (PartitionedRegion r: this.getPartitionedRegions(cache)) {
      CacheDistributionAdvisor cda = (CacheDistributionAdvisor)r.getDistributionAdvisor();
      CacheDistributionAdvisor.CacheProfile cp =
        (CacheDistributionAdvisor.CacheProfile)cda.getProfile(getSender());
      if (cp != null){
        if (isDebugEnabled) {
          logger.debug("Setting hasCacheServer flag on region \"{}\" for {}", r.getFullPath(), getSender());
        }
        cp.hasCacheServer = true;
      }
    }
  }
  
  /** set the hasCacheServer flags for all regions in this cache */
  public void operateOnLocalCache(GemFireCacheImpl cache) {
    int oldLevel =
      LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
    try {
      for (LocalRegion r: this.getAllRegions(cache)) {
        FilterProfile fp = r.getFilterProfile();
        if (fp != null) {
          fp.getLocalProfile().hasCacheServer = true;
        }
      }
      for (PartitionedRegion r: this.getPartitionedRegions(cache)) {
        FilterProfile fp = r.getFilterProfile();
        if (fp != null) {
          fp.getLocalProfile().hasCacheServer = true;
        }
      }
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
  }

  
  private Set<LocalRegion> getAllRegions(GemFireCacheImpl gfc) {
    return gfc.getAllRegions();
  }
  
  private Set<DistributedRegion> getDistributedRegions(GemFireCacheImpl gfc) {
    Set<DistributedRegion> result = new HashSet();
    for (LocalRegion r: gfc.getAllRegions()) {
      if (r instanceof DistributedRegion) {
        result.add((DistributedRegion)r);
      }
    }
    return result;
  }

  private Set<PartitionedRegion> getPartitionedRegions(GemFireCacheImpl gfc) {
    Set<PartitionedRegion> result = new HashSet(gfc.getPartitionedRegions());
    return result;
  }

  /** for deserialization only */
  public AddCacheServerProfileMessage() {
  }

  public int getDSFID() {
    return ADD_CACHESERVER_PROFILE_UPDATE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.processorId);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.processorId = in.readInt();
  }

  @Override
  public String toString() {
    return this.getShortClassName() + "(processorId=" + this.processorId + ")";
  }
}

