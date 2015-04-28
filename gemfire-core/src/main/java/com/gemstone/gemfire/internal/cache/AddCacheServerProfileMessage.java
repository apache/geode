/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

