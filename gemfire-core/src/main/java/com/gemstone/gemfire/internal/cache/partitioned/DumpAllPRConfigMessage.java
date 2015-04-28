/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * A message used for debugging purposes.  For example if a test
 * fails it can call {@link com.gemstone.gemfire.internal.cache.PartitionedRegion#sendDumpAllPartitionedRegions()} 
 * which sends this message to all VMs that have that PartitionedRegion defined.
 * 
 * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#sendDumpAllPartitionedRegions()
 * @author Tushar Apshankar
 */
public final class DumpAllPRConfigMessage extends PartitionMessage
  {
  private static final Logger logger = LogService.getLogger();
  
  public DumpAllPRConfigMessage() {}

  private DumpAllPRConfigMessage(Set recipients, int regionId, ReplyProcessor21 processor) {
    super(recipients, regionId, processor);
  }

  public static PartitionResponse send(Set recipients, PartitionedRegion r) 
      {
    PartitionResponse p = new PartitionResponse(r.getSystem(), recipients);
    DumpAllPRConfigMessage m = new DumpAllPRConfigMessage(recipients, r.getPRId(), p);

    /*Set failures = */r.getDistributionManager().putOutgoing(m);
//    if (failures != null && failures.size() > 0) {
//      throw new PartitionedRegionCommunicationException("Failed sending ", m);
//    }
    return p;
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, 
      PartitionedRegion pr, long startTime) throws CacheException {
    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "DumpAllPRConfigMessage operateOnRegion: {}", pr.getFullPath());
    }
    pr.dumpSelfEntryFromAllPartitionedRegions();

    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.debug("{} dumped allPartitionedRegions", getClass().getName());
    }
    return true;
  }

  public int getDSFID() {
    return PR_DUMP_ALL_PR_CONFIG_MESSAGE;
  }
}
