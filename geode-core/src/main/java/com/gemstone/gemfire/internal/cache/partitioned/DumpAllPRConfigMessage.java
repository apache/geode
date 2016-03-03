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
