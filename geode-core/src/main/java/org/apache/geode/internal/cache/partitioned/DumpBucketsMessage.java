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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A message used for debugging purposes. For example if a test fails it can call
 * {@link PartitionedRegion#dumpAllBuckets(boolean)} which sends this message to all VMs that have
 * that PartitionedRegion defined.
 *
 * @see org.apache.geode.internal.cache.PartitionedRegion#dumpAllBuckets(boolean)
 */
public class DumpBucketsMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  boolean validateOnly;
  boolean bucketsOnly;

  public DumpBucketsMessage() {}

  private DumpBucketsMessage(Set recipients, int regionId, ReplyProcessor21 processor,
      boolean validate, boolean buckets) {
    super(recipients, regionId, processor);
    validateOnly = validate;
    bucketsOnly = buckets;
  }

  public static PartitionResponse send(Set recipients, PartitionedRegion r,
      final boolean validateOnly, final boolean onlyBuckets) {
    PartitionResponse p = new PartitionResponse(r.getSystem(), recipients);
    DumpBucketsMessage m =
        new DumpBucketsMessage(recipients, r.getPRId(), p, validateOnly, onlyBuckets);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());

    r.getDistributionManager().putOutgoing(m);
    return p;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion pr,
      long startTime) throws CacheException {

    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "DumpBucketsMessage operateOnRegion: {}",
          pr.getFullPath());
    }

    PartitionedRegionDataStore ds = pr.getDataStore();
    if (ds != null) {
      if (bucketsOnly) {
        ds.dumpBuckets();
      } else {
        ds.dumpEntries(validateOnly);
      }
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} dumped buckets", getClass().getName());
      }
    }
    return true;
  }

  @Override
  public int getDSFID() {
    return PR_DUMP_BUCKETS_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    validateOnly = in.readBoolean();
    bucketsOnly = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeBoolean(validateOnly);
    out.writeBoolean(bucketsOnly);
  }
}
