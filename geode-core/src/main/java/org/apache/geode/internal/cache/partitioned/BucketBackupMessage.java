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
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message sent requesting that an evaluation of buckets be made to determine if one or more needs
 * to be backed-up in order to satisfy the redundantCopies setting
 *
 * @since GemFire 5.0
 */
public class BucketBackupMessage extends PartitionMessage {

  private static final Logger logger = LogService.getLogger();

  private int bucketId;

  /**
   * Empty constructor provided for {@link org.apache.geode.DataSerializer}
   */
  public BucketBackupMessage() {
    super();
  }

  private BucketBackupMessage(Set recipients, int regionId, int bucketId) {
    super(recipients, regionId, null /* no processor */);
    this.bucketId = bucketId;
  }

  /**
   * Sends a BucketBackupMessage requesting that another VM backup an existing bucket
   *
   * @param recipients the member that the contains keys/value message is sent to
   * @param r the PartitionedRegion that contains the bucket
   */
  public static void send(Set recipients, PartitionedRegion r, int bucketId) {
    Assert.assertTrue(recipients != null, "BucketBackupMessage NULL sender list");
    BucketBackupMessage m = new BucketBackupMessage(recipients, r.getPRId(), bucketId);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    r.getDistributionManager().putOutgoing(m);
  }

  /**
   * This message may be sent to nodes before the PartitionedRegion is completely initialized due to
   * the RegionAdvisor(s) knowing about the existence of a partitioned region at a very early part
   * of the initialization
   */
  @Override
  protected boolean failIfRegionMissing() {
    return false;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion pr,
      long startTime) throws CacheException {

    // This call has come to an uninitialized region.
    // This can occur as bucket grab Op is done outside the
    // d-lock.
    if (pr == null || !pr.isInitialized()) {
      return false;
    }

    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "BucketBackupMessage operateOnRegion: {}",
          pr.getFullPath());
    }
    PartitionedRegionDataStore ds = pr.getDataStore();
    if (ds != null) {
      pr.getRedundancyProvider().finishIncompleteBucketCreation(bucketId);
    } else {
      logger.warn("BucketBackupMessage: data store not configured for this member");
    }
    pr.getPrStats().endPartitionMessagesProcessing(startTime);
    return false;
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  @Override
  public int getDSFID() {
    return PR_BUCKET_BACKUP_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    bucketId = in.readInt();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(bucketId);
  }

}
