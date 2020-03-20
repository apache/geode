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

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * PRSanityCheckMessage is used to assert correctness of prID assignments across the distributed
 * system.
 *
 *
 */
public class PRSanityCheckMessage extends PartitionMessage {

  private String regionName;


  public PRSanityCheckMessage() {
    super();
  }

  /**
   * @param recipients the recipients of the message
   * @param prId the prid of the region
   * @param processor the reply processor, if you expect an answer (don't!)
   * @param regionName the regionIdentifier string
   */
  public PRSanityCheckMessage(Set recipients, int prId, ReplyProcessor21 processor,
      String regionName) {
    super(recipients, prId, processor);
    this.regionName = regionName;
  }


  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append(" regionName=").append(this.regionName);
  }

  @Override
  public int getDSFID() {
    return PR_SANITY_CHECK_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.regionName = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeString(this.regionName, out);
  }

  /**
   * Send a sanity check message and schedule a timer to send another one in
   * gemfire.PRSanityCheckInterval (default 5000) milliseconds. This can be enabled with
   * gemfire.PRSanityCheckEnabled=true.
   */
  public static void schedule(final PartitionedRegion pr) {
    if (!Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "PRSanityCheckDisabled")) {
      final DistributionManager dm = pr.getDistributionManager();
      // RegionAdvisor ra = pr.getRegionAdvisor();
      // final Set recipients = ra.adviseAllPRNodes();
      DistributedRegion prRoot =
          (DistributedRegion) PartitionedRegionHelper.getPRRoot(pr.getCache(), false);
      if (prRoot == null) {
        return;
      }
      final Set recipients = prRoot.getDistributionAdvisor().adviseGeneric();
      if (recipients.size() <= 0) {
        return;
      }
      final PRSanityCheckMessage delayedInstance =
          new PRSanityCheckMessage(recipients, pr.getPRId(), null, pr.getRegionIdentifier());
      delayedInstance.setTransactionDistributed(pr.getCache().getTxManager().isDistributed());
      PRSanityCheckMessage instance =
          new PRSanityCheckMessage(recipients, pr.getPRId(), null, pr.getRegionIdentifier());
      instance.setTransactionDistributed(pr.getCache().getTxManager().isDistributed());
      dm.putOutgoing(instance);
      int sanityCheckInterval = Integer
          .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "PRSanityCheckInterval", 5000).intValue();
      if (sanityCheckInterval != 0) {
        final SystemTimer tm = new SystemTimer(dm.getSystem());
        SystemTimer.SystemTimerTask st = new SystemTimer.SystemTimerTask() {
          @Override
          public void run2() {
            try {
              if (!pr.isLocallyDestroyed && !pr.isClosed && !pr.isDestroyed()) {
                dm.putOutgoing(delayedInstance);
              }
            } catch (CancelException cce) {
              // cache is closed - can't send the message
            } finally {
              tm.cancel();
            }
          }
        };
        tm.schedule(st, sanityCheckInterval);
      }
    }
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.HIGH_PRIORITY_EXECUTOR;
  }

  /**
   * completely override process() from PartitionMessage. This message doesn't operate on a specific
   * partitioned region, so the superclass impl doesn't make any sense to it.
   *
   * @param dm the distribution manager to use
   */
  @Override
  public void process(ClusterDistributionManager dm) {
    PartitionedRegion.validatePRID(getSender(), this.regionId, this.regionName);
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion pr,
      long startTime)
      throws CacheException, QueryException, ForceReattemptException, InterruptedException {
    return false;
  }

}
