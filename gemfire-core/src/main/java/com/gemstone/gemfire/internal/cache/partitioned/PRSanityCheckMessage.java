/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.SystemTimer;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;

/**
 * PRSanityCheckMessage is used to assert correctness of prID assignments
 * across the distributed system.
 * 
 * @author bruce
 *
 */
public final class PRSanityCheckMessage extends PartitionMessage
{
  
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
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append(" regionName=").append(this.regionName);
  }

  public int getDSFID() {
    return PR_SANITY_CHECK_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.regionName = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.regionName, out);
  }

 /**
   * Send a sanity check message and schedule a timer to send another one
   * in gemfire.PRSanityCheckInterval (default 5000) milliseconds.  This can
   * be enabled with gemfire.PRSanityCheckEnabled=true. 
   */
  public static void schedule(final PartitionedRegion pr) {
    if (Boolean.getBoolean("gemfire.PRSanityCheckEnabled")) {
      final DM dm = pr.getDistributionManager();
//      RegionAdvisor ra = pr.getRegionAdvisor();
//      final Set recipients = ra.adviseAllPRNodes();
      DistributedRegion prRoot = (DistributedRegion) PartitionedRegionHelper.getPRRoot(pr.getCache(), false);
      if (prRoot == null) {
        return;
      }
      final Set recipients = prRoot.getDistributionAdvisor().adviseGeneric();
      if (recipients.size() <= 0) {
        return;
      }
      final PRSanityCheckMessage delayedInstance = new PRSanityCheckMessage(
          recipients, pr.getPRId(), null, pr.getRegionIdentifier());
      PRSanityCheckMessage instance = new PRSanityCheckMessage(recipients,
          pr.getPRId(), null, pr.getRegionIdentifier());
      dm.putOutgoing(instance);
      int sanityCheckInterval = Integer.getInteger("gemfire.PRSanityCheckInterval",
                                                   5000).intValue();
      if (sanityCheckInterval != 0) {
        final SystemTimer tm = new SystemTimer(dm.getSystem(), true);
        SystemTimer.SystemTimerTask st = new SystemTimer.SystemTimerTask() {
            @Override
              public void run2() {
              try {
                if (!pr.isLocallyDestroyed && !pr.isClosed && !pr.isDestroyed()) {
                  dm.putOutgoing(delayedInstance);
                }
              }
              catch (CancelException cce) {
                // cache is closed - can't send the message
              }
              finally {
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
    return DistributionManager.HIGH_PRIORITY_EXECUTOR;
  }

  /**
   * completely override process() from PartitionMessage.  This message doesn't
   * operate on a specific partitioned region, so the superclass impl doesn't
   * make any sense to it.
   * @param dm the distribution manager to use
   */
  @Override
  public void process(DistributionManager dm) {
    PartitionedRegion.validatePRID(getSender(), this.regionId, this.regionName);
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, PartitionedRegion pr, long startTime) throws CacheException, QueryException, ForceReattemptException, InterruptedException {
    return false;
  }

}
