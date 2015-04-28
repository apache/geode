/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * @author bruces
 *
 */
public class WaitForViewInstallation extends HighPriorityDistributionMessage
    implements MessageWithReply {
  
  private static final Logger logger = LogService.getLogger();
  
  public static void send(DistributionManager dm) throws InterruptedException {
    long viewId = dm.getMembershipManager().getView().getViewNumber();
    ReplyProcessor21 rp = new ReplyProcessor21(dm, dm.getOtherDistributionManagerIds());
    rp.enableSevereAlertProcessing();
    dm.putOutgoing(new WaitForViewInstallation(viewId, rp.getProcessorId()));
    try {
      rp.waitForReplies();
    } catch (ReplyException e) {
      if (e.getCause() != null  &&  !(e.getCause() instanceof CancelException)) {
        if (logger.isDebugEnabled()) {
          logger.debug("Reply to WaitForViewInstallation received odd exception", e.getCause());
        }
      }
    }
    // this isn't necessary for TXFailoverCommand, which is the only use of this
    // message right now.  TXFailoverCommand performs messaging to all servers,
    // which will force us to wait for the view containing the crash of another
    // server to be processed.
//    dm.waitForViewInstallation(viewId);
  }

  @Override
  public int getProcessorType() {
    return DistributionManager.WAITING_POOL_EXECUTOR;
  }

  @Override
  public boolean sendViaJGroups() {
    return true;
  }

  private long viewId;
  private int processorId;

  /** for deserialization */
  public WaitForViewInstallation() {
  }
  
  private WaitForViewInstallation(long viewId, int processorId) {
    this.viewId = viewId;
    this.processorId = processorId;
  }
  
  @Override
  public int getProcessorId() {
    return this.processorId;
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return WAIT_FOR_VIEW_INSTALLATION;
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(this.viewId);
    out.writeInt(this.processorId);
  }
  
  @Override
  public void fromData(DataInput in) throws ClassNotFoundException, IOException {
    super.fromData(in);
    this.viewId = in.readLong();
    this.processorId = in.readInt();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DistributionMessage#process(com.gemstone.gemfire.distributed.internal.DistributionManager)
   */
  @Override
  protected void process(DistributionManager dm) {
    boolean interrupted = false;
    try {
      dm.waitForViewInstallation(this.viewId);
    } catch (InterruptedException e) {
      interrupted = true;
    } finally {
      if (!interrupted) {
        ReplyMessage.send(getSender(), this.processorId, null,
            getReplySender(dm));
      }
    }
  }
}
