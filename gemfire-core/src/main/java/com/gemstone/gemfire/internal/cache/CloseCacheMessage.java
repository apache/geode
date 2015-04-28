/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.internal.logging.LogService;


/**
 *
 * @author Eric Zoerner
 *
 */
  
/** Creates a new instance of CloseCacheMessage */
public final class CloseCacheMessage extends HighPriorityDistributionMessage
  implements MessageWithReply {
  private static final Logger logger = LogService.getLogger();
  
  private int processorId;
  
  @Override
  public int getProcessorId() {
    return this.processorId;
  }
  
  @Override
  public boolean sendViaJGroups() {
    return true;
  }
  
  @Override
  protected void process(DistributionManager dm) {
    // Now that Cache.close calls close on each region we don't need
    // any of the following code so we can just do an immediate ack.
    boolean systemError = false;
    try {
      try {
          PartitionedRegionHelper.cleanUpMetaDataOnNodeFailure(getSender());
      } catch (VirtualMachineError err) {
        systemError = true;
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        if (logger.isDebugEnabled()) {
          logger.debug("Throwable caught while processing cache close message from:{}", getSender(), t);
        }
      }
    } finally {
      if(!systemError) {
        ReplyMessage.send(getSender(), processorId, null, dm, false, false, true);
      }
    }
  }
  
  public void setProcessorId(int id) {
    this.processorId = id;
  }
  
  @Override
  public String toString() {
    return super.toString() + " (processorId=" + processorId + ")";
  }

  public int getDSFID() {
    return CLOSE_CACHE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.processorId = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.processorId);
  }
}
 
