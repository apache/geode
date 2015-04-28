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
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;

public class ReleaseClearLockMessage extends
  HighPriorityDistributionMessage implements MessageWithReply {

  private static final Logger logger = LogService.getLogger();
  
    private String regionPath;
    private int processorId;

    /** for deserialization */
    public ReleaseClearLockMessage() {
    }

    public ReleaseClearLockMessage(String regionPath, int processorId) {
      this.regionPath = regionPath;
      this.processorId = processorId;
    }
    
    public static void send(
        Set<InternalDistributedMember> members, DM dm, String regionPath) throws ReplyException {
      ReplyProcessor21 processor = new ReplyProcessor21(dm, members);
      ReleaseClearLockMessage msg = new ReleaseClearLockMessage(regionPath, processor.getProcessorId());
      msg.setRecipients(members);

      dm.putOutgoing(msg);
      processor.waitForRepliesUninterruptibly();
    }

    @Override
    protected void process(DistributionManager dm) {
      ReplyException exception = null;
      try {
        DistributedRegion region = DistributedClearOperation.regionUnlocked(getSender(), regionPath);
        if(region != null && region.getVersionVector() != null) {
          region.getVersionVector().unlockForClear(getSender());
        }
      }  catch(VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch(Throwable t) {
        SystemFailure.checkFailure();
        exception = new ReplyException(t);
      }
      finally {
        ReplyMessage replyMsg = new ReplyMessage();
        replyMsg.setProcessorId(processorId);
        replyMsg.setRecipient(getSender());
        if(exception != null) {
          replyMsg.setException(exception);
        }
        if(logger.isDebugEnabled()) {
          logger.debug("Received {}, replying with {}", this, replyMsg);
        }
        dm.putOutgoing(replyMsg);
      }
    }

    public int getDSFID() {
      return RELEASE_CLEAR_LOCK_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
    ClassNotFoundException {
      super.fromData(in);
      regionPath = DataSerializer.readString(in);
      processorId = in.readInt();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(regionPath, out);
      out.writeInt(processorId);
    }
  }