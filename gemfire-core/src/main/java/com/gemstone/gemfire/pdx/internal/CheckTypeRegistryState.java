/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.PdxInitializationException;

public class CheckTypeRegistryState extends HighPriorityDistributionMessage 
          implements MessageWithReply {
  private int processorId;

  public CheckTypeRegistryState() {
    super();
  }

  protected CheckTypeRegistryState(int processorId) {
    super();
    this.processorId = processorId;
  }

  public static void send(DM dm) {
    Set recipients = dm.getOtherDistributionManagerIds();
    ReplyProcessor21 replyProcessor = new ReplyProcessor21(dm, recipients);
    CheckTypeRegistryState msg = new CheckTypeRegistryState(replyProcessor.getProcessorId());
    msg.setRecipients(recipients);
    dm.putOutgoing(msg);
    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if(e.getCause() instanceof PdxInitializationException) {
        throw new PdxInitializationException("Bad PDX configuration on member "
            + e.getSender() + ": " + e.getCause().getMessage(), e.getCause());
      }
      else {
        throw new InternalGemFireError("Unexpected exception", e);
      }
    } catch (InterruptedException e) {
      throw new InternalGemFireError("Unexpected exception", e);
    }
  }

  @Override
  protected void process(DistributionManager dm) {
    ReplyException e = null;
    try {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if(cache != null && !cache.isClosed()) {
        TypeRegistry pdxRegistry = cache.getPdxRegistry();
        if(pdxRegistry != null) {
          TypeRegistration registry = pdxRegistry.getTypeRegistration();
          if(registry instanceof PeerTypeRegistration) {
            PeerTypeRegistration peerRegistry = (PeerTypeRegistration) registry;
            peerRegistry.verifyConfiguration();
          }
        }
      }
    } catch(Exception ex) {
      e = new ReplyException(ex);
    } finally {
      ReplyMessage rm = new ReplyMessage();
      rm.setException(e);
      rm.setProcessorId(processorId);
      rm.setRecipient(getSender());
      dm.putOutgoing(rm);
    }
  }

  public int getDSFID() {
    return CHECK_TYPE_REGISTRY_STATE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.processorId = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.processorId);
  }
}
