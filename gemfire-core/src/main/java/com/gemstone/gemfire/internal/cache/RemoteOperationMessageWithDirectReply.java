/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Used for partitioned region messages which support direct ack responses.
 * Direct ack should be used for message with a response from a single member,
 * or responses which are small.
 * 
 * Messages that extend this class *must* reply using the ReplySender returned
 * by {@link DistributionMessage#getReplySender(com.gemstone.gemfire.distributed.internal.DM)}
 * 
 * Additionally, if the ReplyProcessor used for this message extends PartitionResponse, it should
 * pass false for the register parameter of the PartitionResponse.
 * @author dsmith
 *
 */
public abstract class RemoteOperationMessageWithDirectReply extends
    RemoteOperationMessage implements DirectReplyMessage {
  
  protected DirectReplyProcessor processor;
  
  

  public RemoteOperationMessageWithDirectReply() {
    super();
  }


  public RemoteOperationMessageWithDirectReply(Set recipients, String regionPath,
      DirectReplyProcessor processor) {
    super(recipients, regionPath, processor);
    this.processor = processor;
  }

  public RemoteOperationMessageWithDirectReply(InternalDistributedMember recipient, String regionPath,
      DirectReplyProcessor processor) {
    super(recipient, regionPath, processor);
    this.processor = processor;
  }
  
  /**
   * @param original
   */
  public RemoteOperationMessageWithDirectReply(RemoteOperationMessageWithDirectReply original) {
    super(original);
    this.processor = original.processor;
  }


  public boolean supportsDirectAck() {
    return true;
  }
  
  public DirectReplyProcessor getDirectReplyProcessor() {
    return processor;
  }

  public void registerProcessor() {
    this.processorId = processor.register();
  }
}
