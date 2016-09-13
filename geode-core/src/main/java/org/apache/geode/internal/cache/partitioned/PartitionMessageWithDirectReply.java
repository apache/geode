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

import java.io.DataInput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo;
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
 *
 */
public abstract class PartitionMessageWithDirectReply extends
    PartitionMessage implements DirectReplyMessage {

  protected DirectReplyProcessor processor;

  protected boolean posDup = false;

  public PartitionMessageWithDirectReply() {
    super();
  }

  public PartitionMessageWithDirectReply(Collection<InternalDistributedMember> recipients, int regionId,
      DirectReplyProcessor processor) {
    super(recipients, regionId, processor);
    this.processor = processor;
    this.posDup = false;
  }

  public PartitionMessageWithDirectReply(Set recipients, int regionId,
      DirectReplyProcessor processor, EntryEventImpl event) {
    super(recipients, regionId, processor);
    this.processor = processor;
    this.posDup = event.isPossibleDuplicate();
  }

  public PartitionMessageWithDirectReply(InternalDistributedMember recipient, int regionId,
      DirectReplyProcessor processor) {
    super(recipient, regionId, processor);
    this.processor = processor;
  }
  
  /**
   * @param original
   */
  public PartitionMessageWithDirectReply(
      PartitionMessageWithDirectReply original, EntryEventImpl event) {
    super(original);
    this.processor = original.processor;
    if (event != null) {
      this.posDup = event.isPossibleDuplicate();
    }
    else {
      this.posDup = original.posDup;
    }
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
  
  @Override
  public Set relayToListeners(Set cacheOpRecipients, Set adjunctRecipients,
      FilterRoutingInfo filterRoutingInfo, 
      EntryEventImpl event, PartitionedRegion r, DirectReplyProcessor p)
  {
    this.processor = p;
    return super.relayToListeners(cacheOpRecipients, adjunctRecipients,
        filterRoutingInfo, event, r, p);
  }

  @Override
  protected short computeCompressedShort(short s) {
    s = super.computeCompressedShort(s);
    if (this.posDup) {
      s |= POS_DUP;
    }
    return s;
  }

  @Override
  protected void setBooleans(short s, DataInput in) throws IOException,
      ClassNotFoundException {
    super.setBooleans(s, in);
    if ((s & POS_DUP) != 0) {
      this.posDup = true;
    }
  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append("; posDup=").append(this.posDup);
  }
  
  @Override
  public boolean canStartRemoteTransaction() {
    return true;
  }
}
