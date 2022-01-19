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
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DirectReplyMessage;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.serialization.DeserializationContext;

/**
 * Used for partitioned region messages which support direct ack responses. Direct ack should be
 * used for message with a response from a single member, or responses which are small.
 *
 * Messages that extend this class *must* reply using the ReplySender returned by
 * {@link DistributionMessage#getReplySender(DistributionManager)}
 *
 * Additionally, if the ReplyProcessor used for this message extends PartitionResponse, it should
 * pass false for the register parameter of the PartitionResponse.
 *
 */
public abstract class PartitionMessageWithDirectReply extends PartitionMessage
    implements DirectReplyMessage {

  protected DirectReplyProcessor processor;

  protected boolean posDup = false;

  public PartitionMessageWithDirectReply() {
    super();
  }

  public PartitionMessageWithDirectReply(Collection<InternalDistributedMember> recipients,
      int regionId, DirectReplyProcessor processor) {
    super(recipients, regionId, processor);
    this.processor = processor;
    posDup = false;
  }

  public PartitionMessageWithDirectReply(Set recipients, int regionId,
      DirectReplyProcessor processor, EntryEventImpl event) {
    super(recipients, regionId, processor);
    this.processor = processor;
    posDup = event.isPossibleDuplicate();
  }

  public PartitionMessageWithDirectReply(InternalDistributedMember recipient, int regionId,
      DirectReplyProcessor processor) {
    super(recipient, regionId, processor);
    this.processor = processor;
  }

  public PartitionMessageWithDirectReply(PartitionMessageWithDirectReply original,
      EntryEventImpl event) {
    super(original);
    processor = original.processor;
    if (event != null) {
      posDup = event.isPossibleDuplicate();
    } else {
      posDup = original.posDup;
    }
  }

  @Override
  public boolean supportsDirectAck() {
    return true;
  }

  @Override
  public DirectReplyProcessor getDirectReplyProcessor() {
    return processor;
  }

  @Override
  public void registerProcessor() {
    processorId = processor.register();
  }

  @Override
  public Set relayToListeners(Set cacheOpRecipients, Set adjunctRecipients,
      FilterRoutingInfo filterRoutingInfo, EntryEventImpl event, PartitionedRegion r,
      DirectReplyProcessor p) {
    processor = p;
    return super.relayToListeners(cacheOpRecipients, adjunctRecipients, filterRoutingInfo, event, r,
        p);
  }

  @Override
  public boolean dropMessageWhenMembershipIsPlayingDead() {
    return true;
  }

  @Override
  protected short computeCompressedShort(short s) {
    s = super.computeCompressedShort(s);
    if (posDup) {
      s |= POS_DUP;
    }
    return s;
  }

  @Override
  protected void setBooleans(short s, DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.setBooleans(s, in, context);
    if ((s & POS_DUP) != 0) {
      posDup = true;
    }
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; posDup=").append(posDup);
  }

  @Override
  public boolean canStartRemoteTransaction() {
    return true;
  }
}
