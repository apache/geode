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
package org.apache.geode.internal.cache.tx;

import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DirectReplyMessage;

/**
 * Used for remote operation messages which support direct ack responses. Direct ack should be used
 * for message with a response from a single member, or responses which are small.
 *
 * Messages that extend this class *must* reply using the ReplySender returned by
 * {@link DistributionMessage#getReplySender(DistributionManager)}
 *
 */
public abstract class RemoteOperationMessageWithDirectReply extends RemoteOperationMessage
    implements DirectReplyMessage {

  protected DirectReplyProcessor processor;



  public RemoteOperationMessageWithDirectReply() {
    super();
  }

  public RemoteOperationMessageWithDirectReply(InternalDistributedMember recipient,
      String regionPath, DirectReplyProcessor processor) {
    super(recipient, regionPath, processor);
    this.processor = processor;
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
}
