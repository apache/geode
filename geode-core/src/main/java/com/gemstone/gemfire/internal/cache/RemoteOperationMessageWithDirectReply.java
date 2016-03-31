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
