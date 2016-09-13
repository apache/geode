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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A marker interface that denotes {@link DistributionMessage}s that
 * require a reply.  Messages that do not implement this interface can
 * be sent asynchronously through the transport layer.
 *
 *
 * @since GemFire 3.0
 */
public interface MessageWithReply {

  /**
   * Returns the id of the {@link 
   * com.gemstone.gemfire.distributed.internal.ReplyProcessor21} that is used to
   * handle the reply to this message.
   */
  public int getProcessorId();

  /**
   * Returns the id the sender who is waiting for a reply.
   */
  public InternalDistributedMember getSender();
}
