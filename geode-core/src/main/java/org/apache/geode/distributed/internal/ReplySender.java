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
package org.apache.geode.distributed.internal;

import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DirectReplyMessage;

/**
 * This interface is used by direct ack messages to send a reply to the original sender of the
 * message. Any message which implements {@link DirectReplyMessage} must reply by calling
 * putOutgoing on the ReplySender returned by
 * {@link DistributionMessage#getReplySender(DistributionManager)}
 *
 * The reply sender may be the distribution manager itself, or it may send the reply directly back
 * on the same socket the message as received on.
 *
 */
public interface ReplySender {


  /**
   * @param message to send.
   * @return recipients who did not receive the message. May return an immutable set.
   */
  @Nullable
  Set<InternalDistributedMember> putOutgoing(@NotNull DistributionMessage message);

}
