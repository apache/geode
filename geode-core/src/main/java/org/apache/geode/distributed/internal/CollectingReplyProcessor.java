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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * This reply processor collects all of the exceptions/results from the ReplyMessages it receives
 *
 *
 */
public class CollectingReplyProcessor<T> extends ReplyProcessor21 {

  private Map<InternalDistributedMember, T> results = new HashMap<InternalDistributedMember, T>();

  public CollectingReplyProcessor(DistributionManager dm, Collection initMembers) {
    super(dm, initMembers);
  }

  @Override
  protected void process(DistributionMessage msg, boolean warn) {
    if (msg instanceof ReplyMessage) {
      InternalDistributedSystem.getLogger().info(String.format("%s",
          "processing message with return value " + ((ReplyMessage) msg).getReturnValue()));
      results.put(msg.getSender(), (T) ((ReplyMessage) msg).getReturnValue());
    }
    super.process(msg, warn);
  }

  public Map<InternalDistributedMember, T> getResults() {
    return this.results;
  }

}
