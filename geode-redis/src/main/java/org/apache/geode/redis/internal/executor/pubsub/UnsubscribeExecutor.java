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

package org.apache.geode.redis.internal.executor.pubsub;

import java.util.ArrayList;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public class UnsubscribeExecutor extends AbstractExecutor {
  private static final Logger logger = LogService.getLogger();

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    byte[] channelName = command.getProcessedCommand().get(1);
    long subscriptionCount =
        context.getPubSub().unsubscribe(new String(channelName), context.getClient());

    ArrayList<Object> items = new ArrayList<>();
    items.add("unsubscribe");
    items.add(channelName);
    items.add(subscriptionCount);

    ByteBuf response = null;
    try {
      response = Coder.getArrayResponse(context.getByteBufAllocator(), items);
    } catch (CoderException e) {
      logger.warn("Error encoding unsubscribe response", e);
    }

    command.setResponse(response);
  }

}
