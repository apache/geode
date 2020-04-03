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

public class SubscribeExecutor extends AbstractExecutor {
  private static final Logger logger = LogService.getLogger();

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    ArrayList<ArrayList<Object>> items = new ArrayList<>();
    for (int i = 1; i < command.getProcessedCommand().size(); i++) {
      ArrayList<Object> item = new ArrayList<>();
      byte[] channelName = command.getProcessedCommand().get(i);
      long subscribedChannels =
          context.getPubSub().subscribe(new String(channelName), context, context.getClient());

      item.add("subscribe");
      item.add(channelName);
      item.add(subscribedChannels);

      items.add(item);
    }

    writeResponse(command, context, items);
  }

  private void writeResponse(Command command, ExecutionHandlerContext context,
      ArrayList<ArrayList<Object>> items) {
    ByteBuf aggregatedResponse = context.getByteBufAllocator().buffer();
    items.forEach(item -> {
      ByteBuf response = null;
      try {
        response = Coder.getArrayResponse(context.getByteBufAllocator(), item);
      } catch (CoderException e) {
        logger.warn("Error encoding subscribe response", e);
      }
      if (response != null) {
        aggregatedResponse.writeBytes(response);
        response.release();
      }
    });
    command.setResponse(aggregatedResponse);
  }

}
