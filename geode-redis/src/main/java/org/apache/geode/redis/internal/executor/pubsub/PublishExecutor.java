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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.buffer.ByteBuf;

import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public class PublishExecutor extends AbstractExecutor {

  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> args = command.getProcessedCommand();
    if (args.size() != 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.PUBLISH));
      return;
    }

    String channelName = new String(args.get(1));

    executorService.submit(new PublishingRunnable(context, channelName, args.get(2)));
  }

  public static class PublishingRunnable implements Runnable {

    private final ExecutionHandlerContext context;
    private final String channelName;
    private final byte[] message;

    public PublishingRunnable(ExecutionHandlerContext context, String channelName, byte[] message) {
      this.context = context;
      this.channelName = channelName;
      this.message = message;
    }

    @Override
    public void run() {
      long publishCount = context.getPubSub().publish(channelName, message);
      ByteBuf response = Coder.getIntegerResponse(context.getByteBufAllocator(), publishCount);
      context.writeToChannel(response);
    }
  }

}
