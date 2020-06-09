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
 *
 */

package org.apache.geode.redis.internal.executor.pubsub;

import java.util.ArrayList;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PunsubscribeExecutor extends AbstractExecutor {
  private static final Logger logger = LogService.getLogger();

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    byte[] pattern = command.getProcessedCommand().get(1);
    long subscriptionCount =
        context
            .getPubSub()
            .punsubscribe(new GlobPattern(new String(pattern)), context.getClient());

    ArrayList<Object> items = new ArrayList<>();
    items.add("punsubscribe");
    items.add(pattern);
    items.add(subscriptionCount);

    return RedisResponse.array(items);
  }
}
