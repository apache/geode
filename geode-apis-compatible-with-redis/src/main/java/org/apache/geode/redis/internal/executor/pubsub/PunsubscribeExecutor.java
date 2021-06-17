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

import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.apache.geode.redis.internal.pubsub.Subscription.Type.PATTERN;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PunsubscribeExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    context.eventLoopReady();

    List<byte[]> patternNames = extractPatternNames(command);
    if (patternNames.isEmpty()) {
      patternNames = context.getPubSub().findSubscriptionNames(context.getClient(), PATTERN);
    }

    Collection<Collection<?>> response = punsubscribe(context, patternNames);

    return RedisResponse.flattenedArray(response);
  }

  private List<byte[]> extractPatternNames(Command command) {
    return command.getProcessedCommand().stream().skip(1).collect(Collectors.toList());
  }

  private Collection<Collection<?>> punsubscribe(ExecutionHandlerContext context,
      List<byte[]> patternNames) {
    Collection<Collection<?>> response = new ArrayList<>();

    if (patternNames.isEmpty()) {
      response.add(createItem(null, 0));
    } else {
      for (byte[] pattern : patternNames) {
        long subscriptionCount =
            context.getPubSub().punsubscribe(new GlobPattern(bytesToString(pattern)),
                context.getClient());
        response.add(createItem(pattern, subscriptionCount));
      }
    }
    return response;
  }

  private ArrayList<Object> createItem(byte[] pattern, long subscriptionCount) {
    ArrayList<Object> oneItem = new ArrayList<>();
    oneItem.add("punsubscribe");
    oneItem.add(pattern);
    oneItem.add(subscriptionCount);
    return oneItem;
  }
}
