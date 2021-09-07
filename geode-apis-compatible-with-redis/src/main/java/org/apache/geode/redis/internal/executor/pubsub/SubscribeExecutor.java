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

import static java.util.Arrays.asList;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bSUBSCRIBE;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.pubsub.SubscribeResult;
import org.apache.geode.redis.internal.pubsub.Subscription;

public class SubscribeExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {

    Collection<SubscribeResult> results = new ArrayList<>(command.getCommandArguments().size());
    for (byte[] channelName : command.getCommandArguments()) {
      SubscribeResult result =
          context.getPubSub().subscribe(channelName, context);
      results.add(result);
    }

    return createSubscribeResponse(results, bSUBSCRIBE);
  }

  static RedisResponse createSubscribeResponse(Collection<SubscribeResult> results,
      byte[] messageType) {
    Collection<Collection<?>> items = new ArrayList<>(results.size());
    for (SubscribeResult result : results) {
      items.add(asList(messageType, result.getChannel(), result.getChannelCount()));
    }

    Runnable callback = () -> {
      for (SubscribeResult result : results) {
        Subscription subscription = result.getSubscription();
        if (subscription != null) {
          subscription.readyToPublish();
        }
      }
    };

    RedisResponse response = RedisResponse.flattenedArray(items);
    response.setAfterWriteCallback(callback);

    return response;
  }

}
