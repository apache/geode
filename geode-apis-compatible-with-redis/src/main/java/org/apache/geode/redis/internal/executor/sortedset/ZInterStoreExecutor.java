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
package org.apache.geode.redis.internal.executor.sortedset;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WEIGHT_NOT_A_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;
import static org.apache.geode.redis.internal.netty.Coder.toUpperCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bAGGREGATE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bWEIGHTS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZInterStoreExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElements = command.getProcessedCommand();

    Iterator<byte[]> argIterator = commandElements.iterator();
    // Skip command and destination key
    argIterator.next();
    argIterator.next();

    long numKeys;
    try {
      numKeys = Coder.bytesToLong(argIterator.next());
    } catch (NumberFormatException ex) {
      return RedisResponse.error(ERROR_SYNTAX);
    }

    // Rough validation so that we can use numKeys to initialize the array sizes below.
    if (numKeys > commandElements.size()) {
      return RedisResponse.error(ERROR_SYNTAX);
    }

    List<RedisKey> sourceKeys = new ArrayList<>((int) numKeys);
    List<Double> weights = new ArrayList<>((int) numKeys);
    ZAggregator aggregator = ZAggregator.SUM;

    while (argIterator.hasNext()) {
      byte[] arg = argIterator.next();

      if (sourceKeys.size() < numKeys) {
        sourceKeys.add(new RedisKey(arg));
        continue;
      }

      arg = toUpperCaseBytes(arg);
      if (Arrays.equals(arg, bWEIGHTS)) {
        if (!weights.isEmpty()) {
          return RedisResponse.error(ERROR_SYNTAX);
        }
        for (int i = 0; i < numKeys; i++) {
          if (!argIterator.hasNext()) {
            return RedisResponse.error(ERROR_SYNTAX);
          }
          try {
            weights.add(Coder.bytesToDouble(argIterator.next()));
          } catch (NumberFormatException nex) {
            return RedisResponse.error(ERROR_WEIGHT_NOT_A_FLOAT);
          }
        }
        continue;
      }

      if (Arrays.equals(arg, bAGGREGATE)) {
        try {
          aggregator = ZAggregator.valueOf(Coder.bytesToString(argIterator.next()));
        } catch (IllegalArgumentException | NoSuchElementException e) {
          return RedisResponse.error(ERROR_SYNTAX);
        }
        continue;
      }

      // End up here if we have more keys than weights
      return RedisResponse.error(ERROR_SYNTAX);
    }

    if (sourceKeys.size() != numKeys) {
      return RedisResponse.error(ERROR_SYNTAX);
    }

    int bucket = command.getKey().getBucketId();
    for (RedisKey key : sourceKeys) {
      if (key.getBucketId() != bucket) {
        return RedisResponse.crossSlot(ERROR_WRONG_SLOT);
      }
    }

    List<ZKeyWeight> keyWeights = new ArrayList<>();
    for (int i = 0; i < sourceKeys.size(); i++) {
      double weight = weights.isEmpty() ? 1 : weights.get(i);
      keyWeights.add(new ZKeyWeight(sourceKeys.get(i), weight));
    }

    long result = context.getSortedSetCommands()
        .zinterstore(command.getKey(), keyWeights, aggregator);

    return RedisResponse.integer(result);
  }

}
