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
package org.apache.geode.redis.internal.commands.executor.sortedset;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WEIGHT_NOT_A_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.Coder.toUpperCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.AGGREGATE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.WEIGHTS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.services.RegionProvider;

public abstract class ZStoreExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElements = command.getProcessedCommand();
    ListIterator<byte[]> argIterator = commandElements.listIterator();
    RedisResponse syntaxErrorResponse = RedisResponse.error(ERROR_SYNTAX);

    // Skip command and destination key
    argIterator.next();
    argIterator.next();

    // get the number of keys
    int numKeys;
    try {
      numKeys = narrowLongToInt(Coder.bytesToLong(argIterator.next()));
      if (numKeys > commandElements.size() - 2) {
        return syntaxErrorResponse;
      } else if (numKeys <= 0) {
        return getKeyRequiredError();
      }
    } catch (NumberFormatException ex) {
      return RedisResponse.error(ERROR_NOT_INTEGER);
    }

    List<ZKeyWeight> keyWeights = new ArrayList<>(numKeys);
    ZAggregator aggregator = ZAggregator.SUM;
    byte[] argument;

    // get all the keys
    for (int i = 0; i < numKeys; i++) {
      if (!argIterator.hasNext()) {
        return syntaxErrorResponse;
      }
      argument = argIterator.next();
      keyWeights.add(new ZKeyWeight(new RedisKey(argument), 1D));
    }

    while (argIterator.hasNext()) {
      argument = argIterator.next();
      // found AGGREGATE keyword; parse aggregate
      if (Arrays.equals(toUpperCaseBytes(argument), AGGREGATE)) {
        if (!argIterator.hasNext()) {
          return syntaxErrorResponse;
        }
        argument = argIterator.next();
        try {
          aggregator = ZAggregator.valueOf(Coder.bytesToString(toUpperCaseBytes(argument)));
        } catch (IllegalArgumentException e) {
          return syntaxErrorResponse;
        }
        // found WEIGHTS keyword; parse weights
      } else if (Arrays.equals(toUpperCaseBytes(argument), WEIGHTS)) {
        for (int i = 0; i < numKeys; i++) {
          if (!argIterator.hasNext()) {
            return syntaxErrorResponse;
          }
          argument = argIterator.next();
          try {
            keyWeights.get(i).setWeight(Coder.bytesToDouble(argument));
          } catch (NumberFormatException e) {
            return RedisResponse.error(ERROR_WEIGHT_NOT_A_FLOAT);
          }
        }
      } else {
        return syntaxErrorResponse;
      }
    }

    int slot = command.getKey().getSlot();
    for (ZKeyWeight keyWeight : keyWeights) {
      if (keyWeight.getKey().getSlot() != slot) {
        return RedisResponse.crossSlot(ERROR_WRONG_SLOT);
      }
    }

    return RedisResponse.integer(getResult(context, command, keyWeights, aggregator));
  }

  protected List<RedisKey> getKeysToLock(RegionProvider regionProvider, RedisKey destinationKey,
      List<ZKeyWeight> keyWeights) {
    List<RedisKey> keysToLock = new ArrayList<>(keyWeights.size());
    for (ZKeyWeight kw : keyWeights) {
      regionProvider.ensureKeyIsLocal(kw.getKey());
      keysToLock.add(kw.getKey());
    }
    regionProvider.ensureKeyIsLocal(destinationKey);
    keysToLock.add(destinationKey);

    return keysToLock;
  }

  protected abstract RedisResponse getKeyRequiredError();

  protected abstract long getResult(ExecutionHandlerContext context, Command command,
      List<ZKeyWeight> keyWeights, ZAggregator aggregator);
}
