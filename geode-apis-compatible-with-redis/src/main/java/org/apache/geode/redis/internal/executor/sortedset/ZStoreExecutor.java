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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_KEY_REQUIRED;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WEIGHT_NOT_A_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.Coder.toUpperCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bAGGREGATE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bWEIGHTS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public abstract class ZStoreExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElements = command.getProcessedCommand();
    boolean weightsFound = false;

    ListIterator<byte[]> argIterator = commandElements.listIterator();
    // Skip command and destination key
    argIterator.next();
    argIterator.next();

    int numKeys;
    try {
      numKeys = narrowLongToInt(Coder.bytesToLong(argIterator.next()));
    } catch (NumberFormatException ex) {
      return RedisResponse.error(ERROR_SYNTAX);
    }

    // Rough validation so that we can use numKeys to initialize the array sizes below.
    if (numKeys > commandElements.size() - 2) {
      return RedisResponse.error(ERROR_SYNTAX);
    }

    if (numKeys <= 0) {
      return RedisResponse.error(ERROR_KEY_REQUIRED);
    }

    List<ZKeyWeight> keyWeights = new ArrayList<>(numKeys);
    ZAggregator aggregator = ZAggregator.SUM;
    byte[] argument;
    boolean aggOrWeightFound = false;
    boolean aggregateFound = false;

    do {
      int numWeights = 0;
      argument = argIterator.next();

      if (Arrays.equals(toUpperCaseBytes(argument), bWEIGHTS)) {
        aggOrWeightFound = true;

        if (!argIterator.hasNext()) {
          return RedisResponse.error(ERROR_SYNTAX);
        }

        // start parsing weights
        for (int i = 0; argIterator.hasNext(); i++) {
          argument = argIterator.next();

          if (Arrays.equals(toUpperCaseBytes(argument), bAGGREGATE)) {
            if (numWeights == numKeys) {
              break;
            } else {
              return RedisResponse.error(ERROR_SYNTAX);
            }
          }

          if (keyWeights.size() > numKeys || i >= numKeys) {
            return RedisResponse.error(ERROR_SYNTAX);
          }
          try {
            double arg = Coder.bytesToDouble(argument);
            keyWeights.get(i).setWeight(arg);
            numWeights++;
          } catch (NumberFormatException e) {
            return RedisResponse.error(ERROR_WEIGHT_NOT_A_FLOAT);
          }
        }

        if (numWeights != numKeys) {
          return RedisResponse.error(ERROR_SYNTAX);
        }


      } else if (Arrays.equals(toUpperCaseBytes(argument), bAGGREGATE)) {
        aggOrWeightFound = true;

        // the iterator must have at least one more, and we must have the expected number of keys
        if (!argIterator.hasNext() || keyWeights.size() != numKeys) {
          return RedisResponse.error(ERROR_SYNTAX);
        }

        // start parsing aggregates
        argument = argIterator.next();
        if (Arrays.equals(toUpperCaseBytes(argument), bWEIGHTS)) {
          // if we've found at least one aggregate before the weights keyword then we're good
          if (aggregateFound) {
            break;
          } else {
            return RedisResponse.error(ERROR_SYNTAX);
          }
        }

        try {
          aggregator = ZAggregator.valueOf(Coder.bytesToString(toUpperCaseBytes(argument)));
        } catch (IllegalArgumentException e) {
          return RedisResponse.error(ERROR_SYNTAX);
        }
        aggregateFound = true;

      } else {
        // otherwise it's a key
        if (aggOrWeightFound || keyWeights.size() > numKeys) {
          return RedisResponse.error(ERROR_SYNTAX);
        }
        keyWeights.add(new ZKeyWeight(new RedisKey(argument), 1D));
      }

    } while (argIterator.hasNext());

    if (keyWeights.size() != numKeys) {
      return RedisResponse.error(ERROR_SYNTAX);
    }

    int slot = command.getKey().getSlot();
    for (ZKeyWeight keyWeight : keyWeights) {
      if (keyWeight.getKey().getSlot() != slot) {
        return RedisResponse.crossSlot(ERROR_WRONG_SLOT);
      }
    }

    return RedisResponse.integer(getResult(context, command, keyWeights, aggregator));


    //
    //
    // while (argIterator.hasNext()) {
    // byte[] arg = argIterator.next();
    //
    // if (keyWeights.size() < numKeys) {
    // keyWeights.add(new ZKeyWeight(new RedisKey(arg), 1D));
    // continue;
    // }
    //
    // arg = toUpperCaseBytes(arg);
    // if (Arrays.equals(arg, bWEIGHTS)) {
    // if (weightsFound) {
    // return RedisResponse.error(ERROR_SYNTAX);
    // } else {
    // weightsFound = true;
    // }
    // for (int i = 0; i < numKeys; i++) {
    // if (!argIterator.hasNext()) {
    // return RedisResponse.error(ERROR_SYNTAX);
    // }
    // try {
    // keyWeights.get(i).setWeight(Coder.bytesToDouble(argIterator.next()));
    // } catch (NumberFormatException nex) {
    // return RedisResponse.error(ERROR_WEIGHT_NOT_A_FLOAT);
    // }
    // }
    // continue;
    // }
    //
    // if (Arrays.equals(arg, bAGGREGATE)) {
    // try {
    // aggregator = ZAggregator.valueOf(Coder.bytesToString(argIterator.next()));
    // } catch (IllegalArgumentException | NoSuchElementException e) {
    // return RedisResponse.error(ERROR_SYNTAX);
    // }
    // continue;
    // }
    //
    // // End up here if we have finished parsing keys and encounter another option other than
    // // WEIGHTS and AGGREGATE
    // return RedisResponse.error(ERROR_SYNTAX);
    // }
    //
    // if (keyWeights.size() != numKeys) {
    // return RedisResponse.error(ERROR_SYNTAX);
    // }
    //
    // int slot = command.getKey().getSlot();
    // for (ZKeyWeight keyWeight : keyWeights) {
    // if (keyWeight.getKey().getSlot() != slot) {
    // return RedisResponse.crossSlot(ERROR_WRONG_SLOT);
    // }
    // }
    //
    // return RedisResponse.integer(getResult(context, command, keyWeights, aggregator));
  }

  public abstract long getResult(ExecutionHandlerContext context, Command command,
      List<ZKeyWeight> keyWeights, ZAggregator aggregator);
}
