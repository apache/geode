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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_NX_XX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR;
import static org.apache.geode.redis.internal.netty.Coder.isInfinity;
import static org.apache.geode.redis.internal.netty.Coder.toUpperCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bCH;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bINCR;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNX;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bXX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZAddExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    ZAddExecutorState zAddExecutorState = new ZAddExecutorState();
    List<byte[]> commandElements = command.getProcessedCommand();
    Region<RedisKey, RedisData> region = context.getRegion();
    RedisKey key = command.getKey();

    Iterator<byte[]> commandIterator = commandElements.iterator();
    skipCommandAndKey(commandIterator);

    int optionsFoundCount = findAndValidateZAddOptions(command, commandIterator, zAddExecutorState);
    if (zAddExecutorState.exceptionMessage != null) {
      return RedisResponse.error(zAddExecutorState.exceptionMessage);
    }

    int size = (commandElements.size() - optionsFoundCount - 2) / 2;
    List<byte[]> members = new ArrayList<>(size);
    double[] scores = new double[size];
    int n = 0;
    for (int i = optionsFoundCount + 2; i < commandElements.size(); i += 2, n++) {
      try {
        scores[n] = Coder.bytesToDouble(commandElements.get(i));
      } catch (NumberFormatException e) {
        throw new NumberFormatException(ERROR_NOT_A_VALID_FLOAT);
      }
      members.add(commandElements.get(i + 1));
    }
    ZAddOptions options = makeOptions(zAddExecutorState);
    Object retVal = context.sortedSetLockedExecute(key, false,
        zset -> zset.zadd(region, key, members, scores, options));

    if (zAddExecutorState.incrFound) {
      if (retVal == null) {
        return RedisResponse.nil();
      }
      return RedisResponse.bulkString(retVal);
    }

    return RedisResponse.integer((int) retVal);
  }

  private void skipCommandAndKey(Iterator<byte[]> commandIterator) {
    commandIterator.next();
    commandIterator.next();
  }

  private int findAndValidateZAddOptions(Command command, Iterator<byte[]> commandIterator,
      ZAddExecutorState executorState) {
    boolean scoreFound = false;
    int optionsFoundCount = 0;

    while (commandIterator.hasNext() && !scoreFound) {
      byte[] subCommand = toUpperCaseBytes(commandIterator.next());
      if (Arrays.equals(subCommand, bNX)) {
        executorState.nxFound = true;
        optionsFoundCount++;
      } else if (Arrays.equals(subCommand, bXX)) {
        executorState.xxFound = true;
        optionsFoundCount++;
      } else if (Arrays.equals(subCommand, bCH)) {
        executorState.chFound = true;
        optionsFoundCount++;
      } else if (Arrays.equals(subCommand, bINCR)) {
        executorState.incrFound = true;
        optionsFoundCount++;
      } else if (isInfinity(subCommand)) {
        scoreFound = true;
      } else {
        // assume it's a double; if not, this will throw exception later
        scoreFound = true;
      }
    }
    if ((command.getProcessedCommand().size() - optionsFoundCount - 2) % 2 != 0) {
      executorState.exceptionMessage = ERROR_SYNTAX;
    } else if (executorState.nxFound && executorState.xxFound) {
      executorState.exceptionMessage = ERROR_INVALID_ZADD_OPTION_NX_XX;
    } else if (executorState.incrFound
        && command.getProcessedCommand().size() - optionsFoundCount - 2 > 2) {
      executorState.exceptionMessage = ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR;
    }
    return optionsFoundCount;
  }

  private ZAddOptions makeOptions(ZAddExecutorState executorState) {
    ZAddOptions.Exists existsOption = ZAddOptions.Exists.NONE;

    if (executorState.nxFound) {
      existsOption = ZAddOptions.Exists.NX;
    }
    if (executorState.xxFound) {
      existsOption = ZAddOptions.Exists.XX;
    }
    return new ZAddOptions(existsOption, executorState.chFound, executorState.incrFound);
  }

  private static class ZAddExecutorState {
    private boolean nxFound = false;
    private boolean xxFound = false;
    private boolean chFound = false;
    private boolean incrFound = false;
    private String exceptionMessage = null;
  }
}
