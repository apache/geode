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
package org.apache.geode.redis.internal.executor.string;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_EXPIRE_TIME;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bEX;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNX;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPX;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bXX;

import java.util.List;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.BaseSetOptions;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SetExecutor extends AbstractExecutor {

  private static final int VALUE_INDEX = 2;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    RedisKey keyToSet = command.getKey();
    List<byte[]> commandElementsBytes = command.getProcessedCommand();

    SetOptions setOptions;
    List<byte[]> optionalParameters =
        commandElementsBytes.subList(VALUE_INDEX + 1, commandElementsBytes.size());
    try {
      setOptions = parseOptionalParameters(optionalParameters);
    } catch (IllegalArgumentException ex) {
      return RedisResponse.error(ex.getMessage());
    }

    RedisStringCommands redisStringCommands = context.getStringCommands();
    return doSet(keyToSet, commandElementsBytes.get(VALUE_INDEX), redisStringCommands, setOptions);
  }

  private SetOptions parseOptionalParameters(List<byte[]> optionalParameters)
      throws IllegalArgumentException {

    SetExecutorState executorState = new SetExecutorState();

    // Iterate the list in reverse order to allow similar error reporting behaviour to native redis
    for (int index = optionalParameters.size() - 1; index >= 0; --index) {
      if (equalsIgnoreCaseBytes(optionalParameters.get(index), bXX)) {
        handleXX(executorState);
      } else if (equalsIgnoreCaseBytes(optionalParameters.get(index), bNX)) {
        handleNX(executorState);
      } else {
        // Yhe only valid possibility now is that the parameter is a number preceded by either EX or
        // PX
        handleNumber(executorState, optionalParameters, index);
        // If the above method doesn't throw, we successfully parsed a pair of parameters, so skip
        // the next parameter
        --index;
      }
    }

    if ((executorState.foundPX || executorState.foundEX) && executorState.expirationMillis <= 0) {
      throw new IllegalArgumentException(ERROR_INVALID_EXPIRE_TIME);
    }

    return new SetOptions(executorState.existsOption, executorState.expirationMillis, false);
  }

  private void handleXX(SetExecutorState executorState) {
    if (executorState.foundNX) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    } else {
      executorState.existsOption = BaseSetOptions.Exists.XX;
      executorState.foundXX = true;
    }
  }

  private void handleNX(SetExecutorState executorState) {
    if (executorState.foundXX) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    } else {
      executorState.existsOption = BaseSetOptions.Exists.NX;
      executorState.foundNX = true;
    }
  }

  private void handleNumber(SetExecutorState executorState, List<byte[]> parameters, int index) {
    doBasicValidation(executorState, index);

    byte[] previousParameter = parameters.get(index - 1);
    throwIfNotExpirationParameter(previousParameter);

    long expiration;
    try {
      expiration = bytesToLong(parameters.get(index));
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(ERROR_NOT_INTEGER);
    }

    if (equalsIgnoreCaseBytes(previousParameter, bEX)) {
      handleEX(executorState, expiration);
    } else {
      handlePX(executorState, expiration);
    }
  }

  private void doBasicValidation(SetExecutorState executorState, int index) {
    // The first optional parameter cannot be a number
    if (index == 0) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }

    // We already found and set an expiration value
    if (executorState.expirationMillis != 0) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }
  }

  private void throwIfNotExpirationParameter(byte[] previousParameter) {
    // Numbers must be preceded by either EX or PX
    if (!equalsIgnoreCaseBytes(previousParameter, bEX)
        && !equalsIgnoreCaseBytes(previousParameter, bPX)) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }
  }

  private void handleEX(SetExecutorState executorState, long expiration) {
    if (executorState.foundPX) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }
    executorState.expirationMillis = SECONDS.toMillis(expiration);
    executorState.foundEX = true;
  }

  private void handlePX(SetExecutorState executorState, long expiration) {
    if (executorState.foundEX) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }
    executorState.expirationMillis = expiration;
    executorState.foundPX = true;
  }

  private RedisResponse doSet(RedisKey key, byte[] value, RedisStringCommands redisStringCommands,
      SetOptions setOptions) {

    boolean setCompletedSuccessfully = redisStringCommands.set(key, value, setOptions);

    if (setCompletedSuccessfully) {
      return RedisResponse.ok();
    } else {
      return RedisResponse.nil();
    }
  }

  private static class SetExecutorState {
    SetOptions.Exists existsOption = SetOptions.Exists.NONE;
    long expirationMillis = 0L;
    boolean foundXX = false;
    boolean foundNX = false;
    boolean foundPX = false;
    boolean foundEX = false;
  }
}
