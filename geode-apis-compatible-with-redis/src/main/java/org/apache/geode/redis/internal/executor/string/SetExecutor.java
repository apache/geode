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

import static java.lang.Long.parseLong;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_EXPIRE_TIME;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SetExecutor extends StringExecutor {

  private static final int VALUE_INDEX = 2;
  private static final String SUCCESS = "OK";

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    RedisKey keyToSet = command.getKey();
    List<byte[]> commandElementsBytes = command.getProcessedCommand();
    List<byte[]> optionalParameterBytes = getOptionalParameters(commandElementsBytes);
    RedisStringCommands redisStringCommands = getRedisStringCommands(context);
    SetOptions setOptions;

    try {
      setOptions = parseOptionalParameters(optionalParameterBytes);
    } catch (IllegalArgumentException ex) {
      return RedisResponse.error(ex.getMessage());
    }

    return doSet(keyToSet, commandElementsBytes.get(VALUE_INDEX), redisStringCommands, setOptions);
  }

  private List<byte[]> getOptionalParameters(List<byte[]> commandElementsBytes) {
    return commandElementsBytes.subList(3, commandElementsBytes.size());
  }

  private RedisResponse doSet(RedisKey key, byte[] value, RedisStringCommands redisStringCommands,
      SetOptions setOptions) {

    boolean setCompletedSuccessfully = redisStringCommands.set(key, value, setOptions);

    if (setCompletedSuccessfully) {
      return RedisResponse.string(SUCCESS);
    } else {
      return RedisResponse.nil();
    }
  }

  private SetOptions parseOptionalParameters(List<byte[]> optionalParameterBytes)
      throws IllegalArgumentException {

    boolean keepTTL = false;
    SetOptions.Exists existsOption = SetOptions.Exists.NONE;
    long millisecondsUntilExpiration = 0L;

    List<String> optionalParametersStrings =
        optionalParameterBytes.stream()
            .map(item -> Coder.bytesToString(item).toUpperCase())
            .collect(Collectors.toList());

    throwExceptionIfIncompatableParameterOptions(optionalParametersStrings);
    throwErrorIfNumberInWrongPosition(optionalParametersStrings);
    throwExceptionIfUnknownParameter(optionalParametersStrings);

    // uncomment below when this functionality is reimplemented see GEODE-8263
    // keepTTL = optionalParametersStrings.contains("KEEPTTL");

    if (optionalParametersStrings.contains("PX")) {
      millisecondsUntilExpiration =
          handleExpiration(optionalParametersStrings, "PX");

    } else if (optionalParametersStrings.contains("EX")) {
      millisecondsUntilExpiration =
          handleExpiration(optionalParametersStrings, "EX");
    }

    if (optionalParametersStrings.contains("NX")) {
      existsOption = SetOptions.Exists.NX;
    } else if (optionalParametersStrings.contains("XX")) {
      existsOption = SetOptions.Exists.XX;
    }

    return new SetOptions(existsOption, millisecondsUntilExpiration, keepTTL);
  }

  private long handleExpiration(List<String> optionalParametersStrings, String expirationType) {
    long timeUntilExpiration;
    long millisecondsUntilExpiration;

    String nextParameter =
        getNextParameter(expirationType, optionalParametersStrings);

    timeUntilExpiration =
        convertToLongOrThrowException(nextParameter);

    if (timeUntilExpiration <= 0) {
      throw new IllegalArgumentException(ERROR_INVALID_EXPIRE_TIME);
    }

    if (expirationType.equals("EX")) {
      millisecondsUntilExpiration =
          SECONDS.toMillis(timeUntilExpiration);
    } else {
      millisecondsUntilExpiration = timeUntilExpiration;
    }
    return millisecondsUntilExpiration;
  }

  private String getNextParameter(String currentParameter,
      List<String> optionalParametersStrings) {
    int index = optionalParametersStrings.indexOf(currentParameter);
    if (optionalParametersStrings.size() <= index + 1) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }
    return optionalParametersStrings.get(index + 1);
  }

  private void throwExceptionIfUnknownParameter(List<String> optionalParameters) {
    List<String> validOptionalParamaters = Arrays.asList("EX", "PX", "NX", "XX");

    List<String> parametersInQuestion =
        optionalParameters
            .stream()
            .filter(parameter -> (!validOptionalParamaters.contains(parameter)))
            .collect(Collectors.toList());

    parametersInQuestion.forEach(parameter -> {

      int index = optionalParameters.indexOf(parameter);

      if (!isANumber(parameter)) {
        if (index == 0) {
          throw new IllegalArgumentException(ERROR_SYNTAX);
        }

        String previousParameter = optionalParameters.get(index - 1);
        if (previousOptionIsValidAndExpectsANumber(previousParameter)) {
          throw new IllegalArgumentException(ERROR_NOT_INTEGER);
        }

        throw new IllegalArgumentException(ERROR_SYNTAX);
      }
    });
  }

  private boolean previousOptionIsValidAndExpectsANumber(String previousParameter) {
    List<String> validParamaters = Arrays.asList("EX", "PX");
    return validParamaters.contains(previousParameter);
  }

  private void throwErrorIfNumberInWrongPosition(List<String> optionalParameters) {
    for (int i = 0; i < optionalParameters.size(); i++) {
      String parameter = optionalParameters.get(i);
      if (isANumber(parameter)) {
        if (i == 0) {
          throw new IllegalArgumentException(ERROR_SYNTAX);
        }
        String previousParameter = optionalParameters.get(i - 1);
        if (!previousOptionIsValidAndExpectsANumber(previousParameter)) {
          throw new IllegalArgumentException(ERROR_SYNTAX);
        }
      }
    }
  }

  private boolean isANumber(String parameter) {
    try {
      Long.parseLong(parameter);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private void throwExceptionIfIncompatableParameterOptions(List<String> passedParametersStrings) {

    if (passedParametersStrings.contains("PX")
        && passedParametersStrings.contains("EX")) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }

    if (passedParametersStrings.contains("XX")
        && passedParametersStrings.contains("NX")) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }
  }

  private long convertToLongOrThrowException(String expirationTime) {
    try {
      return parseLong(expirationTime);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(ERROR_NOT_INTEGER);
    }
  }
}
