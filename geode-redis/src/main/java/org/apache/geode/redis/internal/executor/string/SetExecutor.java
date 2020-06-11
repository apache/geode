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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SetExecutor extends StringExecutor {

  private static final String SUCCESS = "OK";

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {

    ByteArrayWrapper keyToSet = command.getKey();
    List<byte[]> commandElementsBytes = command.getProcessedCommand();
    List<byte[]> optionalParameterBytes = getOptionalParameters(commandElementsBytes);
    ByteArrayWrapper valueToSet = getValueToSet(commandElementsBytes);
    RedisStringCommands redisStringCommands = getRedisStringCommands(context);
    SetOptions setOptions;

    try {
      setOptions = parseOptionalParameters(optionalParameterBytes);
    } catch (IllegalArgumentException ex) {
      return RedisResponse.error(ex.getMessage());
    }

    return doSet(keyToSet, valueToSet, redisStringCommands, setOptions);
  }

  private List<byte[]> getOptionalParameters(List<byte[]> commandElementsBytes) {
    return commandElementsBytes.subList(3, commandElementsBytes.size());
  }

  private RedisResponse doSet(ByteArrayWrapper key,
      ByteArrayWrapper value,
      RedisStringCommands redisStringCommands,
      SetOptions setOptions) {

    boolean setCompletedSuccessfully = redisStringCommands.set(key, value, setOptions);

    if (setCompletedSuccessfully) {
      return RedisResponse.string(SUCCESS);
    } else {
      return RedisResponse.nil();
    }
  }

  private ByteArrayWrapper getValueToSet(List<byte[]> commandElems) {
    byte[] value = commandElems.get(2);
    return new ByteArrayWrapper(value);
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

    throwExceptionIfUnknownParameter(optionalParametersStrings);
    throwExceptionIfIncompatableParamaterOptions(optionalParametersStrings);
    keepTTL = optionalParametersStrings.contains("KEEPTTL");

    if (optionalParametersStrings.contains("PX")) {
      millisecondsUntilExpiration =
          handleEpiration(optionalParametersStrings, "PX" );

    } else if (optionalParametersStrings.contains("EX")) {
      millisecondsUntilExpiration =
          handleEpiration(optionalParametersStrings, "EX" );
    }

    if (optionalParametersStrings.contains("NX")) {
      existsOption = SetOptions.Exists.NX;
    } else if (optionalParametersStrings.contains("XX")) {
      existsOption = SetOptions.Exists.XX;
    }

    return new SetOptions(existsOption, millisecondsUntilExpiration, keepTTL);
  }

  private long handleEpiration(List<String> optionalParametersStrings, String expirationType) {

    long timeUntilExpiration;
    long millisecondsUntilExpiration;

    String nextParameter =
        getNextParameter(expirationType, optionalParametersStrings);

    timeUntilExpiration =
        convertToLongOrThrowException(nextParameter);

    if (timeUntilExpiration <= 0) {
      throw new IllegalArgumentException(ERROR_INVALID_EXPIRE_TIME);
    }

    if (expirationType.equals("EX")){
      millisecondsUntilExpiration =
          SECONDS.toMillis(timeUntilExpiration);
    }else {
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
    List<String> validOptionalParamaters = Arrays.asList("EX", "PX", "NX", "XX", "KEEPTTL");

    List<String> parametersInQuestion =
        optionalParameters
            .stream()
            .filter(parameter -> (!validOptionalParamaters.contains(parameter)))
            .collect(Collectors.toList());

    parametersInQuestion.forEach(parameter -> throwErrorIfNotANumberInExpectedPosition(
        parameter,
        optionalParameters));
  }

  private void throwErrorIfNotANumberInExpectedPosition(
      String parameter,
      List<String> optionalParameters) {
    if (previousOptionIsValidAndExpectsANumber(parameter, optionalParameters)) {
      convertToLongOrThrowException(parameter);
    } else {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }
  }

  private boolean previousOptionIsValidAndExpectsANumber(String paramter,
      List<String> optionalParameters) {

    List<String> validParamaters = Arrays.asList("EX", "PX");
    if (optionalParameters.size() < 2) {
      return false;
    }

    int indexOfParameter = optionalParameters.indexOf(paramter);
    String previousParameter = optionalParameters.get(indexOfParameter - 1);

    return validParamaters.contains(previousParameter);
  }

  private void throwExceptionIfIncompatableParamaterOptions(List<String> passedParametersStrings) {

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
      return Long.parseLong(expirationTime);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(ERROR_NOT_INTEGER);
    }
  }
}
