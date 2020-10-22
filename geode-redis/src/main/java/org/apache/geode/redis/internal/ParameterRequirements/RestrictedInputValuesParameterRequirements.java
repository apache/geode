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

package org.apache.geode.redis.internal.ParameterRequirements;

import java.util.List;

import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class RestrictedInputValuesParameterRequirements implements ParameterRequirements {

 private final List<String> allowedValues;
 private final String ERROR_MESSAGE;

  public RestrictedInputValuesParameterRequirements(List<String> allowedValues,
                                                    String errorMessage) {
    this.allowedValues = allowedValues;
    ERROR_MESSAGE = errorMessage;
  }

  @Override
  public void checkParameters(Command command,
      ExecutionHandlerContext executionHandlerContext) {
    List<byte[]> parameters = command.getProcessedCommand();
    String commandType = command.getCommandType().name();

    parameters.forEach(parameter -> {
      String parameterString = new String(parameter);
      if (isNotAllowed(parameterString) &&
          !parameterString.equalsIgnoreCase(commandType)) {
        throw new RedisParametersMismatchException(ERROR_MESSAGE);
      }
    });
  }

  private boolean isNotAllowed(String parameterString) {
    return !(allowedValues.contains(parameterString));
  }
}
