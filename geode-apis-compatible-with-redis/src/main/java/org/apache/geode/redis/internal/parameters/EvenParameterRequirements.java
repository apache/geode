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

package org.apache.geode.redis.internal.parameters;

import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class EvenParameterRequirements implements ParameterRequirements {

  private final String errorMessage;

  public EvenParameterRequirements() {
    this.errorMessage = null;
  }

  public EvenParameterRequirements(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  @Override
  public void checkParameters(Command command, ExecutionHandlerContext executionHandlerContext) {
    if (!isEven(command.getProcessedCommand().size())) {
      throw new RedisParametersMismatchException(getErrorMessage(command));
    }
  }

  private boolean isEven(int n) {
    return n % 2 == 0;
  }

  private String getErrorMessage(Command command) {
    if (errorMessage != null) {
      return errorMessage;
    }

    return command.wrongNumberOfArgumentsErrorMessage();
  }
}
