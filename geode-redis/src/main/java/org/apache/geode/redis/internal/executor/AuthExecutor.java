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
package org.apache.geode.redis.internal.executor;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Executor;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class AuthExecutor implements Executor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.AUTH));
      return;
    }
    byte[] password = context.getAuthPassword();
    if (password == null) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_NO_PASS));
      return;
    }
    boolean correct = Arrays.equals(commandElems.get(1), password);

    if (correct) {
      context.setAuthenticationVerified();
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), "OK"));
    } else {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_INVALID_PWD));
    }
  }

}
