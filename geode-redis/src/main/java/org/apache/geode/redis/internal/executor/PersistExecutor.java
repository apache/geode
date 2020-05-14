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

import java.util.List;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class PersistExecutor extends AbstractExecutor {

  private final int TIMEOUT_REMOVED = 1;

  private final int KEY_NOT_EXIST_OR_NO_TIMEOUT = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() != 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.PERSIST));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    boolean canceled = context.getRegionProvider().cancelKeyExpiration(key);

    if (canceled) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), TIMEOUT_REMOVED));
    } else {
      command.setResponse(
          Coder.getIntegerResponse(context.getByteBufAllocator(), KEY_NOT_EXIST_OR_NO_TIMEOUT));
    }
  }

}
