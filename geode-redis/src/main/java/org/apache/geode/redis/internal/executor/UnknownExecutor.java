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
 *
 */
package org.apache.geode.redis.internal.executor;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_COMMAND;

import java.util.Collection;

import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class UnknownExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {

    StringBuilder commandProcessedText = new StringBuilder();

    Collection<byte[]> processedCmds = command.getProcessedCommand();

    if (processedCmds != null && !processedCmds.isEmpty()) {

      for (byte[] bytes : processedCmds) {
        if (bytes == null || bytes.length == 0) {
          continue; // skip blanks
        }

        commandProcessedText.append(Coder.bytesToString(bytes)).append(" ");
      }
    }

    return RedisResponse.error(ERROR_UNKNOWN_COMMAND + " " + commandProcessedText);
  }
}
