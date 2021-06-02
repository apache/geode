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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.org.apache.hadoop.fs.GlobPattern;

public class KeysExecutor extends AbstractExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.KEYS));
      return;
    }

    String glob = Coder.bytesToString(commandElems.get(1));
    Set<String> allKeys = context.getKeyRegistrar().keys();
    List<String> matchingKeys = new ArrayList<String>();

    Pattern pattern;
    try {
      pattern = GlobPattern.compile(glob);
    } catch (PatternSyntaxException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_ILLEGAL_GLOB));
      return;
    }

    for (String key : allKeys) {
      if (!(key.equals(GeodeRedisServer.REDIS_META_DATA_REGION)
          || key.equals(GeodeRedisServer.STRING_REGION) || key.equals(GeodeRedisServer.HLL_REGION))
          && pattern.matcher(key).matches()) {
        matchingKeys.add(key);
      }
    }

    if (matchingKeys.isEmpty()) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
    } else {
      respondBulkStrings(command, context, matchingKeys);
    }
  }
}
