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
package org.apache.geode.redis.internal.executor.key;

import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class KeysExecutor extends AbstractExecutor {
  private static final Logger logger = LogService.getLogger();

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    String glob = bytesToString(commandElems.get(1));
    Set<RedisKey> allKeys = getDataRegion(context).keySet();
    List<RedisKey> matchingKeys = new ArrayList<>();

    Pattern pattern;
    try {
      pattern = GlobPattern.compile(glob);
    } catch (PatternSyntaxException e) {
      logger.warn(
          "Could not compile the pattern: '{}' due to the following exception: '{}'. KEYS will return an empty list.",
          glob, e.getMessage());
      return RedisResponse.emptyArray();
    }

    for (RedisKey bytesKey : allKeys) {
      String key = bytesKey.toString();
      if (pattern.matcher(key).matches()) {
        matchingKeys.add(bytesKey);
      }
    }

    if (matchingKeys.isEmpty()) {
      return RedisResponse.emptyArray();
    }

    return respondBulkStrings(matchingKeys);
  }
}
