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
package org.apache.geode.redis.internal.executor.hash;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_ILLEGAL_GLOB;

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.key.AbstractScanExecutor;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * Implementation of the HScan command used to incrementally iterate over a collection of elements.
 */
public class HScanExecutor extends AbstractScanExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    ByteArrayWrapper key = command.getKey();

    byte[] cAr = commandElems.get(2);
    String cursorString = Coder.bytesToString(cAr);

    int cursor = 0;
    Pattern matchPattern = null;
    String globMatchPattern = null;
    int count = DEFAULT_COUNT;
    try {
      cursor = Integer.parseInt(cursorString);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_CURSOR);
    }

    if (cursor < 0) {
      return RedisResponse.error(ERROR_CURSOR);
    }

    if (commandElems.size() > 4) {
      try {
        byte[] bytes = commandElems.get(3);
        String tmp = Coder.bytesToString(bytes);
        if (tmp.equalsIgnoreCase("MATCH")) {
          bytes = commandElems.get(4);
          globMatchPattern = Coder.bytesToString(bytes);
        } else if (tmp.equalsIgnoreCase("COUNT")) {
          bytes = commandElems.get(4);
          count = Coder.bytesToInt(bytes);
        }
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_COUNT);
      }
    }

    if (commandElems.size() > 6) {
      try {
        byte[] bytes = commandElems.get(5);
        String tmp = Coder.bytesToString(bytes);
        if (tmp.equalsIgnoreCase("MATCH")) {
          bytes = commandElems.get(6);
          globMatchPattern = Coder.bytesToString(bytes);
        } else if (tmp.equalsIgnoreCase("COUNT")) {
          bytes = commandElems.get(6);
          count = Coder.bytesToInt(bytes);
        }
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_COUNT);
      }
    }

    if (count < 0) {
      return RedisResponse.error(ERROR_COUNT);
    }

    try {
      matchPattern = convertGlobToRegex(globMatchPattern);
    } catch (PatternSyntaxException e) {
      return RedisResponse.error(ERROR_ILLEGAL_GLOB);
    }

    RedisHashCommands redisHashCommands =
        new RedisHashCommandsFunctionExecutor(context.getRegionProvider().getDataRegion());
    List<Object> returnList = redisHashCommands.hscan(key, matchPattern, count, cursor);

    if (returnList.isEmpty()) {
      return RedisResponse.error(ERROR_CURSOR);
    } else {
      return RedisResponse.scan(returnList);
    }
  }
}
