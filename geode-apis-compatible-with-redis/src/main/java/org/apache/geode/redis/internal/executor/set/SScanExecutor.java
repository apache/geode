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
package org.apache.geode.redis.internal.executor.set;


import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bCOUNT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMATCH;

import java.math.BigInteger;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisDataType;
import org.apache.geode.redis.internal.data.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.key.AbstractScanExecutor;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SScanExecutor extends AbstractScanExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    String cursorString = bytesToString(commandElems.get(2));
    BigInteger cursor;
    Pattern matchPattern;
    byte[] globPattern = null;
    int count = DEFAULT_COUNT;

    try {
      cursor = new BigInteger(cursorString).abs();
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_CURSOR);
    }

    if (cursor.compareTo(UNSIGNED_LONG_CAPACITY) > 0) {
      return RedisResponse.error(ERROR_CURSOR);
    }

    RedisKey key = command.getKey();

    RedisData value = context.getRegionProvider().getRedisData(key);
    if (value.isNull()) {
      context.getRedisStats().incKeyspaceMisses();
      return RedisResponse.emptyScan();
    }

    if (value.getType() != REDIS_SET) {
      throw new RedisDataTypeMismatchException(ERROR_WRONG_TYPE);
    }

    command.getCommandType().checkDeferredParameters(command, context);

    if (!cursor.equals(context.getSscanCursor())) {
      cursor = new BigInteger("0");
    }

    for (int i = 3; i < commandElems.size(); i = i + 2) {
      byte[] commandElemBytes = commandElems.get(i);
      if (equalsIgnoreCaseBytes(commandElemBytes, bMATCH)) {
        commandElemBytes = commandElems.get(i + 1);
        globPattern = commandElemBytes;

      } else if (equalsIgnoreCaseBytes(commandElemBytes, bCOUNT)) {
        commandElemBytes = commandElems.get(i + 1);
        try {
          count = narrowLongToInt(bytesToLong(commandElemBytes));
        } catch (NumberFormatException e) {
          return RedisResponse.error(ERROR_NOT_INTEGER);
        }

        if (count < 1) {
          return RedisResponse.error(ERROR_SYNTAX);
        }

      } else {
        return RedisResponse.error(ERROR_SYNTAX);
      }
    }

    try {
      matchPattern = convertGlobToRegex(globPattern);
    } catch (PatternSyntaxException e) {
      LogService.getLogger().warn(
          "Could not compile the pattern: '{}' due to the following exception: '{}'. SSCAN will return an empty list.",
          globPattern, e.getMessage());
      return RedisResponse.emptyScan();
    }

    Pair<BigInteger, List<Object>> scanResult =
        context.getSetCommands().sscan(key, matchPattern, count, cursor);

    context.setSscanCursor(scanResult.getLeft());

    return RedisResponse.scan(scanResult.getLeft().intValue(), scanResult.getRight());
  }

  // TODO: When SSCAN is supported, refactor to use these methods and not override executeCommand()
  @Override
  protected Pair<Integer, List<byte[]>> executeScan(ExecutionHandlerContext context, RedisKey key,
      Pattern pattern, int count, int cursor) {
    return null;
  }

  @Override
  protected RedisDataType getDataType() {
    return null;
  }
}
