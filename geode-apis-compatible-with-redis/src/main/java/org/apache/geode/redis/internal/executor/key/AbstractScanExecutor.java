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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bCOUNT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMATCH;

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisDataType;
import org.apache.geode.redis.internal.data.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public abstract class AbstractScanExecutor extends AbstractExecutor {
  private static final Logger logger = LogService.getLogger();
  protected final int DEFAULT_COUNT = 10;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    int cursor;
    try {
      cursor = narrowLongToInt(bytesToLong(commandElems.get(2)));
    } catch (NumberFormatException ex) {
      return RedisResponse.error(ERROR_CURSOR);
    }

    RedisKey key = command.getKey();

    // Because we're trying to preserve the same semantics of error conditions, with native redis,
    // the ordering of input validation is reflected here. To that end the first check ends up
    // being an existence check of the key. That causes a race since the data value needs to be
    // accessed again when the actual command does its work. If the relevant bucket doesn't get
    // locked throughout the call, the bucket may move producing inconsistent results.
    return context.getRegionProvider().execute(key, () -> {
      RedisData value = context.getRegionProvider().getRedisData(key);
      if (value.isNull()) {
        context.getRedisStats().incKeyspaceMisses();
        return RedisResponse.emptyScan();
      }

      if (value.getType() != getDataType()) {
        throw new RedisDataTypeMismatchException(ERROR_WRONG_TYPE);
      }

      command.getCommandType().checkDeferredParameters(command, context);
      int count = DEFAULT_COUNT;
      byte[] globPattern = null;

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

      Pattern matchPattern;
      try {
        matchPattern = convertGlobToRegex(globPattern);
      } catch (PatternSyntaxException e) {
        logger.warn(
            "Could not compile the pattern: '{}' due to the following exception: '{}'. {} will return an empty list.",
            globPattern, e.getMessage(), command.getCommandType().name());

        return RedisResponse.emptyScan();
      }

      Pair<Integer, List<byte[]>> scanResult;
      try {
        scanResult = executeScan(context, key, matchPattern, count, cursor);
      } catch (IllegalArgumentException ex) {
        return RedisResponse.error(ERROR_NOT_INTEGER);
      }

      return RedisResponse.scan(scanResult.getLeft(), scanResult.getRight());
    });
  }

  /**
   * @param pattern A glob pattern.
   * @return A regex pattern to recognize the given glob pattern.
   */
  @VisibleForTesting
  public Pattern convertGlobToRegex(byte[] pattern) {
    if (pattern == null) {
      return null;
    }
    return GlobPattern.createPattern(pattern);
  }

  protected abstract Pair<Integer, List<byte[]>> executeScan(ExecutionHandlerContext context,
      RedisKey key, Pattern pattern, int count, int cursor);

  protected abstract RedisDataType getDataType();
}
