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
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bCOUNT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMATCH;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.RedisDataType;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ScanExecutor extends AbstractScanExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    String cursorString = command.getStringKey();
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

    if (!cursor.equals(context.getScanCursor())) {
      cursor = new BigInteger("0");
    }

    for (int i = 2; i < commandElems.size(); i = i + 2) {
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
          "Could not compile the pattern: '{}' due to the following exception: '{}'. SCAN will return an empty list.",
          globPattern, e.getMessage());
      return RedisResponse.emptyScan();
    }

    Pair<BigInteger, List<Object>> scanResult =
        scan(getDataRegion(context).keySet(), matchPattern, count, cursor);
    context.setScanCursor(scanResult.getLeft());

    return RedisResponse.scan(scanResult.getLeft().intValue(), scanResult.getRight());
  }

  private Pair<BigInteger, List<Object>> scan(Collection<RedisKey> list,
      Pattern matchPattern,
      int count, BigInteger cursor) {
    List<Object> returnList = new ArrayList<>();
    int size = list.size();
    BigInteger beforeCursor = new BigInteger("0");
    int numElements = 0;
    int i = -1;
    for (RedisKey key : list) {
      i++;
      if (beforeCursor.compareTo(cursor) < 0) {
        beforeCursor = beforeCursor.add(new BigInteger("1"));
        continue;
      }

      if (matchPattern != null) {
        if (GlobPattern.matches(matchPattern, key.toString())) {
          returnList.add(key);
          numElements++;
        }
      } else {
        returnList.add(key);
        numElements++;
      }
      if (numElements == count) {
        break;
      }
    }

    Pair<BigInteger, List<Object>> scanResult;
    if (i >= size - 1) {
      scanResult = new ImmutablePair<>(new BigInteger("0"), returnList);
    } else {
      scanResult = new ImmutablePair<>(new BigInteger(String.valueOf(i + 1)), returnList);
    }
    return scanResult;
  }

  // TODO: When SCAN is supported, refactor to use these methods and not override executeCommand()
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
