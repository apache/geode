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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.AbstractScanExecutor;

public class HScanExecutor extends AbstractScanExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HSCAN));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion =
        (Region<ByteArrayWrapper, ByteArrayWrapper>) context.getRegionProvider().getRegion(key);
    checkDataType(key, RedisDataType.REDIS_HASH, context);
    if (keyRegion == null) {
      command.setResponse(
          Coder.getScanResponse(context.getByteBufAllocator(), new ArrayList<String>()));
      return;
    }
    byte[] cAr = commandElems.get(2);
    String cursorString = Coder.bytesToString(cAr);

    int cursor = 0;
    Pattern matchPattern = null;
    String globMatchPattern = null;
    int count = DEFUALT_COUNT;
    try {
      cursor = Integer.parseInt(cursorString);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_CURSOR));
      return;
    }
    if (cursor < 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_CURSOR));
      return;
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
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_COUNT));
        return;
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
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_COUNT));
        return;
      }
    }

    if (count < 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_COUNT));
      return;
    }

    try {
      matchPattern = convertGlobToRegex(globMatchPattern);
    } catch (PatternSyntaxException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_ILLEGAL_GLOB));
      return;
    }

    List<Object> returnList =
        getIteration(new HashSet(keyRegion.entrySet()), matchPattern, count, cursor);

    command.setResponse(Coder.getScanResponse(context.getByteBufAllocator(), returnList));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<Object> getIteration(Collection<?> list, Pattern matchPattern, int count,
      int cursor) {
    List<Object> returnList = new ArrayList<Object>();
    int size = list.size();
    int beforeCursor = 0;
    int numElements = 0;
    int i = -1;
    for (Entry<ByteArrayWrapper, ByteArrayWrapper> entry : (Collection<Entry<ByteArrayWrapper, ByteArrayWrapper>>) list) {
      ByteArrayWrapper key = entry.getKey();
      ByteArrayWrapper value = entry.getValue();
      i++;
      if (beforeCursor < cursor) {
        beforeCursor++;
        continue;
      } else if (numElements < count) {
        if (matchPattern != null) {
          if (matchPattern.matcher(key.toString()).matches()) {
            returnList.add(key);
            returnList.add(value);
            numElements++;
          }
        } else {
          returnList.add(key);
          returnList.add(value);
          numElements++;
        }
      } else {
        break;
      }
    }

    if (i == size - 1) {
      returnList.add(0, String.valueOf(0));
    } else {
      returnList.add(0, String.valueOf(i));
    }
    return returnList;
  }

}
