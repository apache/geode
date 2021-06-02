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
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class ScanExecutor extends AbstractScanExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SCAN));
      return;
    }

    String cursorString = command.getStringKey();
    int cursor = 0;
    Pattern matchPattern = null;
    String globMatchString = null;
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

    if (commandElems.size() > 3) {
      try {
        byte[] bytes = commandElems.get(2);
        String tmp = Coder.bytesToString(bytes);
        if (tmp.equalsIgnoreCase("MATCH")) {
          bytes = commandElems.get(3);
          globMatchString = Coder.bytesToString(bytes);
        } else if (tmp.equalsIgnoreCase("COUNT")) {
          bytes = commandElems.get(3);
          count = Coder.bytesToInt(bytes);
        }
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_COUNT));
        return;
      }
    }

    if (commandElems.size() > 5) {
      try {
        byte[] bytes = commandElems.get(4);
        String tmp = Coder.bytesToString(bytes);
        if (tmp.equalsIgnoreCase("COUNT")) {
          bytes = commandElems.get(5);
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
      matchPattern = convertGlobToRegex(globMatchString);
    } catch (PatternSyntaxException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_ILLEGAL_GLOB));
      return;
    }

    @SuppressWarnings("unchecked")
    List<String> returnList = (List<String>) getIteration(context.getKeyRegistrar().keys(),
        matchPattern, count, cursor);

    command.setResponse(Coder.getScanResponse(context.getByteBufAllocator(), returnList));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<?> getIteration(Collection<?> list, Pattern matchPattern, int count, int cursor) {
    List<String> returnList = new ArrayList<String>();
    int size = list.size();
    int beforeCursor = 0;
    int numElements = 0;
    int i = -1;
    for (String key : (Collection<String>) list) {
      if (key.equals(GeodeRedisServer.REDIS_META_DATA_REGION)
          || key.equals(GeodeRedisServer.STRING_REGION)
          || key.equals(GeodeRedisServer.HLL_REGION)) {
        continue;
      }
      i++;
      if (beforeCursor < cursor) {
        beforeCursor++;
        continue;
      } else if (numElements < count) {
        if (matchPattern != null) {
          if (matchPattern.matcher(key).matches()) {
            returnList.add(key);
            numElements++;
          }
        } else {
          returnList.add(key);
          numElements++;
        }
      } else {
        break;
      }
    }

    if (i == size - (NUM_DEFAULT_REGIONS + 1)) {
      returnList.add(0, String.valueOf(0));
    } else {
      returnList.add(0, String.valueOf(i));
    }
    return returnList;
  }

}
