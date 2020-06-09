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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_ILLEGAL_GLOB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ScanExecutor extends AbstractScanExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      return RedisResponse.error(ArityDef.SCAN);
    }

    String cursorString = command.getStringKey();
    int cursor = 0;
    Pattern matchPattern = null;
    String globMatchString = null;
    int count = DEFAULT_COUNT;
    try {
      cursor = Integer.parseInt(cursorString);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_CURSOR);
    }
    if (cursor < 0) {
      return RedisResponse.error(ERROR_CURSOR);
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
        return RedisResponse.error(ERROR_COUNT);
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
        return RedisResponse.error(ERROR_COUNT);
      }
    }

    if (count < 0) {
      return RedisResponse.error(ERROR_COUNT);
    }

    try {
      matchPattern = convertGlobToRegex(globMatchString);
    } catch (PatternSyntaxException e) {
      return RedisResponse.error(ERROR_ILLEGAL_GLOB);
    }

    @SuppressWarnings("unchecked")
    List<String> returnList = (List<String>) getIteration(getDataRegion(context).keySet(),
        matchPattern, count, cursor);

    return RedisResponse.scan(returnList);
  }

  @SuppressWarnings("unchecked")
  private List<?> getIteration(Collection<?> list, Pattern matchPattern, int count, int cursor) {
    List<String> returnList = new ArrayList<String>();
    int size = list.size();
    int beforeCursor = 0;
    int numElements = 0;
    int i = -1;
    for (String key : (Collection<String>) list) {
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

    if (i >= size - 1) {
      returnList.add(0, String.valueOf(0));
    } else {
      returnList.add(0, String.valueOf(i));
    }
    return returnList;
  }

}
