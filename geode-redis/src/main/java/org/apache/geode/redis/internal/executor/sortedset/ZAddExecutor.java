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
package org.apache.geode.redis.internal.executor.sortedset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.DoubleWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;

public class ZAddExecutor extends SortedSetExecutor {

  private final String ERROR_NOT_NUMERICAL = "The inteded score is not a float";


  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4 || commandElems.size() % 2 == 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZADD));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    int numberOfAdds = 0;

    if (commandElems.size() > 4) {
      Map<ByteArrayWrapper, DoubleWrapper> map = new HashMap<ByteArrayWrapper, DoubleWrapper>();
      for (int i = 2; i < commandElems.size(); i++) {
        byte[] scoreArray = commandElems.get(i++);
        byte[] memberArray = commandElems.get(i);

        Double score;
        try {
          score = Coder.bytesToDouble(scoreArray);
        } catch (NumberFormatException e) {
          command.setResponse(
              Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERICAL));
          return;
        }

        map.put(new ByteArrayWrapper(memberArray), new DoubleWrapper(score));
        numberOfAdds++;
      }
      Region<ByteArrayWrapper, DoubleWrapper> keyRegion =
          getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);
      keyRegion.putAll(map);
    } else {
      byte[] scoreArray = commandElems.get(2);
      byte[] memberArray = commandElems.get(3);
      Double score;
      try {
        score = Coder.bytesToDouble(scoreArray);
      } catch (NumberFormatException e) {
        command.setResponse(
            Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERICAL));
        return;
      }
      Region<ByteArrayWrapper, DoubleWrapper> keyRegion =
          getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);
      Object oldVal = keyRegion.put(new ByteArrayWrapper(memberArray), new DoubleWrapper(score));

      if (oldVal == null) {
        numberOfAdds = 1;
      }
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numberOfAdds));
  }

}
