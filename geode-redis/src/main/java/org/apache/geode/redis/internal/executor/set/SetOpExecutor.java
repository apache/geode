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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Extendable;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RegionProvider;

public abstract class SetOpExecutor extends SetExecutor implements Extendable {

  @SuppressWarnings("unchecked")
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    int setsStartIndex = isStorage() ? 2 : 1;
    if (commandElems.size() < setsStartIndex + 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }
    RegionProvider rC = context.getRegionProvider();
    ByteArrayWrapper destination = null;
    if (isStorage()) {
      destination = command.getKey();
    }

    ByteArrayWrapper firstSetKey = new ByteArrayWrapper(commandElems.get(setsStartIndex++));
    if (!isStorage()) {
      checkDataType(firstSetKey, RedisDataType.REDIS_SET, context);
    }
    Region<ByteArrayWrapper, Boolean> region =
        (Region<ByteArrayWrapper, Boolean>) rC.getRegion(firstSetKey);
    Set<ByteArrayWrapper> firstSet = null;
    if (region != null) {
      firstSet = new HashSet<ByteArrayWrapper>(region.keySet());
    }
    ArrayList<Set<ByteArrayWrapper>> setList = new ArrayList<Set<ByteArrayWrapper>>();
    for (int i = setsStartIndex; i < commandElems.size(); i++) {
      ByteArrayWrapper key = new ByteArrayWrapper(commandElems.get(i));
      checkDataType(key, RedisDataType.REDIS_SET, context);
      region = (Region<ByteArrayWrapper, Boolean>) rC.getRegion(key);
      if (region != null) {
        setList.add(region.keySet());
      } else if (this instanceof SInterExecutor) {
        setList.add(null);
      }
    }
    if (setList.isEmpty()) {
      if (isStorage()) {
        command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
        context.getRegionProvider().removeKey(destination);
      } else {
        respondBulkStrings(command, context, firstSet);
      }
      return;
    }

    Set<ByteArrayWrapper> resultSet = setOp(firstSet, setList);
    if (isStorage()) {
      Region<ByteArrayWrapper, Boolean> newRegion = null; // (Region<ByteArrayWrapper, Boolean>)
                                                          // rC.getRegion(destination);
      rC.removeKey(destination);
      if (resultSet != null) {
        Map<ByteArrayWrapper, Boolean> map = new HashMap<ByteArrayWrapper, Boolean>();
        for (ByteArrayWrapper entry : resultSet) {
          map.put(entry, Boolean.TRUE);
        }
        if (!map.isEmpty()) {
          newRegion = (Region<ByteArrayWrapper, Boolean>) rC.getOrCreateRegion(destination,
              RedisDataType.REDIS_SET, context);
          newRegion.putAll(map);
        }
        command
            .setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), resultSet.size()));
      } else {
        command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      }
    } else {
      if (resultSet == null || resultSet.isEmpty()) {
        command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      } else {
        respondBulkStrings(command, context, resultSet);
      }
    }
  }

  protected abstract boolean isStorage();

  protected abstract Set<ByteArrayWrapper> setOp(Set<ByteArrayWrapper> firstSet,
      List<Set<ByteArrayWrapper>> setList);
}
