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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RegionProvider;

public class SMoveExecutor extends SetExecutor {

  private final int MOVED = 1;

  private final int NOT_MOVED = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SMOVE));
      return;
    }

    ByteArrayWrapper source = command.getKey();
    ByteArrayWrapper destination = new ByteArrayWrapper(commandElems.get(2));
    ByteArrayWrapper mem = new ByteArrayWrapper(commandElems.get(3));

    checkDataType(source, RedisDataType.REDIS_SET, context);
    checkDataType(destination, RedisDataType.REDIS_SET, context);

    Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region = getRegion(context);

    Set<ByteArrayWrapper> sourceSet = region.get(source);

    if (sourceSet == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_MOVED));
      return;
    }

    sourceSet = new HashSet<ByteArrayWrapper>(sourceSet); // copy to support transactions;
    boolean removed = sourceSet.remove(mem);


    Set<ByteArrayWrapper> destinationSet = region.get(destination);

    if (destinationSet == null)
      destinationSet = new HashSet<ByteArrayWrapper>();
    else
      destinationSet = new HashSet<ByteArrayWrapper>(destinationSet); // copy to support
                                                                      // transactions

    destinationSet.add(mem);

    RegionProvider rc = context.getRegionProvider();

    region.put(destination, destinationSet);
    rc.metaPut(destination, RedisDataType.REDIS_SET);

    region.put(source, sourceSet);
    rc.metaPut(source, RedisDataType.REDIS_SET);

    if (!removed) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_MOVED));
    } else {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), MOVED));
    }

  }

}
