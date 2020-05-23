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
package org.apache.geode.redis.internal.executor.hll;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.hll.HyperLogLogPlus;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class PFAddExecutor extends HllExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.PFADD));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    Region<ByteArrayWrapper, HyperLogLogPlus> keyRegion =
        context.getRegionProvider().gethLLRegion();

    HyperLogLogPlus hll = keyRegion.get(key);

    boolean changed = false;

    if (hll == null) {
      hll = new HyperLogLogPlus(DEFAULT_HLL_DENSE);
    }

    for (int i = 2; i < commandElems.size(); i++) {
      byte[] bytes = commandElems.get(i);
      boolean offerChange = hll.offer(bytes);
      if (offerChange) {
        changed = true;
      }
    }

    keyRegion.put(key, hll);

    if (changed) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 1));
    } else {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
    }
  }

}
