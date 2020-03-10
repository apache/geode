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

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.hll.CardinalityMergeException;
import org.apache.geode.internal.hll.HyperLogLogPlus;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;

public class PFCountExecutor extends HllExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.PFCOUNT));
      return;
    }

    Region<ByteArrayWrapper, HyperLogLogPlus> keyRegion =
        context.getRegionProvider().gethLLRegion();

    List<HyperLogLogPlus> hlls = new ArrayList<HyperLogLogPlus>();

    for (int i = 1; i < commandElems.size(); i++) {
      ByteArrayWrapper k = new ByteArrayWrapper(commandElems.get(i));
      checkDataType(k, RedisDataType.REDIS_HLL, context);
      HyperLogLogPlus h = keyRegion.get(k);
      if (h != null) {
        hlls.add(h);
      }
    }
    if (hlls.isEmpty()) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      return;
    }

    HyperLogLogPlus tmp = hlls.remove(0);
    HyperLogLogPlus[] estimators = hlls.toArray(new HyperLogLogPlus[hlls.size()]);
    try {
      tmp = (HyperLogLogPlus) tmp.merge(estimators);
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
    long cardinality = tmp.cardinality();
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), cardinality));
  }

}
