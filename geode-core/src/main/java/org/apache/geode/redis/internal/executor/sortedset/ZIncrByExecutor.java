/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.redis.internal.executor.sortedset;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.DoubleWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class ZIncrByExecutor extends SortedSetExecutor {

  private final String ERROR_NOT_NUMERIC = "The number provided is not numeric";
  private final String ERROR_NAN = "This increment is illegal because it would result in a NaN";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() != 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZINCRBY));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);

    ByteArrayWrapper member = new ByteArrayWrapper(commandElems.get(3));

    double incr;

    try {
      byte[] incrArray = commandElems.get(2);
      incr = Coder.bytesToDouble(incrArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    DoubleWrapper score = keyRegion.get(member);

    if (score == null) {
      keyRegion.put(member, new DoubleWrapper(incr));
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), incr));
      return;
    }
    double result = score.score + incr;
    if (Double.isNaN(result)) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NAN));
      return;
    }
    score.score = result;
    keyRegion.put(member, score);
    command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), score.score));
  }

}
