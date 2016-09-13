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
package org.apache.geode.redis.internal.executor.hash;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class HIncrByFloatExecutor extends HashExecutor {

  private final String ERROR_FIELD_NOT_USABLE = "The value at this field cannot be incremented numerically because it is not a float";

  private final String ERROR_INCREMENT_NOT_USABLE = "The increment on this key must be floating point numeric";

  private final int FIELD_INDEX = 2;

  private final int INCREMENT_INDEX = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HINCRBYFLOAT));
      return;
    }

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    Double increment;

    try {
      increment = Coder.bytesToDouble(incrArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INCREMENT_NOT_USABLE));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_HASH);

    byte[] byteField = commandElems.get(FIELD_INDEX);
    ByteArrayWrapper field = new ByteArrayWrapper(byteField);

    /*
     * Put incrememnt as value if field doesn't exist
     */

    ByteArrayWrapper oldValue = keyRegion.get(field);

    if (oldValue == null) {
      keyRegion.put(field, new ByteArrayWrapper(incrArray));
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), increment));
      return;
    }

    /*
     * If the field did exist then increment the field
     */
    String valueS = oldValue.toString();
    if (valueS.contains(" ")) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_FIELD_NOT_USABLE));
      return;
    }
    Double value;

    try {
      value = Coder.stringToDouble(valueS);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_FIELD_NOT_USABLE));
      return;
    }

    value += increment;
    keyRegion.put(field, new ByteArrayWrapper(Coder.doubleToBytes(value)));
    command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), value));
  }

}
