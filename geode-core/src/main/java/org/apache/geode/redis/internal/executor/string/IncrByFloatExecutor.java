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
package org.apache.geode.redis.internal.executor.string;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class IncrByFloatExecutor extends StringExecutor {

  private final String ERROR_VALUE_NOT_USABLE = "Invalid value at this key and cannot be incremented numerically";

  private final String ERROR_INCREMENT_NOT_USABLE = "The increment on this key must be a valid floating point numeric";

  private final String ERROR_OVERFLOW = "This incrementation cannot be performed due to overflow";

  private final int INCREMENT_INDEX = 2;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();
    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.INCRBYFLOAT));
      return;
    }
    
    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper valueWrapper = r.get(key);

    /*
     * Try increment
     */

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    String doub = Coder.bytesToString(incrArray).toLowerCase();
    if (doub.contains("inf") || doub.contains("nan")) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), "Increment would produce NaN or infinity"));
      return;
    } else if (valueWrapper != null && valueWrapper.toString().contains(" ")) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_VALUE_NOT_USABLE));
      return;
    }
    
    
    Double increment;

    try {
      increment = Coder.stringToDouble(doub);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INCREMENT_NOT_USABLE));
      return;
    }

    /*
     * Value does not exist
     */

    if (valueWrapper == null) {
      r.put(key, new ByteArrayWrapper(incrArray));
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), increment));
      return;
    }

    /*
     * Value exists
     */

    String stringValue = Coder.bytesToString(valueWrapper.toBytes());

    Double value;
    try {
      value = Coder.stringToDouble(stringValue);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_VALUE_NOT_USABLE));
      return;
    }

    /*
     * Check for overflow
     */
    if (value >= 0 && increment > (Double.MAX_VALUE - value)) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_OVERFLOW));
      return;
    }

    double result = value + increment;
    if (Double.isNaN(result) || Double.isInfinite(result)) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_NAN_INF_INCR));
      return;
    }
    value += increment;

    stringValue = "" + value;
    r.put(key, new ByteArrayWrapper(Coder.stringToBytes(stringValue)));

    command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), value));
  }

}
