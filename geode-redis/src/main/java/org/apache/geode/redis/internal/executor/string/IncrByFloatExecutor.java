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
package org.apache.geode.redis.internal.executor.string;

import static org.apache.geode.redis.internal.executor.string.SetOptions.Exists.NONE;

import java.util.List;

import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisResponse;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;

public class IncrByFloatExecutor extends StringExecutor {

  private final String ERROR_VALUE_NOT_USABLE =
      "Invalid value at this key and cannot be incremented numerically";

  private final String ERROR_INCREMENT_NOT_USABLE =
      "The increment on this key must be a valid floating point numeric";

  private final String ERROR_OVERFLOW = "This incrementation cannot be performed due to overflow";

  private final int INCREMENT_INDEX = 2;

  @Override
  public RedisResponse executeCommandWithResponse(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      return RedisResponse.error(ArityDef.INCRBYFLOAT);
    }

    ByteArrayWrapper key = command.getKey();
    RedisStringCommands stringCommands = getRedisStringCommands(context);
    ByteArrayWrapper valueWrapper = stringCommands.get(key);

    /*
     * Try increment
     */

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    String doub = Coder.bytesToString(incrArray).toLowerCase();
    if (doub.contains("inf") || doub.contains("nan")) {
      return RedisResponse.error("Increment would produce NaN or infinity");
    } else if (valueWrapper != null && valueWrapper.toString().contains(" ")) {
      return RedisResponse.error(ERROR_VALUE_NOT_USABLE);
    }

    double increment;

    try {
      increment = Coder.stringToDouble(doub);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_INCREMENT_NOT_USABLE);
    }

    /*
     * Value does not exist
     */

    if (valueWrapper == null) {
      stringCommands.set(key, new ByteArrayWrapper(incrArray), null);
      return respondBulkStrings(increment);
    }

    /*
     * Value exists
     */

    String stringValue = Coder.bytesToString(valueWrapper.toBytes());

    double value;
    try {
      value = Coder.stringToDouble(stringValue);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_VALUE_NOT_USABLE);
    }

    /*
     * Check for overflow
     */
    if (value >= 0 && increment > (Double.MAX_VALUE - value)) {
      return RedisResponse.error(ERROR_OVERFLOW);
    }

    double result = value + increment;
    if (Double.isNaN(result) || Double.isInfinite(result)) {
      return RedisResponse.error(RedisConstants.ERROR_NAN_INF_INCR);
    }
    value += increment;

    stringValue = "" + value;
    SetOptions setOptions = new SetOptions(NONE, 0L, true);
    stringCommands.set(key, new ByteArrayWrapper(Coder.stringToBytes(stringValue)), setOptions);

    return respondBulkStrings(value);
  }

}
