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
package org.apache.geode.redis.internal.executor.hash;

import java.util.List;
import java.util.Map;

import org.apache.geode.cache.TimeoutException;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;

/**
 * <pre>
 * Implementation of HINCRBYFLOAT Redis command.
 * The purpose is to increment the specified field of a hash for a given key.
 *  The value is floating number (represented as a double), by the specified increment.
 *
 * Examples:
 *
 * redis> HSET mykey field 10.50
 * (integer) 1
 * redis> HINCRBYFLOAT mykey field 0.1
 * "10.6"
 * redis> HINCRBYFLOAT mykey field -5
 * "5.6"
 * redis> HSET mykey field 5.0e3
 * (integer) 0
 * redis> HINCRBYFLOAT mykey field 2.0e2
 * "5200"
 *
 *
 * </pre>
 */
public class HIncrByFloatExecutor extends HashExecutor {

  private final String ERROR_FIELD_NOT_USABLE =
      "The value at this field cannot be incremented numerically because it is not a float";

  private final String ERROR_INCREMENT_NOT_USABLE =
      "The increment on this key must be floating point numeric";

  private final int FIELD_INDEX = 2;

  private final int INCREMENT_INDEX = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    double increment;

    try {
      increment = Coder.bytesToDouble(incrArray);
    } catch (NumberFormatException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INCREMENT_NOT_USABLE));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    double value;

    try (AutoCloseableLock regionLock = withRegionLock(context, key)) {
      Map<ByteArrayWrapper, ByteArrayWrapper> map = getMap(context, key);

      byte[] byteField = commandElems.get(FIELD_INDEX);
      ByteArrayWrapper field = new ByteArrayWrapper(byteField);

      /*
       * Put increment as value if field doesn't exist
       */

      ByteArrayWrapper oldValue = map.get(field);

      if (oldValue == null) {
        map.put(field, new ByteArrayWrapper(incrArray));

        this.saveMap(map, context, key);

        respondBulkStrings(command, context, increment);
        return;
      }

      /*
       * If the field did exist then increment the field
       */
      String valueS = oldValue.toString();
      if (valueS.contains(" ")) {
        command.setResponse(
            Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_FIELD_NOT_USABLE));
        return;
      }

      try {
        value = Coder.stringToDouble(valueS);
      } catch (NumberFormatException e) {
        command.setResponse(
            Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_FIELD_NOT_USABLE));
        return;
      }

      value += increment;
      map.put(field, new ByteArrayWrapper(Coder.doubleToBytes(value)));

      this.saveMap(map, context, key);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), "Thread interrupted."));
      return;
    } catch (TimeoutException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          "Timeout acquiring lock. Please try again."));
      return;
    }
    respondBulkStrings(command, context, value);
  }

}
