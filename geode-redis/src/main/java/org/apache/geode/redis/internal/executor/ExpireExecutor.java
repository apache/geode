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
package org.apache.geode.redis.internal.executor;

import java.util.List;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Extendable;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RegionProvider;

public class ExpireExecutor extends AbstractExecutor implements Extendable {

  private final String ERROR_SECONDS_NOT_USABLE = "The number of seconds specified must be numeric";

  private final int SECONDS_INDEX = 2;

  private final int SET = 1;

  private final int NOT_SET = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }
    ByteArrayWrapper wKey = command.getKey();
    RegionProvider rC = context.getRegionProvider();
    byte[] delayByteArray = commandElems.get(SECONDS_INDEX);
    long delay;
    try {
      delay = Coder.bytesToLong(delayByteArray);
    } catch (NumberFormatException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_SECONDS_NOT_USABLE));
      return;
    }

    if (delay <= 0) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
      return;
    }

    // If time unit given is not in millis convert to millis
    if (!timeUnitMillis())
      delay = delay * millisInSecond;

    boolean expirationSet = false;

    if (rC.hasExpiration(wKey))
      expirationSet = rC.modifyExpiration(wKey, delay);
    else
      expirationSet = rC.setExpiration(wKey, delay);


    if (expirationSet)
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), SET));
    else
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
  }

  /*
   * Overridden by PExpire
   */
  protected boolean timeUnitMillis() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.EXPIRE;
  }

}
