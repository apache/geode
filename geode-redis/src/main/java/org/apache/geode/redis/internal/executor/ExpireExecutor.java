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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;

import java.util.List;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Extendable;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RegionProvider;

public class ExpireExecutor extends AbstractExecutor implements Extendable {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    int NOT_SET = 0;
    int SET = 1;
    int SECONDS_INDEX = 2;

    if (commandElems.size() != 3) {
      command.setResponse(
          Coder.getErrorResponse(
              context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    RegionProvider regionProvider = context.getRegionProvider();
    byte[] delayByteArray = commandElems.get(SECONDS_INDEX);
    long delay;
    try {
      delay = Coder.bytesToLong(delayByteArray);
    } catch (NumberFormatException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INTEGER));
      return;
    }

    if (delay <= 0) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
      return;
    }

    // If time unit given is not in millis convert to millis
    if (!timeUnitMillis()) {
      delay = delay * millisInSecond;
    }

    boolean expirationSucessfullySet;

    if (regionProvider.hasExpiration(key)) {
      expirationSucessfullySet = regionProvider.modifyExpiration(key, delay);
    } else {
      expirationSucessfullySet = regionProvider.setExpiration(key, delay);
    }

    if (expirationSucessfullySet) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), SET));
    } else {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
    }
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
