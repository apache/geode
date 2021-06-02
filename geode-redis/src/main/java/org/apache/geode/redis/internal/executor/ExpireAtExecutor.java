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

public class ExpireAtExecutor extends AbstractExecutor implements Extendable {

  private final String ERROR_TIMESTAMP_NOT_USABLE = "The timestamp specified must be numeric";

  private final int TIMESTAMP_INDEX = 2;

  private final int SET = 1;

  private final int NOT_SET = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }
    RegionProvider rC = context.getRegionProvider();
    ByteArrayWrapper wKey = command.getKey();

    byte[] timestampByteArray = commandElems.get(TIMESTAMP_INDEX);
    long timestamp;
    try {
      timestamp = Coder.bytesToLong(timestampByteArray);
    } catch (NumberFormatException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_TIMESTAMP_NOT_USABLE));
      return;
    }

    if (!timeUnitMillis()) {
      timestamp = timestamp * millisInSecond;
    }

    long currentTimeMillis = System.currentTimeMillis();

    if (timestamp <= currentTimeMillis) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
      return;
    }

    long delayMillis = timestamp - currentTimeMillis;

    boolean expirationSet = false;

    if (rC.hasExpiration(wKey)) {
      expirationSet = rC.modifyExpiration(wKey, delayMillis);
    } else {
      expirationSet = rC.setExpiration(wKey, delayMillis);
    }

    if (expirationSet) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), SET));
    } else {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
    }
  }

  protected boolean timeUnitMillis() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.EXPIREAT;
  }

}
