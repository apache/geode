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

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Extendable;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public class SetEXExecutor extends StringExecutor implements Extendable {

  private final String ERROR_SECONDS_NOT_A_NUMBER =
      "The expiration argument provided was not a number";

  private final String ERROR_SECONDS_NOT_LEGAL = "The expiration argument must be greater than 0";

  private final String SUCCESS = "OK";

  private final int VALUE_INDEX = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] value = commandElems.get(VALUE_INDEX);

    byte[] expirationArray = commandElems.get(2);
    long expiration;
    try {
      expiration = Coder.bytesToLong(expirationArray);
    } catch (NumberFormatException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_SECONDS_NOT_A_NUMBER));
      return;
    }

    if (expiration <= 0) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_SECONDS_NOT_LEGAL));
      return;
    }

    if (!timeUnitMillis()) {
      expiration *= AbstractExecutor.millisInSecond;
    }

    checkAndSetDataType(key, context);
    r.put(key, new ByteArrayWrapper(value));

    context.getRegionProvider().setExpiration(key, expiration);

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));

  }

  protected boolean timeUnitMillis() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.SETEX;
  }

}
