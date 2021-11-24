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
package org.apache.geode.redis.internal.commands.executor.string;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;
import static org.apache.geode.redis.internal.commands.executor.BaseSetOptions.Exists.NX;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisKeyExistsException;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.services.RegionProvider;


public abstract class AbstractMSetExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    List<byte[]> commandElems = command.getCommandArguments();

    int numElements = commandElems.size() / 2;
    List<RedisKey> keys = new ArrayList<>(numElements);
    List<byte[]> values = new ArrayList<>(numElements);

    RedisKey previousKey = null;
    for (int i = 0; i < commandElems.size(); i += 2) {
      RedisKey key = new RedisKey(commandElems.get(i));

      if (previousKey != null && key.getSlot() != previousKey.getSlot()) {
        return RedisResponse.crossSlot(ERROR_WRONG_SLOT);
      }
      keys.add(key);
      values.add(commandElems.get(i + 1));

      previousKey = key;
    }

    try {
      executeMSet(context, keys, values);
    } catch (RedisKeyExistsException e) {
      return getKeyExistsErrorResponse();
    }

    return getSuccessResponse();
  }

  protected void mset(ExecutionHandlerContext context, List<RedisKey> keys, List<byte[]> values,
      boolean ifAllKeysAbsent) {
    List<RedisKey> keysToLock = new ArrayList<>(keys.size());
    RegionProvider regionProvider = context.getRegionProvider();
    for (RedisKey key : keys) {
      regionProvider.ensureKeyIsLocal(key);
      keysToLock.add(key);
    }

    // Pass a key in so that the bucket will be locked. Since all keys are already guaranteed to be
    // in the same bucket we can use any key for this.
    context.lockedExecuteInTransaction(keysToLock.get(0), keysToLock,
        () -> mset0(regionProvider, keys, values, ifAllKeysAbsent));
  }

  private Void mset0(RegionProvider regionProvider, List<RedisKey> keys, List<byte[]> values,
      boolean ifAllKeysAbsent) {
    SetOptions options = ifAllKeysAbsent ? new SetOptions(NX, 0L, false) : null;
    for (int i = 0; i < keys.size(); i++) {
      if (!SetExecutor.set(regionProvider, keys.get(i), values.get(i), options)) {
        // rolls back transaction
        throw new RedisKeyExistsException("at least one key already exists");
      }
    }
    return null;
  }


  protected abstract RedisResponse getKeyExistsErrorResponse();

  protected abstract RedisResponse getSuccessResponse();

  protected abstract void executeMSet(ExecutionHandlerContext context, List<RedisKey> keys,
      List<byte[]> values);
}
