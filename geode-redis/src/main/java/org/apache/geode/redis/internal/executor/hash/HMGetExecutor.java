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

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;

/**
 * <pre>
 * Implementation of the HMGET command.
 * Returns values associated with the specified fields in the hash stored for a given key.
 *
 * Examples:
 *
 * redis> HSET myhash field1 "Hello"
 * (integer) 1
 * redis> HSET myhash field2 "World"
 * (integer) 1
 * redis> HMGET myhash field1 field2 nofield
 * 1) "Hello"
 * 2) "World"
 * 3) (nil)
 *
 * </pre>
 */
public class HMGetExecutor extends HashExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    ByteArrayWrapper key = command.getKey();

    RedisHash map = getMap(context, key);

    checkDataType(key, RedisDataType.REDIS_HASH, context);

    ArrayList<ByteArrayWrapper> fields = new ArrayList<ByteArrayWrapper>();
    for (int i = 2; i < commandElems.size(); i++) {
      byte[] fieldArray = commandElems.get(i);
      ByteArrayWrapper field = new ByteArrayWrapper(fieldArray);
      fields.add(field);
    }

    ArrayList<ByteArrayWrapper> values = new ArrayList<ByteArrayWrapper>();

    /*
     * This is done to preserve order in the output
     */
    for (ByteArrayWrapper field : fields) {
      values.add(map.get(field));
    }

    respondBulkStrings(command, context, values);
  }
}
