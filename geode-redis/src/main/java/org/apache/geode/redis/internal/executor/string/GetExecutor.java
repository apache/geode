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

import static org.apache.geode.redis.internal.RedisCommandType.GET;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.CommandFunction;

public class GetExecutor extends StringExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {

    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_STRING, context);

    Region<ByteArrayWrapper, RedisData> region =
        context.getRegionProvider().getDataRegion();

    ByteArrayWrapper valueAsBytes = null;

    Object value = CommandFunction.execute(GET, key, null, region);

    if (value instanceof ByteArrayWrapper) {
      valueAsBytes = (ByteArrayWrapper) value;
    } else if (value instanceof RedisString) {
      valueAsBytes = ((RedisString) value).getValue();
    }

    respondBulkStrings(command, context, valueAsBytes);
  }

}
