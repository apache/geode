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

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public abstract class StringExecutor extends AbstractExecutor {

  protected void checkDataType(ByteArrayWrapper key, ExecutionHandlerContext context) {
    super.checkDataType(key, RedisDataType.REDIS_STRING, context);
  }

  protected void checkAndSetDataType(ByteArrayWrapper key, ExecutionHandlerContext context) {
    RedisDataType dataTypeOfOldValue = context.getRegionProvider().metaPutIfAbsent(key, RedisDataType.REDIS_STRING);
    if (!isValidDataType(dataTypeOfOldValue)) {
      throwDataTypeException(key, dataTypeOfOldValue);
    }
  }

  private boolean isValidDataType(RedisDataType dataType) {
    return isKeyUnused(dataType) || dataType == RedisDataType.REDIS_STRING;
  }

  private boolean isKeyUnused(RedisDataType dataType) {
    return dataType == null;
  }

  private void throwDataTypeException(ByteArrayWrapper key, RedisDataType dataType) {
    if (dataType == RedisDataType.REDIS_PROTECTED)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is protected");
    else
      throw new RedisDataTypeMismatchException(
        "The key name \"" + key + "\" is already used by a " + dataType.toString());
  }

}
