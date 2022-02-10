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


import static org.apache.geode.redis.internal.commands.executor.BaseSetOptions.Exists.NX;

import java.util.List;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class MSetNXExecutor extends AbstractMSetExecutor {

  private static final int ALL_KEYS_SET = 1;

  private static final int NO_KEY_SET = 0;

  @Immutable
  private static final SetOptions msetnxOptions = new SetOptions(NX, 0L, false);

  @Override
  protected void executeMSet(ExecutionHandlerContext context, List<RedisKey> keys,
      List<byte[]> values) {
    mset(context, keys, values, msetnxOptions);
  }

  @Override
  protected RedisResponse getKeyExistsErrorResponse() {
    return RedisResponse.integer(NO_KEY_SET);
  }

  @Override
  protected RedisResponse getSuccessResponse() {
    return RedisResponse.integer(ALL_KEYS_SET);
  }
}
