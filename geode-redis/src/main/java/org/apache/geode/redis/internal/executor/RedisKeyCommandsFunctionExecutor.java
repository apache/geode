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

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisData;

public class RedisKeyCommandsFunctionExecutor implements RedisKeyCommands {
  private Region<ByteArrayWrapper, RedisData> region;

  public RedisKeyCommandsFunctionExecutor(
      Region<ByteArrayWrapper, RedisData> region) {
    this.region = region;
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    return (boolean) CommandFunction.execute(RedisCommandType.DEL, key, new Object[] {}, region);
  }

  @Override
  public boolean exists(ByteArrayWrapper key) {
    return (boolean) CommandFunction.execute(RedisCommandType.EXISTS, key, new Object[] {}, region);
  }
}
