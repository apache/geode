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

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;

public interface RedisHashCommands {
  int hset(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToSet, boolean NX);

  int hdel(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToRemove);

  Collection<ByteArrayWrapper> hgetall(ByteArrayWrapper key);

  int hexists(ByteArrayWrapper key, ByteArrayWrapper field);

  ByteArrayWrapper hget(ByteArrayWrapper key, ByteArrayWrapper field);

  int hlen(ByteArrayWrapper key);

  int hstrlen(ByteArrayWrapper key, ByteArrayWrapper field);

  List<ByteArrayWrapper> hmget(ByteArrayWrapper key, List<ByteArrayWrapper> fields);

  Collection<ByteArrayWrapper> hvals(ByteArrayWrapper key);

  Collection<ByteArrayWrapper> hkeys(ByteArrayWrapper key);

  List<Object> hscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor);

  long hincrby(ByteArrayWrapper key, ByteArrayWrapper field, long increment);

  double hincrbyfloat(ByteArrayWrapper key, ByteArrayWrapper field, double increment);
}
