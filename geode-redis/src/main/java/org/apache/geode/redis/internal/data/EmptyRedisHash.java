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
 *
 */

package org.apache.geode.redis.internal.data;

import static java.util.Collections.emptyList;

import java.util.List;

import org.apache.geode.cache.Region;

public class EmptyRedisHash extends RedisHash {
  public EmptyRedisHash() {
    super(emptyList());
  }

  @Override
  public int hset(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToSet, boolean nx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long hincrby(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      ByteArrayWrapper field, long increment)
      throws NumberFormatException, ArithmeticException {
    throw new UnsupportedOperationException();
  }

  @Override
  public double hincrbyfloat(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      ByteArrayWrapper field, double increment) throws NumberFormatException {
    throw new UnsupportedOperationException();
  }
}
