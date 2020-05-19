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

package org.apache.geode.redis.internal;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class RedisResponse {

  private final Function<ByteBufAllocator, ByteBuf> coderCallback;

  private RedisResponse(Function<ByteBufAllocator, ByteBuf> coderCallback) {
    this.coderCallback = coderCallback;
  }

  public ByteBuf encode(ByteBufAllocator allocator) {
    return coderCallback.apply(allocator);
  }

  public static RedisResponse integer(long numericValue) {
    return new RedisResponse((bba) -> Coder.getIntegerResponse(bba, numericValue));
  }

  public static RedisResponse string(String stringValue) {
    return new RedisResponse((bba) -> Coder.getSimpleStringResponse(bba, stringValue));
  }

  public static RedisResponse bulkString(Object value) {
    return new RedisResponse((bba) -> {
      try {
        return Coder.getBulkStringResponse(bba, value);
      } catch (CoderException e) {
        return Coder.getErrorResponse(bba, "Internal server error: " + e.getMessage());
      }
    });
  }

  public static RedisResponse ok() {
    return new RedisResponse((bba) -> Coder.getSimpleStringResponse(bba, "OK"));
  }

  public static RedisResponse nil() {
    return new RedisResponse(Coder::getNilResponse);
  }

  public static RedisResponse array(Collection<?> collection) {
    return new RedisResponse((bba) -> {
      try {
        return Coder.getArrayResponse(bba, collection);
      } catch (CoderException e) {
        return Coder.getErrorResponse(bba, "Internal server error: " + e.getMessage());
      }
    });
  }

  public static RedisResponse emptyArray() {
    return new RedisResponse(Coder::getEmptyArrayResponse);
  }

  public static RedisResponse error(String error) {
    return new RedisResponse((bba) -> Coder.getErrorResponse(bba, error));
  }

  public static RedisResponse wrongType(String error) {
    return new RedisResponse((bba) -> Coder.getWrongTypeResponse(bba, error));
  }

  public static RedisResponse scan(List<?> items) {
    return new RedisResponse((bba) -> Coder.getScanResponse(bba, items));
  }
}
