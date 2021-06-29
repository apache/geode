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

package org.apache.geode.redis.internal.executor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.CoderException;

public class RedisResponse {

  private final Function<ByteBuf, ByteBuf> coderCallback;

  private Runnable afterWriteCallback;

  private RedisResponse(Function<ByteBuf, ByteBuf> coderCallback) {
    this.coderCallback = coderCallback;
  }

  public void setAfterWriteCallback(Runnable callback) {
    afterWriteCallback = callback;
  }

  public void afterWrite() {
    if (afterWriteCallback != null) {
      afterWriteCallback.run();
    }
  }

  public ByteBuf encode(ByteBufAllocator allocator) {
    return coderCallback.apply(allocator.buffer());
  }

  public static RedisResponse integer(long numericValue) {
    return new RedisResponse((buffer) -> Coder.getIntegerResponse(buffer, numericValue));
  }

  public static RedisResponse integer(boolean exists) {
    return new RedisResponse((buffer) -> Coder.getIntegerResponse(buffer, exists ? 1 : 0));
  }

  public static RedisResponse string(String stringValue) {
    return new RedisResponse((buffer) -> Coder.getSimpleStringResponse(buffer, stringValue));
  }

  public static RedisResponse string(byte[] byteArray) {
    return new RedisResponse((buffer) -> Coder.getSimpleStringResponse(buffer, byteArray));
  }

  public static RedisResponse bulkString(Object value) {
    return new RedisResponse((buffer) -> {
      try {
        return Coder.getBulkStringResponse(buffer, value);
      } catch (CoderException e) {
        return Coder.getErrorResponse(buffer, "Internal server error: " + e.getMessage());
      }
    });
  }

  public static RedisResponse ok() {
    return new RedisResponse(Coder::getOKResponse);
  }

  public static RedisResponse nil() {
    return new RedisResponse(Coder::getNilResponse);
  }

  public static RedisResponse flattenedArray(Collection<Collection<?>> nestedCollection) {
    return new RedisResponse((buffer) -> {
      try {
        return Coder.getFlattenedArrayResponse(buffer, nestedCollection);
      } catch (CoderException e) {
        return Coder.getErrorResponse(buffer, "Internal server error: " + e.getMessage());
      }
    });
  }

  public static RedisResponse array(Collection<?> collection) {
    return new RedisResponse((buffer) -> {
      try {
        return Coder.getArrayResponse(buffer, collection);
      } catch (CoderException e) {
        return Coder.getErrorResponse(buffer, "Internal server error: " + e.getMessage());
      }
    });
  }

  public static RedisResponse emptyArray() {
    return new RedisResponse(Coder::getEmptyArrayResponse);
  }

  public static RedisResponse emptyString() {
    return new RedisResponse(Coder::getEmptyStringResponse);
  }

  public static RedisResponse error(String error) {
    return new RedisResponse((buffer) -> Coder.getErrorResponse(buffer, error));
  }

  public static RedisResponse moved(String error) {
    return new RedisResponse((buffer) -> Coder.getMovedResponse(buffer, error));
  }

  public static RedisResponse oom(String error) {
    return new RedisResponse((bba) -> Coder.getOOMResponse(bba, error));
  }

  public static RedisResponse busykey(String error) {
    return new RedisResponse((bba) -> Coder.getBusyKeyResponse(bba, error));
  }

  public static RedisResponse customError(String error) {
    return new RedisResponse((buffer) -> Coder.getCustomErrorResponse(buffer, error));
  }

  public static RedisResponse wrongType(String error) {
    return new RedisResponse((buffer) -> Coder.getWrongTypeResponse(buffer, error));
  }

  public static RedisResponse scan(BigInteger cursor, List<?> scanResult) {
    return new RedisResponse((buffer) -> Coder.getScanResponse(buffer, cursor, scanResult));
  }

  public static RedisResponse emptyScan() {
    return scan(new BigInteger("0"), new ArrayList<>());
  }

  /**
   * Be aware that this implementation will create extra garbage since it allocates from the heap.
   */
  public String toString() {
    return encode(new UnpooledByteBufAllocator(false)).toString(Charset.defaultCharset());
  }

  public static RedisResponse bigDecimal(BigDecimal numericValue) {
    return new RedisResponse((buffer) -> Coder.getBigDecimalResponse(buffer, numericValue));
  }

}
