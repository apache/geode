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
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.CoderException;

public class RedisResponse {

  private final Function<ByteBuf, ByteBuf> coderCallback;

  private Runnable afterWriteCallback;

  private RedisResponse(Function<ByteBuf, ByteBuf> coderCallback) {
    this.coderCallback = coderCallback;
  }

  public boolean hasAfterWriteCallback() {
    return afterWriteCallback != null;
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

  public static RedisResponse integer(byte[] numericValue) {
    return new RedisResponse((buffer) -> Coder.getIntegerResponse(buffer, numericValue));
  }

  @Immutable
  private static final RedisResponse ZERO = new RedisResponse(Coder::getZeroIntResponse);

  @Immutable
  private static final RedisResponse ONE = new RedisResponse(Coder::getOneIntResponse);

  public static RedisResponse integer(boolean exists) {
    return exists ? ONE : ZERO;
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
        return Coder.getStringResponse(buffer, value, true);
      } catch (CoderException e) {
        return Coder.getInternalErrorResponse(buffer, e.getMessage());
      }
    });
  }

  @Immutable
  private static final RedisResponse OK = new RedisResponse(Coder::getOKResponse);

  public static RedisResponse ok() {
    return OK;
  }

  @Immutable
  private static final RedisResponse NIL = new RedisResponse(Coder::getNilResponse);

  public static RedisResponse nil() {
    return NIL;
  }

  @Immutable
  private static final RedisResponse EMPTY = new RedisResponse(Coder::getEmptyResponse);

  public static RedisResponse flattenedArray(Collection<Collection<?>> nestedCollection) {
    if (nestedCollection.isEmpty()) {
      return EMPTY;
    }
    return new RedisResponse((buffer) -> {
      try {
        return Coder.getFlattenedArrayResponse(buffer, nestedCollection);
      } catch (CoderException e) {
        return Coder.getInternalErrorResponse(buffer, e.getMessage());
      }
    });
  }

  public static RedisResponse array(Collection<?> collection, boolean useBulkStrings) {
    if (collection == null || collection.isEmpty()) {
      return emptyArray();
    }
    return new RedisResponse((buffer) -> {
      try {
        return Coder.getArrayResponse(buffer, collection, useBulkStrings);
      } catch (CoderException e) {
        return Coder.getInternalErrorResponse(buffer, e.getMessage());
      }
    });
  }

  public static RedisResponse array(Object... items) {
    return array(Arrays.asList(items), true);
  }

  @Immutable
  private static final RedisResponse EMPTY_ARRAY = new RedisResponse(Coder::getEmptyArrayResponse);

  public static RedisResponse emptyArray() {
    return EMPTY_ARRAY;
  }

  @Immutable
  private static final RedisResponse EMPTY_STRING =
      new RedisResponse(Coder::getEmptyStringResponse);

  public static RedisResponse emptyString() {
    return EMPTY_STRING;
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

  public static RedisResponse crossSlot(String error) {
    return new RedisResponse((bba) -> Coder.getCrossSlotResponse(bba, error));
  }

  public static RedisResponse busykey(String error) {
    return new RedisResponse((bba) -> Coder.getBusyKeyResponse(bba, error));
  }

  public static RedisResponse wrongpass(String error) {
    return new RedisResponse((bba) -> Coder.getWrongpassResponse(bba, error));
  }

  public static RedisResponse wrongType(String error) {
    return new RedisResponse((buffer) -> Coder.getWrongTypeResponse(buffer, error));
  }

  public static RedisResponse noAuth(String error) {
    return new RedisResponse((buffer) -> Coder.getNoAuthResponse(buffer, error));
  }

  public static RedisResponse scan(int cursor, List<?> scanResult) {
    return new RedisResponse((buffer) -> Coder.getScanResponse(buffer, cursor, scanResult));
  }


  @Immutable
  private static final RedisResponse EMPTY_SCAN = scan(0, Collections.emptyList());

  public static RedisResponse emptyScan() {
    return EMPTY_SCAN;
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
