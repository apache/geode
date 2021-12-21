/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.serialization;

import java.lang.ref.SoftReference;

/**
 * Provides a simple thread local cache of a single byte array.
 */
class ThreadLocalByteArrayCache {
  private final ThreadLocal<SoftReference<byte[]>> cache = new ThreadLocal<>();
  private final int maximumArraySize;

  /**
   * @param maximumArraySize byte arrays larger than this size will not be cached
   */
  public ThreadLocalByteArrayCache(int maximumArraySize) {
    this.maximumArraySize = maximumArraySize;
  }

  /**
   * Returns a byte array whose length it at least minimumLength.
   * NOTE: the same thread can not safely call this method again
   * until it has finished using the byte array returned by a previous call.
   * If minimumLength is larger than this cache's maximumArraySize
   * then the returned byte array will not be cached.
   *
   * @param minimumLength the minimum length of the byte array
   * @return a byte array, owned by this thread, whose length is at least minimumLength.
   */
  public byte[] get(int minimumLength) {
    SoftReference<byte[]> reference = cache.get();
    byte[] result = (reference != null) ? reference.get() : null;
    if (result == null || result.length < minimumLength) {
      result = new byte[minimumLength];
      if (minimumLength <= maximumArraySize) {
        cache.set(new SoftReference<>(result));
      }
    }
    return result;
  }
}
