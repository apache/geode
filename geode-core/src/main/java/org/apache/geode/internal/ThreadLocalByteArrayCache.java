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
package org.apache.geode.internal;

/**
 * Provides a simple thread local cache of a single byte array.
 */
public class ThreadLocalByteArrayCache {
  private final ThreadLocal<byte[]> cache = new ThreadLocal<byte[]>();

  /**
   * Returns a byte array whose length it at least minimumLength.
   * NOTE: the same thread can not safely call this method again
   * until it has finished using the byte array returned by a previous call.
   *
   * @param minimumLength the minimum length of the byte array
   * @return a byte array, owned by this thread, whose length is at least minimumLength.
   */
  public byte[] get(int minimumLength) {
    byte[] result = cache.get();
    if (result == null || result.length < minimumLength) {
      result = new byte[minimumLength];
      cache.set(result);
    }
    return result;
  }
}
