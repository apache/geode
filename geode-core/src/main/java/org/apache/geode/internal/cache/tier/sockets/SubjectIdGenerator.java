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
package org.apache.geode.internal.cache.tier.sockets;

import java.util.OptionalLong;

/**
 * Generates sequences of unique IDs. Each call to {@link #generateId()} returns a different ID
 * in some sequence, until the generator has generated every unique ID in that sequence. When the
 * generator exhausts all IDs in a sequence, it returns an empty {@code OptionalLong} to report
 * ID exhaustion, and prepares a subsequence sequence.
 * <p>
 * The relationship between successive sequences is defined by the implementation. When a typical
 * implementation exhausts a sequence, it will attempt to produce subsequent IDs in a different
 * sequence.
 * </p>
 */
public interface SubjectIdGenerator {
  /**
   * Returns the next unique ID in the current sequence. If the generator has already generated
   * every ID in the current sequence, it returns an empty {@code OptionalLong} to report ID
   * exhaustion. Subsequent calls return IDs from a new sequence.
   * <p>
   * If {@code generateId()} returns an empty {@code OptionalLong}, the caller should handle the
   * ID exhaustion before requesting the next ID. For example, the caller might invalidate all
   * uses of previously generated IDs.
   * </p>
   *
   * @return an {@code OptionalLong} describing the next unique ID in the current sequence, or an
   *         empty {@code OptionalLong} if there are no further unique IDs in the sequence.
   */
  OptionalLong generateId();
}
