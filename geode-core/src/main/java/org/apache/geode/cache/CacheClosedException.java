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
package org.apache.geode.cache;

import org.apache.geode.CancelException;

/**
 * Indicates that the caching system has been closed. Can be thrown from almost any method related
 * to regions or the {@code Cache} after the cache has been closed.
 *
 * @see Cache
 * @since GemFire 3.0
 */
public class CacheClosedException extends CancelException {
  private static final long serialVersionUID = -6479561694497811262L;

  /**
   * Constructs a new {@code CacheClosedException}.
   */
  public CacheClosedException() {
    super();
  }

  /**
   * Constructs a new {@code CacheClosedException} with a message string.
   *
   * @param msg a message string
   */
  public CacheClosedException(String msg) {
    super(msg);
  }

  /**
   * Constructs a new {@code CacheClosedException} with a message string and a cause.
   *
   * @param msg the message string
   * @param cause a causal Throwable
   */
  public CacheClosedException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructs a new {@code CacheClosedException} with a cause.
   *
   * @param cause a causal Throwable
   */
  public CacheClosedException(Throwable cause) {
    super(cause);
  }
}
