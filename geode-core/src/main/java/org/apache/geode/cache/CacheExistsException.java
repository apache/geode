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


/**
 * Thrown when attempting to create a {@link Cache} if one already exists.
 *
 *
 * @see CacheFactory#create
 * @since GemFire 3.0
 */
public class CacheExistsException extends CacheException {
  private static final long serialVersionUID = 4090002289325418100L;

  /** The <code>Cache</code> that already exists */
  private final transient Cache cache;

  /////////////////////// Constructors ///////////////////////

  public CacheExistsException() {
    cache = null;
  }

  /**
   * Constructs an instance of <code>CacheExistsException</code> with the specified detail message.
   *
   * @param msg the detail message
   */
  public CacheExistsException(Cache cache, String msg) {
    super(msg);
    this.cache = cache;
  }

  /**
   * Constructs an instance of <code>CacheExistsException</code> with the specified detail message
   * and cause.
   *
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CacheExistsException(Cache cache, String msg, Throwable cause) {
    super(msg, cause);
    this.cache = cache;
  }

  /////////////////////// Instance Methods ///////////////////////

  /**
   * Returns the <code>Cache</code> that already exists.
   *
   * @since GemFire 4.0
   */
  public Cache getCache() {
    return cache;
  }
}
