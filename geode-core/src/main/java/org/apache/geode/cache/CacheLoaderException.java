/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.cache;

/** Thrown from the {@link CacheLoader#load} method indicating that an error
 * occurred when a CacheLoader was attempting to load a value. This
 * exception is propagated back to the caller of <code>Region.get</code>.
 *
 *
 *
 * @see org.apache.geode.cache.Region#get(Object)
 * @see CacheLoader#load
 * @since GemFire 3.0
 */
public class CacheLoaderException extends OperationAbortedException {
private static final long serialVersionUID = -3383072059406642140L;
  
  /**
   * Creates a new instance of <code>CacheLoaderException</code>.
   */
  public CacheLoaderException() {
  }
  
  
  /**
   * Constructs an instance of <code>CacheLoaderException</code> with the specified detail message.
   * @param msg the detail message
   */
  public CacheLoaderException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>CacheLoaderException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CacheLoaderException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>CacheLoaderException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public CacheLoaderException(Throwable cause) {
    super(cause);
  }
}
