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

package com.gemstone.gemfire.cache;

/**
 * An exception thrown by a <code>CacheWriter</code> method. This exception
 * is propagated back to the caller that initiated modification of the
 * cache, even if the caller is not in the same cache VM.
 *
 *
 * @see CacheWriter
 * @see com.gemstone.gemfire.cache.Region#put(Object, Object)
 * @see com.gemstone.gemfire.cache.Region#destroy(Object)
 * @see com.gemstone.gemfire.cache.Region#create(Object, Object)
 * @see com.gemstone.gemfire.cache.Region#destroyRegion()
 * @see com.gemstone.gemfire.cache.Region#get(Object)
 * @since GemFire 3.0
 */
public class CacheWriterException extends OperationAbortedException {
private static final long serialVersionUID = -2872212342970454458L;
  
  /**
   * Creates a new instance of <code>CacheWriterException</code>.
   */
  public CacheWriterException() {
  }
  
  
  /**
   * Constructs an instance of <code>CacheWriterException</code> with the specified detail message.
   * @param msg the detail message
   */
  public CacheWriterException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>CacheWriterException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CacheWriterException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>CacheWriterException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public CacheWriterException(Throwable cause) {
    super(cause);
  }
}
