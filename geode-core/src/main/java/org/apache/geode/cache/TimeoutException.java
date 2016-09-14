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

/** Thrown if a <code>netSearch</code> times out without getting a response back from a cache,
 * or when attempting to acquire a distributed lock.
 *
 *
 *
 * @see LoaderHelper#netSearch
 * @see org.apache.geode.cache.Region#invalidateRegion()
 * @see org.apache.geode.cache.Region#destroyRegion()
 * @see Region#createSubregion
 * @see org.apache.geode.cache.Region#get(Object)
 * @see org.apache.geode.cache.Region#put(Object, Object)
 * @see org.apache.geode.cache.Region#create(Object, Object)
 * @see org.apache.geode.cache.Region#invalidate(Object)
 * @see org.apache.geode.cache.Region#destroy(Object)
 * @see org.apache.geode.distributed.DistributedLockService
 * @since GemFire 3.0
 */
public class TimeoutException extends OperationAbortedException {
private static final long serialVersionUID = -6260761691185737442L;
  
  /**
   * Creates a new instance of <code>TimeoutException</code> without detail message.
   */
  public TimeoutException() {
  }
  
  
  /**
   * Constructs an instance of <code>TimeoutException</code> with the specified detail message.
   * @param msg the detail message
   */
  public TimeoutException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>TimeoutException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public TimeoutException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>TimeoutException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public TimeoutException(Throwable cause) {
    super(cause);
  }
}
