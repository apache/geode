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

/** Thrown when an operation is invoked on <code>Region</code> for an entry that
 * doesn't exist in the <code>Region</code>. This exception is <i>not</i>
 * thrown by {@link com.gemstone.gemfire.cache.Region#get(Object)} or {@link Region#getEntry}.
 *
 *
 *
 * @see com.gemstone.gemfire.cache.Region#invalidate(Object)
 * @see com.gemstone.gemfire.cache.Region#destroy(Object)
 * @see Region.Entry
 * @since GemFire 3.0
 */
public class EntryNotFoundException extends CacheException {
private static final long serialVersionUID = -2404101631744605659L;
  /**
   * Constructs an instance of <code>EntryNotFoundException</code> with the specified detail message.
   * @param msg the detail message
   */
  public EntryNotFoundException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>EntryNotFoundException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public EntryNotFoundException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
