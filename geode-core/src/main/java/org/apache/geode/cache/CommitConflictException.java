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

/** Thrown when a commit fails due to a write conflict.
 *
 *
 * @see CacheTransactionManager#commit
 * @since GemFire 4.0
 */
public class CommitConflictException extends TransactionException {
  private static final long serialVersionUID = -1491184174802596675L;

  /**
   * Constructs an instance of <code>CommitConflictException</code> with the specified detail message.
   * @param msg the detail message
   */
  public CommitConflictException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>CommitConflictException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CommitConflictException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>CommitConflictException</code> with the specified cause.
   * @param cause the causal Throwable
   * @since GemFire 6.5
   */
  public CommitConflictException(Throwable cause) {
    super(cause);
  }
  
}
