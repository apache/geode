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

/** Indicates that the operation that
 * would have otherwise affected the cache has been aborted.
 * Only subclasses are instantiated.
 *
 *
 *
 * @since GemFire 3.0
 */
public abstract class OperationAbortedException extends CacheRuntimeException {
  private static final long serialVersionUID = -8293166225026556949L;

  /**
   * Creates a new instance of <code>OperationAbortedException</code> without detail message.
   */
  public OperationAbortedException() {
  }
  
  
  /**
   * Constructs an instance of <code>OperationAbortedException</code> with the specified detail message.
   * @param msg the detail message
   */
  public OperationAbortedException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>OperationAbortedException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public OperationAbortedException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>OperationAbortedException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public OperationAbortedException(Throwable cause) {
    super(cause);
  }
}
