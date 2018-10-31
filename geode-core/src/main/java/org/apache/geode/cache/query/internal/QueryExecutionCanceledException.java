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

package org.apache.geode.cache.query.internal;

import org.apache.geode.cache.CacheRuntimeException;

/**
 * Internal exception thrown when a query has been canceled and
 * QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCanceled() is called due to various threads
 * using the method, access
 * to the query object may not be available for certain threads This exception is generically used
 * and caught by DefaultQuery, which will then throw the appropriate exception
 *
 * @since GemFire 7.0
 */
public class QueryExecutionCanceledException extends CacheRuntimeException {
  private static final long serialVersionUID = -2699578956684551688L;

  /**
   * Creates a new instance of <code>QueryExecutionCanceledException</code> without detail message.
   */
  public QueryExecutionCanceledException() {}

  /**
   * Constructs an instance of <code>QueryExecutionCanceledException</code> with the specified
   * detail message.
   *
   * @param msg the detail message.
   */
  public QueryExecutionCanceledException(String msg) {
    super(msg);
  }

  /**
   * Constructs an instance of <code>QueryExecutionCanceledException</code> with the specified
   * detail message and cause.
   *
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public QueryExecutionCanceledException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructs an instance of <code>QueryExecutionCanceledException</code> with the specified
   * cause.
   *
   * @param cause the causal Throwable
   */
  public QueryExecutionCanceledException(Throwable cause) {
    super(cause);
  }
}
