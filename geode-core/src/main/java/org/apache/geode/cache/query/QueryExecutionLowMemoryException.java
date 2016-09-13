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

package org.apache.geode.cache.query;

import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.control.ResourceManager;
/**
 * Thrown when the query is executing and the critical heap percentage is met.
 * @see ResourceManager#setCriticalHeapPercentage(float)
 * @see ResourceManager#getCriticalHeapPercentage()
 * 
 * If critical heap percentage is set, the query monitor can be disabled
 * from sending out QueryExecutionLowMemoryExeceptions at the risk of
 * a query exhausting all memory.
 *
 * @since GemFire 7.0
 */
public class QueryExecutionLowMemoryException extends CacheRuntimeException {
  
  /**
   * Creates a new instance of <code>QueryExecutionLowMemoryException</code> without detail message.
   */
  public QueryExecutionLowMemoryException() {
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionLowMemoryException</code> with the specified detail message.
   * @param msg the detail message.
   */
  public QueryExecutionLowMemoryException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionLowMemoryException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public QueryExecutionLowMemoryException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionLowMemoryException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public QueryExecutionLowMemoryException(Throwable cause) {
    super(cause);
  }
}
