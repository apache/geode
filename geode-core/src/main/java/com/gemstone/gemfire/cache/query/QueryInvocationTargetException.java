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
package com.gemstone.gemfire.cache.query;

/**
 * Thrown if an exception is thrown when a method is invoked during query execution.
 *
 * @since 4.0
 */

public class QueryInvocationTargetException extends QueryException {
private static final long serialVersionUID = 2978208305701582906L;
  
  /**
   * Construct an instance of QueryInvalidException
   * @param cause a Throwable cause of this exception
   */
  public QueryInvocationTargetException(Throwable cause) {
    super(cause);
  }
  
  
  /**
   * Construct an instance of QueryInvalidException
   * @param msg the error message
   */
  public QueryInvocationTargetException(String msg) {
    super(msg);
  }
  
  /**
   * Construct an instance of QueryInvalidException
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public QueryInvocationTargetException(String msg, Throwable cause) {
    super(msg, cause);
  }  
}
