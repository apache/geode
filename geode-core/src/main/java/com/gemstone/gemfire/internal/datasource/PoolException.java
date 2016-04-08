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
package com.gemstone.gemfire.internal.datasource;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * Exception thrown from Connection Pool.
 * 
 */
public class PoolException extends GemFireCheckedException {
private static final long serialVersionUID = -6178632158204356727L;

  //public Exception excep;

  /** Creates a new instance of PoolException */
  public PoolException() {
    super();
  }

  /**
   * @param message
   */
  public PoolException(String message) {
    super(message);
  }

  /**
   * Single Argument constructor to construct a new exception with the specified
   * detail message. Calls Exception class constructor.
   * 
   * @param message The detail message. The detail message is saved for later
   *          retrieval.
   */
  public PoolException(String message, Exception ex) {
    super(message,ex);
    //this.excep = ex;
  }

}
