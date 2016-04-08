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
/*
 * CreateConnectionException.java
 *
 * Created on February 18, 2005, 3:34 PM
 */
package com.gemstone.gemfire.internal.datasource;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * Exception thrown from the connection provider.
 * 
 */
public class ConnectionProviderException extends GemFireCheckedException  {
private static final long serialVersionUID = -7406652144153958227L;

  public Exception excep;

  /** Creates a new instance of CreateConnectionException */
  public ConnectionProviderException() {
    super();
  }

  /**
   * @param message
   */
  public ConnectionProviderException(String message) {
    super(message);
  }

  /**
   * Single Argument constructor to construct a new exception with the specified
   * detail message. Calls Exception class constructor.
   * 
   * @param message The detail message. The detail message is saved for later
   *          retrieval.
   */
  public ConnectionProviderException(String message, Exception ex) {
    super(message);
    this.excep = ex;
  }

  /**
   * @return ???
   */
  @Override
  public StackTraceElement[] getStackTrace() {
    return excep.getStackTrace();
  }
}
