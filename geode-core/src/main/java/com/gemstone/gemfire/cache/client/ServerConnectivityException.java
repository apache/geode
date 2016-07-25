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
package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.GemFireException;

/**
 * A generic exception indicating that a failure has happened while communicating
 * with a gemfire server. Subclasses of this exception provide more detail
 * on specific failures.
 * @since GemFire 5.7
 */
public class ServerConnectivityException extends GemFireException {
private static final long serialVersionUID = -5205644901262051330L;

  /**
   * Create a new instance of ServerConnectivityException without a detail message or cause.
   */
  public ServerConnectivityException() {
  }

  /**
   * 
   * Create a new instance of ServerConnectivityException with a detail message
   * @param message the detail message
   */
  public ServerConnectivityException(String message) {
    super(message);
  }

  /**
   * Create a new instance of ServerConnectivityException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public ServerConnectivityException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new instance of ServerConnectivityException with a cause
   * @param cause the cause
   */
  public ServerConnectivityException(Throwable cause) {
    super(cause);
  }

}
