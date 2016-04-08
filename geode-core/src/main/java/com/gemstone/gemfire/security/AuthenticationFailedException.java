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

package com.gemstone.gemfire.security;

/**
 * Thrown if authentication of this client/peer fails.
 * 
 * @since 5.5
 */
public class AuthenticationFailedException extends GemFireSecurityException {
private static final long serialVersionUID = -8202866472279088879L;

  // TODO Derive from SecurityException
  /**
   * Constructs instance of <code>AuthenticationFailedException</code> with
   * error message.
   * 
   * @param message
   *                the error message
   */
  public AuthenticationFailedException(String message) {
    super(message);
  }

  /**
   * Constructs instance of <code>AuthenticationFailedException</code> with
   * error message and cause.
   * 
   * @param message
   *                the error message
   * @param cause
   *                a <code>Throwable</code> that is a cause of this exception
   */
  public AuthenticationFailedException(String message, Throwable cause) {
    super(message, cause);
  }

}
