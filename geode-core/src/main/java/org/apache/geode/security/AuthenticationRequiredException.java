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

package org.apache.geode.security;

/**
 * Thrown if the distributed system is in secure mode and this client/peer has
 * not set the security credentials.
 * 
 * @since GemFire 5.5
 */
public class AuthenticationRequiredException extends GemFireSecurityException {
private static final long serialVersionUID = 4675976651103154919L;

  /**
   * Constructs instance of <code>NotAuthenticatedException</code> with error
   * message.
   * 
   * @param message
   *                the error message
   */
  public AuthenticationRequiredException(String message) {
    super(message);
  }

  /**
   * Constructs instance of <code>NotAuthenticatedException</code> with error
   * message and cause.
   * 
   * @param message
   *                the error message
   * @param cause
   *                a <code>Throwable</code> that is a cause of this exception
   */
  public AuthenticationRequiredException(String message, Throwable cause) {
    super(message, cause);
  }

}
