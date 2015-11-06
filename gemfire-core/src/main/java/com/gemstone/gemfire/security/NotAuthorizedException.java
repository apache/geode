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

import java.security.Principal;

/**
 * Thrown when a client/peer is unauthorized to perform a requested operation.
 * 
 * @author Neeraj Kumar
 * @since 5.5
 */
public class NotAuthorizedException extends GemFireSecurityException {
private static final long serialVersionUID = 419215768216387745L;
  private Principal principal = null;
  /**
   * Constructs instance of <code>NotAuthorizedException</code> with error
   * message.
   * 
   * @param message
   *                the error message
   */
  public NotAuthorizedException(String message) {
    super(message);
  }

  public NotAuthorizedException(String message, Principal ppl) {
    super(message);
    this.principal = ppl;
  }
  
  public Principal getPrincipal() {
    return this.principal;
  }
  /**
   * Constructs instance of <code>NotAuthorizedException</code> with error
   * message and cause.
   * 
   * @param message
   *                the error message
   * @param cause
   *                a <code>Throwable</code> that is a cause of this exception
   */
  public NotAuthorizedException(String message, Throwable cause) {
    super(message, cause);
  }

}
