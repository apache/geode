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

/**
 * <code>RoleException</code> is the superclass of those exceptions
 * that can be thrown to indicate a reliability failure on one or more {@link
 * Region regions} that have been configured with required roles using 
 * {@link MembershipAttributes}.
 *
 * @deprecated this feature is scheduled to be removed
 */
public abstract class RoleException extends CacheRuntimeException {
  private static final long serialVersionUID = -7521056108445887394L;

  /**
   * Creates a new instance of <code>RoleException</code> without
   * detail message.
   */
  public RoleException() {
  }
  
  
  /**
   * Constructs an instance of <code>RoleException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public RoleException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>RoleException</code> with the
   * specified detail message and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public RoleException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>RoleException</code> with the
   * specified cause.
   * @param cause the causal Throwable
   */
  public RoleException(Throwable cause) {
    super(cause);
  }

}

