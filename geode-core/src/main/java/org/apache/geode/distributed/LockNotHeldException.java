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

package org.apache.geode.distributed;

/**
 * A <code>LockNotHeldException</code> is thrown when attempting unlock a lock that is not held by
 * the thread performing the unlock.
 *
 * @since GemFire 4.0
 */
public class LockNotHeldException extends IllegalStateException {
  private static final long serialVersionUID = 3827860179805916215L;

  /**
   * Constructs a new exception with <code>null</code> as its detail message. The cause is not
   * initialized, and may subsequently be initialized by a call to {@link Throwable#initCause}.
   */
  public LockNotHeldException() {
    super();
  }

  /**
   * Constructs a new exception with the specified detail message. The cause is not initialized, and
   * may subsequently be initialized by a call to {@link Throwable#initCause}.
   *
   * @param message the detail message. The detail message is saved for later retrieval by the
   *        {@link #getMessage()} method.
   */
  public LockNotHeldException(String message) {
    super(message);
  }

}

