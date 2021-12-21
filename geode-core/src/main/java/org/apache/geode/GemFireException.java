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
package org.apache.geode;

/**
 * This is the abstract superclass of exceptions that are thrown to indicate incorrect usage of
 * GemFire.
 * <p>
 * Since these exceptions are unchecked, this class really <em>ought</em> to be called
 * {@code GemFireRuntimeException}; however, the current name is retained for compatibility's sake.
 * <p>
 * This class is abstract to enforce throwing more specific exception types. Please avoid using
 * GemFireException to describe an arbitrary error condition
 *
 * @see GemFireCheckedException
 * @see org.apache.geode.cache.CacheRuntimeException
 */
public abstract class GemFireException extends RuntimeException {
  private static final long serialVersionUID = -6972360779789402295L;

  /**
   * Creates a new {@code GemFireException} with no detailed message.
   */
  public GemFireException() {
    super();
  }

  /**
   * Creates a new {@code GemFireException} with the given detail message.
   */
  public GemFireException(String message) {
    super(message);
  }

  /**
   * Creates a new {@code GemFireException} with the given detail message and cause.
   */
  public GemFireException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new {@code GemFireException} with the given cause and no detail message
   */
  public GemFireException(Throwable cause) {
    super(cause);
  }

  /**
   * Returns the root cause of this {@code GemFireException} or {@code null} if the cause is
   * nonexistent or unknown.
   */
  public Throwable getRootCause() {
    if (getCause() == null) {
      return null;
    }
    Throwable root = getCause();
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root;
  }

}
