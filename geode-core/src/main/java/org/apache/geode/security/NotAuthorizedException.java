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
package org.apache.geode.security;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.Principal;

import javax.naming.NamingException;

/**
 * Thrown when a client/peer is unauthorized to perform a requested operation.
 *
 * @since GemFire 5.5
 */
public class NotAuthorizedException extends GemFireSecurityException {

  private static final long serialVersionUID = 419215768216387745L;

  private Principal principal = null;

  /**
   * Constructs a new exception with the specified detail message and principal.
   *
   * @param message the detail message (which is saved for later retrieval by the
   *        {@link #getMessage()} method). (A <tt>null</tt> value is permitted.)
   */
  public NotAuthorizedException(final String message) {
    this(message, null, null);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * <p>
   * If {@code message} is null, then the detail message associated with {@code cause} <i>is</i>
   * automatically used as this exception's detail message.
   *
   * @param message the detail message (which is saved for later retrieval by the
   *        {@link #getMessage()} method). (A <tt>null</tt> value is permitted.)
   * @param cause the cause (which is saved for later retrieval by the {@link #getCause()} method).
   *        (A <tt>null</tt> value is permitted, and indicates that the cause is nonexistent or
   *        unknown.)
   */
  public NotAuthorizedException(final String message, final Throwable cause) {
    this(message, cause, null);
  }

  /**
   * Constructs a new exception with the specified detail message and principal.
   *
   * @param message the detail message (which is saved for later retrieval by the
   *        {@link #getMessage()} method). (A <tt>null</tt> value is permitted.)
   * @param principal the principal for which authorization failed. (A <tt>null</tt> value is
   *        permitted.)
   */
  public NotAuthorizedException(final String message, final Principal principal) {
    this(message, null, principal);
  }

  /**
   * Constructs a new exception with the specified detail message, cause and principal.
   *
   * <p>
   * If {@code message} is null, then the detail message associated with {@code cause} <i>is</i>
   * automatically used as this exception's detail message.
   *
   * @param message the detail message (which is saved for later retrieval by the
   *        {@link #getMessage()} method). (A <tt>null</tt> value is permitted.)
   * @param cause the cause (which is saved for later retrieval by the {@link #getCause()} method).
   *        (A <tt>null</tt> value is permitted, and indicates that the cause is nonexistent or
   *        unknown.)
   * @param principal the principal for which authorization failed. (A <tt>null</tt> value is
   *        permitted.)
   */
  public NotAuthorizedException(final String message, final Throwable cause,
      final Principal principal) {
    super(message, cause);
    this.principal = principal;
  }

  /**
   * Returns the {@code principal} for which authorization failed.
   *
   * @return the {@code principal} for which authorization failed.
   */
  public synchronized Principal getPrincipal() {
    return principal;
  }

  private synchronized void writeObject(final ObjectOutputStream out) throws IOException {
    final Principal thisPrincipal = principal;
    if (!isSerializable(thisPrincipal)) {
      principal = null;
    }

    final Object resolvedObj = getResolvedObj();
    NamingException namingException = null;
    if (!isSerializable(resolvedObj)) {
      namingException = (NamingException) getCause();
      namingException.setResolvedObj(null);
    }

    try {
      out.defaultWriteObject();
    } finally {
      principal = thisPrincipal;
      if (namingException != null) {
        namingException.setResolvedObj(resolvedObj);
      }
    }
  }
}
