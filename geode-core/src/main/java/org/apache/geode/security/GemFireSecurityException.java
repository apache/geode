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
import java.io.Serializable;

import javax.naming.NamingException;

import org.apache.geode.GemFireException;

/**
 * The base class for all org.apache.geode.security package related exceptions.
 *
 * @since GemFire 5.5
 */
public class GemFireSecurityException extends GemFireException {

  private static final long serialVersionUID = 3814254578203076926L;

  private final Throwable cause;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message (which is saved for later retrieval by the
   *        {@link #getMessage()} method). (A <tt>null</tt> value is permitted.)
   */
  public GemFireSecurityException(final String message) {
    this(message, null);
  }

  /**
   * Constructs a new exception with the specified cause.
   *
   * <p>
   * Note that the detail message associated with {@code cause} <i>is</i> automatically used as this
   * exception's detail message.
   *
   * @param cause the cause (which is saved for later retrieval by the {@link #getCause()} method).
   *        (A <tt>null</tt> value is permitted, and indicates that the cause is nonexistent or
   *        unknown.)
   */
  public GemFireSecurityException(final Throwable cause) {
    this(cause != null ? cause.getMessage() : null, cause);
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
  public GemFireSecurityException(final String message, final Throwable cause) {
    super(message != null ? message : (cause != null ? cause.getMessage() : null));
    this.cause = cause;
  }

  @Override
  public synchronized Throwable getCause() {
    return (cause == this ? null : cause);
  }

  /**
   * Returns true if the provided {@code object} implements {@code Serializable}.
   *
   * @param object the {@code object} to test for implementing {@code Serializable}.
   * @return true if the provided {@code object} implements {@code Serializable}.
   */
  protected boolean isSerializable(final Object object) {
    if (object == null) {
      return true;
    }
    return object instanceof Serializable;
  }

  /**
   * Returns {@link NamingException#getResolvedObj()} if the {@code cause} is a
   * {@code NamingException}. Returns <tt>null</tt> for any other type of {@code cause}.
   *
   * @return {@code NamingException#getResolvedObj()} if the {@code cause} is a
   *         {@code NamingException}.
   */
  protected Object getResolvedObj() {
    final Throwable thisCause = cause;
    if (thisCause != null && thisCause instanceof NamingException) {
      return ((NamingException) thisCause).getResolvedObj();
    }
    return null;
  }

  private synchronized void writeObject(final ObjectOutputStream out) throws IOException {
    final Object resolvedObj = getResolvedObj();
    if (isSerializable(resolvedObj)) {
      out.defaultWriteObject();
    } else {
      final NamingException namingException = (NamingException) getCause();
      namingException.setResolvedObj(null);
      try {
        out.defaultWriteObject();
      } finally {
        namingException.setResolvedObj(resolvedObj);
      }
    }
  }
}
