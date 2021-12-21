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
 * This is the abstract superclass of exceptions that are thrown and declared.
 * <p>
 * This class ought to be called <em>GemFireException</em>, but that name is reserved for an older
 * class that extends {@link java.lang.RuntimeException}.
 *
 * @see org.apache.geode.GemFireException
 * @since GemFire 5.1
 */
public abstract class GemFireCheckedException extends Exception {
  private static final long serialVersionUID = -8659184576090173188L;

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>GemFireException</code> with no detailed message.
   */
  public GemFireCheckedException() {
    super();
  }

  /**
   * Creates a new <code>GemFireCheckedException</code> with the given detail message.
   */
  public GemFireCheckedException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>GemFireException</code> with the given detail message and cause.
   */
  public GemFireCheckedException(String message, Throwable cause) {
    super(message);
    initCause(cause);
  }

  /**
   * Creates a new <code>GemFireCheckedException</code> with the given cause and no detail message
   */
  public GemFireCheckedException(Throwable cause) {
    super();
    initCause(cause);
  }

  //////////////////// Instance Methods ////////////////////

  /**
   * Returns the root cause of this <code>GemFireCheckedException</code> or <code>null</code> if the
   * cause is nonexistent or unknown.
   */
  public Throwable getRootCause() {
    if (getCause() == null) {
      return null;
    }
    Throwable root = getCause();
    while (root != null) {
      if (root.getCause() == null) {
        break;
      } else {
        root = root.getCause();
      }
    }
    return root;
  }

}
