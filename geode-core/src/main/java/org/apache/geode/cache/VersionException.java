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
package org.apache.geode.cache;

import org.apache.geode.GemFireCheckedException;

/**
 * An <code>VersionException</code> is an exception that indicates a client / server version
 * mismatch exception has occurred.
 *
 * @since GemFire 5.7
 */
public abstract class VersionException extends GemFireCheckedException {
  private static final long serialVersionUID = 5530501399086757174L;

  /** Constructs a new <code>VersionException</code>. */
  public VersionException() {
    super();
  }

  /**
   * Constructs a new <code>VersionException</code> with a message string.
   *
   * @param s the detail message
   */
  public VersionException(String s) {
    super(s);
  }

  /**
   * Constructs a <code>VersionException</code> with a message string and a base exception
   *
   * @param s the detail message
   * @param cause the cause
   */
  public VersionException(String s, Throwable cause) {
    super(s, cause);
  }

  /**
   * Constructs a <code>VersionException</code> with a cause
   *
   * @param cause the cause
   */
  public VersionException(Throwable cause) {
    super(cause);
  }
}
