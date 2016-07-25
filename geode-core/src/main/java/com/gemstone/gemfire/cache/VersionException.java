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
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * An <code>VersionException</code> is an exception that indicates
 * a client / server version mismatch exception has occurred.
 *
 * @since GemFire 5.7
 */
public abstract class VersionException extends GemFireCheckedException {

  /** Constructs a new <code>VersionException</code>. */
  public VersionException() {
    super();
  }

  /** Constructs a new <code>VersionException</code> with a message string. */
  public VersionException(String s) {
    super(s);
  }

  /** Constructs a <code>VersionException</code> with a message string and
   * a base exception
   */
  public VersionException(String s, Throwable cause) {
    super(s, cause);
  }

  /** Constructs a <code>VersionException</code> with a cause */
  public VersionException(Throwable cause) {
    super(cause);
  }
}
