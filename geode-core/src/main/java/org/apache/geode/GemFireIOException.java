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
 * A <code>GemFireIOException</code> is thrown when a GemFire operation failure is caused by an
 * <code>IOException</code>.
 *
 *
 */
public class GemFireIOException extends GemFireException {
  private static final long serialVersionUID = 5694009444435264497L;

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>GemFireIOException</code>.
   */
  public GemFireIOException(String message, Throwable cause) {
    super(message
        + ((cause != null && cause.getMessage() != null) ? " ( " + cause.getMessage() + " )" : ""),
        cause);
  }

  public GemFireIOException(String message) {
    super(message);
  }
}
