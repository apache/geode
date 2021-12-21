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

package org.apache.geode.internal.shared;

/**
 * Encapsulates an error in invoking OS native calls. A counterpart of JNA's
 * <code>LastErrorException</code> so as to not expose the JNA <code>LastErrorException</code>
 * class, and also for ODBC/.NET drivers that don't use JNA.
 *
 * @since GemFire 8.0
 */
public class NativeErrorException extends Exception {

  private static final long serialVersionUID = -1417824976407332942L;

  private final int errorCode;

  public NativeErrorException(String msg, int errorCode, Throwable cause) {
    super(msg, cause);
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
