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
package org.apache.geode.management.internal.cli.shell;

/**
 * RuntimeException to wrap JMX Connection Error/Exception.
 *
 * @since GemFire 7.0
 */
public class JMXConnectionException extends RuntimeException {
  private static final long serialVersionUID = 3872374016604940917L;

  public static final int OTHER = 1;
  public static final int MANAGER_NOT_FOUND_EXCEPTION = 2;
  public static final int CONNECTION_EXCEPTION = 3;

  private final int exceptionType;

  public JMXConnectionException(String message, Throwable cause, int exceptionType) {
    super(message, cause);
    this.exceptionType = exceptionType;
  }

  public JMXConnectionException(String message, int exceptionType) {
    super(message);
    this.exceptionType = exceptionType;
  }

  public JMXConnectionException(Throwable cause, int exceptionType) {
    super(cause);
    this.exceptionType = exceptionType;
  }

  public JMXConnectionException(int exceptionType) {
    this.exceptionType = exceptionType;
  }

  public int getExceptionType() {
    return exceptionType;
  }
}
