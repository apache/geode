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
package org.apache.geode.internal.datasource;

import org.apache.geode.GemFireCheckedException;

/**
 * Exception thrown from DataSource factory.
 */
public class DataSourceCreateException extends GemFireCheckedException {

  private static final long serialVersionUID = 5107756585306908219L;

  /**
   * Creates a new instance of CreateConnectionException
   */
  public DataSourceCreateException() {
    super();
  }

  /**
   * Single Argument constructor to construct a new exception with the specified detail message.
   * Calls Exception class constructor.
   *
   * @param message The detail message. The detail message is saved for later retrieval.
   */
  public DataSourceCreateException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message The detail message. The detail message is saved for later retrieval.
   * @param cause the cause (which is saved for later retrieval.
   */
  public DataSourceCreateException(String message, Throwable cause) {
    super(message, cause);
  }

}
