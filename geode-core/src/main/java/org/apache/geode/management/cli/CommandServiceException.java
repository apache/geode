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
package com.gemstone.gemfire.management.cli;

/**
 * Indicates that an exception occurred while accessing/creating a Command
 * Service for processing GemFire Command Line Interface (CLI) commands.
 * 
 * 
 * @since GemFire 7.0
 */
public class CommandServiceException extends Exception {

  private static final long serialVersionUID = 7316102209844678329L;

  /**
   * Constructs a new <code>CommandServiceException</code> with the specified
   * detail message and cause.
   * 
   * @param message
   *          The detail message.
   * @param cause
   *          The cause of this exception or <code>null</code> if the cause is
   *          unknown.
   */
  public CommandServiceException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new <code>CommandServiceException</code> with the specified detail 
   * message.
   * 
   * @param message
   *          The detail message.
   */
  public CommandServiceException(String message) {
    super(message);
  }

  /**
   * Constructs a new <code>CommandServiceException</code> by wrapping the
   * specified cause. The detail for this exception will be null if the cause is
   * null or cause.toString() if a cause is provided.
   * 
   * @param cause
   *          The cause of this exception or <code>null</code> if the cause is
   *          unknown.
   */
  public CommandServiceException(Throwable cause) {
    super(cause);
  }
}
