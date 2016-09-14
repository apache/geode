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
package org.apache.geode.management.cli;

/**
 * Indicates that an exception occurred while processing a GemFire Command Line
 * Interface (CLI) command. The exception may be thrown because of:
 * <ul>
 * <li>parsing errors from unknown parameters
 * <li>errors from invalid values for parameters
 * </ul>
 * 
 * @since GemFire 7.0
 */
public class CommandProcessingException extends RuntimeException {

  private static final long serialVersionUID = -1398779521639575884L;

  /** Error Type indicating an invalid command */
  public static int COMMAND_INVALID = 1;
  /** Error Type indicating an ambiguous command name */
  public static int COMMAND_NAME_AMBIGUOUS = 2;
  /**
   * Error Type indicating an unknown or unavailable command. GemFire CLI
   * Commands are context sensitive & might not be always available.
   */
  public static int COMMAND_INVALID_OR_UNAVAILABLE = 3;
  /** Error Type indicating an invalid named parameter of a command. */
  public static int OPTION_INVALID = 4;
  /** Error Type indicating an invalid parameter of a command. */
  public static int ARGUMENT_INVALID = 5;
  /** Error Type indicating an invalid value for a parameter of a command. */
  public static int OPTION_VALUE_INVALID = 6;
  /**
   * Error Type indicating the absence of a mandatory named parameter of a
   * command.
   */
  public static int REQUIRED_OPTION_MISSING = 7;
  /** Error Type indicating the absence of a mandatory parameter of a command. */
  public static int REQUIRED_ARGUMENT_MISSING = 8;
  /**
   * Error Type indicating the absence of value for named parameter of a
   * command.
   */
  public static int OPTION_VALUE_REQUIRED = 9;
  // public static int ARGUMENT_VALUE_REQUIRED = 10;
  /**
   * Error Type indicating IO errors occurred while accessing File/Network
   * resource
   */
  public static int RESOURCE_ACCESS_ERROR = 11;

  private final int errorType;
  private final Object errorData;

  private static String[] errorTypeStrings = { "", // 0
      "COMMAND_INVALID", // 1
      "COMMAND_NAME_AMBIGUOUS", // 2
      "COMMAND_INVALID_OR_UNAVAILABLE", // 3
      "OPTION_INVALID", // 4
      "ARGUMENT_INVALID", // 5
      "OPTION_VALUE_INVALID", // 6
      "REQUIRED_OPTION_MISSING", // 7
      "REQUIRED_ARGUMENT_MISSING", // 8
      "OPTION_VALUE_REQUIRED", // 9
      "ARGUMENT_VALUE_REQUIRED", // 10
      "RESOURCE_ACCESS_ERROR" // 11
  };

  /**
   * Constructs a new <code>CommandProcessingException</code>
   * 
   * @param message
   *          The error detail message.
   * @param errorType
   *          One of the error types defined in
   *          <code>CommandProcessingException</code>
   * @param errorData
   *          Additional information about the error.
   */
  public CommandProcessingException(String message, int errorType, Object errorData) {
    super(message);
    this.errorType = errorType;
    this.errorData = errorData;
  }

  /**
   * Returns the Error Type.
   */
  public int getErrorType() {
    return errorType;
  }

  /**
   * Returns the error data.
   */
  public Object getErrorData() {
    return errorData;
  }

  /**
   * Returns the String representation of this
   * <code>CommandProcessingException<code>
   */
  @Override
  public String toString() {
    return CommandProcessingException.class.getSimpleName() + "[errorType=" + errorTypeStrings[errorType] + ", errorData="
        + errorData + "]";
  }
}
