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
package org.apache.geode.management.internal.cli;

/**
 * Encapsulates the context for command-line completion operations.
 * Provides information about what is being completed (command name, option name, or option value)
 * and the partial input provided by the user.
 *
 * @since Spring Shell 3.x completion implementation
 */
public class CompletionContext {

  /**
   * Type of completion being performed.
   */
  public enum Type {
    /** Completing a command name (e.g., "cre" → "create region") */
    COMMAND_NAME,

    /** Completing an option name (e.g., "--n" → "--name") */
    OPTION_NAME,

    /** Completing an option value (e.g., "--action=" → "APPLY", "STAGE") */
    OPTION_VALUE,

    /** Unknown or unsupported completion context */
    UNKNOWN
  }

  private final Type type;
  private final String commandName;
  private final String optionName;
  private final String partialInput;
  private final int cursorPosition;

  /**
   * Private constructor - use factory methods instead.
   */
  private CompletionContext(Type type, String commandName, String optionName,
      String partialInput, int cursorPosition) {
    this.type = type;
    this.commandName = commandName;
    this.optionName = optionName;
    this.partialInput = partialInput != null ? partialInput : "";
    this.cursorPosition = cursorPosition;
  }

  /**
   * Create a context for completing a command name.
   *
   * @param partialInput The partial command name typed by the user
   * @return CompletionContext for command name completion
   */
  public static CompletionContext commandName(String partialInput) {
    return new CompletionContext(Type.COMMAND_NAME, null, null, partialInput, 0);
  }

  /**
   * Create a context for completing an option name.
   *
   * @param commandName The command being executed
   * @param partialOption The partial option name (without "--")
   * @return CompletionContext for option name completion
   */
  public static CompletionContext optionName(String commandName, String partialOption) {
    return new CompletionContext(Type.OPTION_NAME, commandName, null, partialOption, 0);
  }

  /**
   * Create a context for completing an option value.
   *
   * @param commandName The command being executed
   * @param optionName The option name (without "--")
   * @param partialValue The partial value typed by the user
   * @return CompletionContext for option value completion
   */
  public static CompletionContext optionValue(String commandName, String optionName,
      String partialValue) {
    return new CompletionContext(Type.OPTION_VALUE, commandName, optionName, partialValue, 0);
  }

  /**
   * Create an unknown completion context.
   *
   * @return CompletionContext with UNKNOWN type
   */
  public static CompletionContext unknown() {
    return new CompletionContext(Type.UNKNOWN, null, null, "", 0);
  }

  /**
   * Get the type of completion.
   *
   * @return The completion type
   */
  public Type getType() {
    return type;
  }

  /**
   * Get the command name (for option/value completion).
   *
   * @return The command name, or null if not applicable
   */
  public String getCommandName() {
    return commandName;
  }

  /**
   * Get the option name (for value completion).
   *
   * @return The option name, or null if not applicable
   */
  public String getOptionName() {
    return optionName;
  }

  /**
   * Get the partial input typed by the user.
   *
   * @return The partial input string
   */
  public String getPartialInput() {
    return partialInput;
  }

  /**
   * Get the cursor position.
   *
   * @return The cursor position in the input
   */
  public int getCursorPosition() {
    return cursorPosition;
  }

  @Override
  public String toString() {
    return "CompletionContext{" +
        "type=" + type +
        ", commandName='" + commandName + '\'' +
        ", optionName='" + optionName + '\'' +
        ", partialInput='" + partialInput + '\'' +
        ", cursorPosition=" + cursorPosition +
        '}';
  }
}
