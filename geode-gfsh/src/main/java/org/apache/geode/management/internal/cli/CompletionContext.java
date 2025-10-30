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
  private final boolean isFirstOption; // NEW: Track if user has provided any options yet
  private final Object commandManager; // ROOT CAUSE #10: Needed for hint/help topic completion

  /**
   * Private constructor - use factory methods instead.
   */
  private CompletionContext(Type type, String commandName, String optionName,
      String partialInput, int cursorPosition, boolean isFirstOption, Object commandManager) {
    this.type = type;
    this.commandName = commandName;
    this.optionName = optionName;
    this.partialInput = partialInput != null ? partialInput : "";
    this.cursorPosition = cursorPosition;
    this.isFirstOption = isFirstOption;
    this.commandManager = commandManager;
  }

  /**
   * Create a context for completing a command name.
   *
   * @param partialInput The partial command name typed by the user
   * @return CompletionContext for command name completion
   */
  public static CompletionContext commandName(String partialInput) {
    return new CompletionContext(Type.COMMAND_NAME, null, null, partialInput, 0, false, null);
  }

  /**
   * Create a context for completing an option name.
   *
   * @param commandName The command being executed
   * @param partialOption The partial option name (without "--")
   * @param isFirstOption Whether this is the first option being completed
   * @return CompletionContext for option name completion
   */
  public static CompletionContext optionName(String commandName, String partialOption,
      boolean isFirstOption) {
    return new CompletionContext(Type.OPTION_NAME, commandName, null, partialOption, 0,
        isFirstOption, null);
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
    return new CompletionContext(Type.OPTION_VALUE, commandName, optionName, partialValue, 0,
        false, null);
  }

  /**
   * Create an unknown completion context.
   *
   * @return CompletionContext with UNKNOWN type
   */
  public static CompletionContext unknown() {
    return new CompletionContext(Type.UNKNOWN, null, null, "", 0, false, null);
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

  /**
   * Check if this is the first option being completed.
   * When true, only mandatory options should be shown.
   *
   * @return true if no options have been provided yet
   */
  public boolean isFirstOption() {
    return isFirstOption;
  }

  /**
   * Get the CommandManager instance for access to command metadata.
   * ROOT CAUSE #10: Needed for hint/help topic completion to access Helper.getTopicNames().
   *
   * @return The CommandManager, or null if not set
   */
  public Object getCommandManager() {
    return commandManager;
  }

  /**
   * Create a new context with the CommandManager set.
   * ROOT CAUSE #10: GfshParser calls this to pass CommandManager to completion providers.
   *
   * @param newCommandManager The CommandManager instance
   * @return A new CompletionContext with commandManager set
   */
  public CompletionContext withCommandManager(Object newCommandManager) {
    return new CompletionContext(this.type, this.commandName, this.optionName,
        this.partialInput, this.cursorPosition, this.isFirstOption, newCommandManager);
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
