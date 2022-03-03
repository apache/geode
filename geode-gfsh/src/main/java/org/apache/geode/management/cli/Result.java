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
package org.apache.geode.management.cli;

import java.io.IOException;

/**
 * The result of processing a GemFire Command Line Interface (CLI) command string.
 *
 * A string representation of this Result can be iterated over using the methods
 * {@link #hasNextLine()}, {@link #nextLine()} and {@link #resetToFirstLine()}.
 *
 * A Result may have one or more files as part of the command output and if so they can be saved to
 * the file system using {@link #saveIncomingFiles(String)}. To check whether the result has a file
 * in it use {@link #hasIncomingFiles()}.
 *
 *
 * @since GemFire 7.0
 */
public interface Result {

  /**
   * Indicates a Results status.
   *
   * @since GemFire 7.0
   */
  enum Status {
    /**
     * Indicates that the command completed successfully.
     */
    OK(0),

    /**
     * Indicates that an error occurred while processing the command.
     */
    ERROR(-1);

    private final int code;

    Status(int code) {
      this.code = code;
    }

    /**
     * Returns the code associated with this state.
     */
    public int getCode() {
      return code;
    }
  }

  /**
   * Returns the status of a processed command.
   */
  Status getStatus();

  /**
   * Resets the pointer to the first line in the Result.
   */
  void resetToFirstLine();

  /**
   * Returns whether the result has any more lines of information.
   *
   * @return True if there are more lines, false otherwise.
   */
  boolean hasNextLine();

  /**
   * Returns the next line of information from the Result.
   *
   * @throws IndexOutOfBoundsException if this method is called more times than there are lines of
   *         information.
   */
  String nextLine();

  /**
   * Returns whether this Result has a file as a part of the command output.
   *
   * @return True if there is a file, false otherwise.
   * @deprecated since 1.10. file transfer in plugin commands is never supported
   */
  default boolean hasIncomingFiles() {
    return false;
  }

  /**
   * Save the file(s) from this Result. {@link #hasIncomingFiles()} should be used before calling
   * this method to verify that the Result contains a file.
   *
   * @param directory Directory to which the file(s) should be saved.
   * @throws IOException If an error occurs while saving the file.
   * @throws RuntimeException If there is no file in the Result to save.
   * @deprecated since 1.10. file transfer in plugin commands is never supported
   */
  default void saveIncomingFiles(String directory) throws IOException {}

  /****
   * Return whether the configuration changes due to command have been persisted to cluster
   * configuration or not.
   *
   * @return True if the command has failed to persist configuration changes , false otherwise.
   * @deprecated since 1.10. This only affect the gfsh output line which is not api bound
   */
  default boolean failedToPersist() {
    return false;
  }

  /*****
   * Sets whether the command changes have not been persisted to the cluster configuration
   *
   * @param commandPersisted true if the command changes are persisted to the cluster configuration,
   *        false otherwise.
   *
   * @deprecated since 1.10. This only affect the gfsh output line which is not api bound
   */
  default void setCommandPersisted(boolean commandPersisted) {}

  /**
   * @return the string representation of the result with the specified line separator.
   */
  default String asString() {
    StringBuilder builder = new StringBuilder();
    while (hasNextLine()) {
      builder.append(nextLine());
    }
    resetToFirstLine();
    return builder.toString();
  }
}
