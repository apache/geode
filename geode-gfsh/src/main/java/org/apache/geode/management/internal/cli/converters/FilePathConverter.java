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
package org.apache.geode.management.internal.cli.converters;

import java.io.File;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * Spring Shell 3.x converter for File objects.
 *
 * <p>
 * Converts a file path string to a {@link File} object.
 * Used by commands with file options (e.g., --file for run command).
 *
 * <p>
 * This converter delegates file system completion to {@link FilePathStringConverter}.
 * For auto-completion, use the completion methods from FilePathStringConverter:
 * <ul>
 * <li>{@link FilePathStringConverter#getRoots()}</li>
 * <li>{@link FilePathStringConverter#getSiblings(String)}</li>
 * </ul>
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * - Spring Shell 1.x: Used supports(), convertFromText(), getAllPossibleValues()
 * - Spring Shell 3.x: Simple Converter<String, File> for conversion only
 * - Completion delegated to FilePathStringConverter utility methods
 * - Auto-completion should be implemented via ValueProvider (separate concern)
 *
 * @since GemFire 7.0
 */
@Component
public class FilePathConverter implements Converter<String, File> {
  private FilePathStringConverter delegate;

  /**
   * Creates a FilePathConverter with a default delegate.
   */
  public FilePathConverter() {
    delegate = new FilePathStringConverter();
  }

  /**
   * Sets a custom delegate for file path completion logic.
   *
   * <p>
   * This is primarily used for testing to inject a mock delegate.
   *
   * @param delegate the FilePathStringConverter to use for completion
   */
  public void setDelegate(FilePathStringConverter delegate) {
    this.delegate = delegate;
  }

  /**
   * Gets the current delegate.
   *
   * @return the FilePathStringConverter delegate
   */
  public FilePathStringConverter getDelegate() {
    return delegate;
  }

  /**
   * Converts a file path string to a File object.
   *
   * @param source the file path string
   * @return File object representing the path
   */
  @Override
  public File convert(@NonNull String source) {
    return new File(source);
  }
}
