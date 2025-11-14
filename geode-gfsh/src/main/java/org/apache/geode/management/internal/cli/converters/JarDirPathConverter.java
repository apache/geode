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

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * Spring Shell 3.x converter for JAR directory paths.
 *
 * <p>
 * Converts a directory path string to a String. Used by the deploy command's --dir parameter
 * to specify a directory containing JAR files to deploy.
 *
 * <p>
 * Example usage:
 *
 * <pre>
 * deploy --dir=/path/to/lib
 * </pre>
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * - Spring Shell 1.x: Used for both conversion AND file system auto-completion
 * - Spring Shell 3.x: Conversion only; auto-completion via ValueProvider (separate concern)
 * - This converter is a simple passthrough (String → String)
 * - File system completion should be implemented in a separate ValueProvider
 *
 * @since GemFire 7.0
 */
@Component
public class JarDirPathConverter implements Converter<String, String> {

  /**
   * Converts a directory path string to a String (passthrough).
   *
   * @param source the directory path
   * @return the same directory path
   */
  @Override
  public String convert(@NonNull String source) {
    return source;
  }
}
