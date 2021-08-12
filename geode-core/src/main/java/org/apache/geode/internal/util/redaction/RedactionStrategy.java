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
package org.apache.geode.internal.util.redaction;

/**
 * Defines the algorithm for scanning input strings for redaction of sensitive strings.
 */
@FunctionalInterface
interface RedactionStrategy {

  /**
   * Parse a string to find key/value pairs and redact the values if identified as sensitive.
   *
   * <p>
   * The following format is expected:<br>
   * - Each key/value pair should be separated by spaces.<br>
   * - The key must be preceded by '--', '-D', or '--J=-D'.<br>
   * - The value may optionally be wrapped in quotation marks.<br>
   * - The value is assigned to a key with '=', '=' padded with any number of optional spaces, or
   * any number of spaces without '='.<br>
   * - The value must not contain spaces without being wrapped in quotation marks.<br>
   * - The value may contain spaces or any other symbols when wrapped in quotation marks.
   *
   * <p>
   * Examples:
   * <ol>
   * <li>"--password=secret"
   * <li>"--user me --password secret"
   * <li>"-Dflag -Dopt=arg"
   * <li>"--classpath=."
   * <li>"password=secret"
   * </ol>
   *
   * @param string The string input to be parsed
   *
   * @return A string that has sensitive data redacted.
   */
  String redact(String string);
}
