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
package org.apache.geode.test.dunit.standalone;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class HeapSize {
  /**
   * Same default as specified in test.gradle:
   *
   * <pre>
   * maxHeapSize '768m'
   * </pre>
   */
  static final int MAX_HEAP_MB_DEFAULT = 768;

  private final Pattern regex = Pattern.compile("[0-9]+");
  private final InputArgumentsProvider parser;

  HeapSize() {
    this(new InputArgumentsProvider());
  }

  HeapSize(final InputArgumentsProvider parser) {
    this.parser = parser;
  }

  String getMaxHeapSize() {
    String argument = findMaxHeapSize(parser.getInputArguments());
    if (argument != null) {
      return parseMaxHeapSize(argument);
    }
    return "" + MAX_HEAP_MB_DEFAULT;
  }

  String parseMaxHeapSize(String argument) {
    Matcher matcher = regex.matcher(argument);
    if (matcher.find()) {
      if (argument.toLowerCase().endsWith("m")) {
        return String.valueOf(Integer.valueOf(matcher.group()));
      } else if (argument.toLowerCase().endsWith("g")) {
        return String.valueOf(Integer.valueOf(matcher.group()) * 1024);
      }
    }
    throw new IllegalArgumentException("No number found in " + argument);
  }

  String findMaxHeapSize(List<String> inputArguments) {
    for (String argument : inputArguments) {
      if (argument.startsWith("-Xmx")) {
        return argument;
      }
    }
    return null;
  }
}
