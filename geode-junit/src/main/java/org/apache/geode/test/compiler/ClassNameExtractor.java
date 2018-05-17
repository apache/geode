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
package org.apache.geode.test.compiler;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClassNameExtractor {
  private static final Pattern EXTRACT_CLASS_NAME_REGEX =
      Pattern.compile("(?:public|private|protected)* *(?:abstract)* *(?:class|interface) +(\\w+)");

  public String extractFromSourceCode(String sourceCode) {
    Matcher m = EXTRACT_CLASS_NAME_REGEX.matcher(sourceCode);
    if (m.find()) {
      return m.group(1);
    } else {
      throw new IllegalArgumentException(
          String.format("Unable to parse class or interface name from: \n'%s'", sourceCode));
    }
  }
}
