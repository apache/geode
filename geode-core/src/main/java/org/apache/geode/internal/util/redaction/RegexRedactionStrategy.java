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

import java.util.function.Function;
import java.util.regex.Matcher;

/**
 * Simple redaction strategy using regex to parse options.
 */
class RegexRedactionStrategy implements RedactionStrategy {

  private final Function<String, Boolean> isSensitive;
  private final String redacted;

  RegexRedactionStrategy(Function<String, Boolean> isSensitive, String redacted) {
    this.isSensitive = isSensitive;
    this.redacted = redacted;
  }

  @Override
  public String redact(String string) {
    Matcher matcher = ParserRegex.getPattern().matcher(string);
    while (matcher.find()) {
      String option = matcher.group(2);
      if (!isSensitive.apply(option)) {
        continue;
      }

      String leadingBoundary = matcher.group(1);
      String separator = matcher.group(3);
      String withRedaction = leadingBoundary + option + separator + redacted;
      string = string.replace(matcher.group(), withRedaction);
    }

    return string;
  }
}
