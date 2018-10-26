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
package org.apache.geode.internal.logging.assertj.impl;

import java.util.List;

import org.assertj.core.api.Condition;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ErrorMessageFactory;

public class ShouldContainLine extends BasicErrorMessageFactory {

  /**
   * Creates a new <code>{@link ShouldContainLine}</code>.
   *
   * @param actual the actual file in the failed assertion.
   * @param entryCondition entry condition.
   * @return the created {@code ErrorMessageFactory}.
   */
  public static ErrorMessageFactory shouldContainLine(List<String> actual,
      Condition<?> entryCondition) {
    return new ShouldContainLine(actual, entryCondition);
  }

  private ShouldContainLine(List<String> actual, Condition<?> entryCondition) {
    super("%n" +
        "Expecting:%n" +
        " <%s>%n" +
        "to contain a line satisfying:%n" +
        " <%s>",
        actual, entryCondition);
  }
}
