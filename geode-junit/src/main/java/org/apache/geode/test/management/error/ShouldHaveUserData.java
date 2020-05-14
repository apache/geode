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
package org.apache.geode.test.management.error;

import static org.assertj.core.util.Preconditions.checkArgument;

import java.util.Objects;

import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ErrorMessageFactory;

public class ShouldHaveUserData extends BasicErrorMessageFactory {

  public static ErrorMessageFactory shouldHaveUserData(Object actualUserData,
      Object expectedUserData) {
    checkArgument(expectedUserData != null, "expected cause should not be null");
    // actualCause has no cause
    if (actualUserData == null) {
      return new ShouldHaveUserData(expectedUserData);
    }
    // same message => different type
    if (Objects.equals(actualUserData, expectedUserData)) {
      return new ShouldHaveUserData(actualUserData, expectedUserData.getClass());
    }
    // // same type => different message
    // if (Objects.equals(actualUserData.getClass(), expectedUserData.getClass())) {
    // return new ShouldHaveUserData(actualUserData, expectedUserData);
    // }
    return new ShouldHaveUserData(actualUserData, expectedUserData);
  }

  private ShouldHaveUserData(Object actualUserData, Object expectedUserData) {
    super("%n" +
        "Expecting a cause with type:%n" +
        "  <%s>%n" +
        "and message:%n" +
        "  <%s>%n" +
        "but type was:%n" +
        "  <%s>%n" +
        "and message was:%n" +
        "  <%s>.",
        expectedUserData.getClass().getName(), expectedUserData,
        actualUserData.getClass().getName(), actualUserData);
  }

  private ShouldHaveUserData(Object expectedUserData) {
    super("%n" +
        "Expecting a cause with type:%n" +
        "  <%s>%n" +
        "and message:%n" +
        "  <%s>%n" +
        "but actualCause had no cause.",
        expectedUserData.getClass().getName(), expectedUserData);
  }

  private ShouldHaveUserData(Object actualUserData, Class<? extends Object> expectedUserData) {
    super("%n" +
        "Expecting a cause with type:%n" +
        "  <%s>%n" +
        "but type was:%n" +
        "  <%s>.",
        expectedUserData.getName(), actualUserData.getClass().getName());
  }

  private ShouldHaveUserData(Object actualUserData, String expectedUserData) {
    super("%n" +
        "Expecting a cause with message:%n" +
        "  <%s>%n" +
        "but message was:%n" +
        "  <%s>.",
        expectedUserData, actualUserData);
  }
}
