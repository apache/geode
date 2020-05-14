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

import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ErrorMessageFactory;

/**
 * Creates an error message indicating that an assertion that verifies that a {@link Notification}
 * has no user data failed.
 */
public class ShouldHaveNoUserData extends BasicErrorMessageFactory {

  /**
   * Creates a new <code>{@link ShouldHaveNoUserData}</code>.
   *
   * @param actual the actual value in the failed assertion.
   * @return the created {@code ErrorMessageFactory}.
   */
  public static ErrorMessageFactory shouldHaveNoUserData(Object actual) {
    return new ShouldHaveNoUserData(actual);
  }

  private ShouldHaveNoUserData(Object actual) {
    super("%nExpecting notification without user data, but user data was:<%s>", actual);
  }
}
