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
package org.apache.geode.management.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ManagementHelperTest {
  @Test
  public void isValidClassNameGivenEmptyStringReturnsFalse() {
    assertThat(ManagementHelper.isClassNameValid("")).isFalse();
  }

  @Test
  public void isValidClassNameGivenDashReturnsFalse() {
    assertThat(ManagementHelper.isClassNameValid("-")).isFalse();
  }

  @Test
  public void isValidClassNameGivenSpaceReturnsFalse() {
    assertThat(ManagementHelper.isClassNameValid(" ")).isFalse();
  }

  @Test
  public void isValidClassNameGivenCommaReturnsFalse() {
    assertThat(ManagementHelper.isClassNameValid(",")).isFalse();
  }

  @Test
  public void isValidClassNameGivenLeadingDotReturnsFalse() {
    assertThat(ManagementHelper.isClassNameValid(".a")).isFalse();
  }

  @Test
  public void isValidClassNameGivenTrailingDotReturnsFalse() {
    assertThat(ManagementHelper.isClassNameValid("a.")).isFalse();
  }

  @Test
  public void isValidClassNameGivenTwoDotsReturnsFalse() {
    assertThat(ManagementHelper.isClassNameValid("a..a")).isFalse();
  }

  @Test
  public void isValidClassNameGivenNameThatStartsWithDigitReturnsFalse() {
    assertThat(ManagementHelper.isClassNameValid("9a")).isFalse();
  }

  @Test
  public void isValidClassNameGivenNameReturnsTrue() {
    assertThat(ManagementHelper.isClassNameValid("a9")).isTrue();
  }

  @Test
  public void isValidClassNameGivenDotDelimitedNamesReturnsTrue() {
    assertThat(ManagementHelper.isClassNameValid("$a1._b2.c3$")).isTrue();
  }

  @Test
  public void isValidClassNameGivenMiddleNameThatStartsWithDigitReturnsFalse() {
    assertThat(ManagementHelper.isClassNameValid("a1.2b.c3")).isFalse();
  }
}
