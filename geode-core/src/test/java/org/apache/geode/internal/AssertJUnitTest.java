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

package org.apache.geode.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;


public class AssertJUnitTest {
  @Test
  public void testAssertArgumentIsLegal() {
    Assert.assertArgument(true, "");
  }

  @Test
  public void testAssertArgumentIsIllegal() {
    assertThatThrownBy(() -> {
      Assert.assertArgument(false, "The actual argument is %1$s!", "illegal");
    }).isInstanceOf(IllegalArgumentException.class).hasMessage("The actual argument is illegal!");
  }

  @Test
  public void testAssetNotNullWithNonNullObject() {
    Assert.assertNotNull(new Object(), "");
  }

  @Test
  public void testAssertNotNullWithNullObject() {
    assertThatThrownBy(() -> {
      Assert.assertNotNull(null, "This is an %1$s message!", "expected");
    }).isInstanceOf(NullPointerException.class).hasMessage("This is an expected message!");

  }

  @Test
  public void testAssertStateIsValid() {
    Assert.assertState(true, "");
  }

  @Test
  public void testAssertStateIsInvalid() {
    assertThatThrownBy(() -> {
      Assert.assertState(false, "The actual state is %1$s!", "invalid");
    }).isInstanceOf(IllegalStateException.class).hasMessage("The actual state is invalid!");
  }
}
