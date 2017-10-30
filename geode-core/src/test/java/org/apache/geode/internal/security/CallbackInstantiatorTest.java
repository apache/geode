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
package org.apache.geode.internal.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({UnitTest.class, SecurityTest.class})
public class CallbackInstantiatorTest {

  private static final String STRING = "STRING";
  private static final String NULL_STRING = null;
  private static final String STRING_NON_STATIC = "STRING_NON_STATIC";
  private static final Boolean BOOLEAN = Boolean.TRUE;

  @Test
  public void testGetObjectFromConstructor() {
    String string = CallbackInstantiator.getObjectOfType(String.class.getName(), String.class);
    assertThat(string).isNotNull().isEqualTo("");

    CharSequence charSequence =
        CallbackInstantiator.getObjectOfType(String.class.getName(), CharSequence.class);
    assertThat(charSequence).isNotNull().isEqualTo("");

    assertThatThrownBy(
        () -> CallbackInstantiator.getObjectOfType("com.abc.testString", String.class))
            .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(
        () -> CallbackInstantiator.getObjectOfType(String.class.getName(), Boolean.class))
            .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> CallbackInstantiator.getObjectOfType("", String.class))
        .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> CallbackInstantiator.getObjectOfType(null, String.class))
        .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> CallbackInstantiator.getObjectOfType("  ", String.class))
        .isInstanceOf(GemFireSecurityException.class);
  }

  @Test
  public void testGetObjectFromFactoryMethod() {
    String string = CallbackInstantiator.getObjectOfType(Factories.class.getName() + ".getString",
        String.class);
    assertThat(string).isNotNull().isEqualTo(STRING);

    CharSequence charSequence = CallbackInstantiator
        .getObjectOfType(Factories.class.getName() + ".getString", String.class);
    assertThat(charSequence).isNotNull().isEqualTo(STRING);

    assertThatThrownBy(() -> CallbackInstantiator
        .getObjectOfType(Factories.class.getName() + ".getStringNonStatic", String.class))
            .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> CallbackInstantiator
        .getObjectOfType(Factories.class.getName() + ".getNullString", String.class))
            .isInstanceOf(GemFireSecurityException.class);
  }

  private static class Factories {

    public static String getString() {
      return STRING;
    }

    public static String getNullString() {
      return NULL_STRING;
    }

    public String getStringNonStatic() {
      return STRING_NON_STATIC;
    }

    public static Boolean getBoolean() {
      return BOOLEAN;
    }
  }
}
