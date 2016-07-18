/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.security;


import static org.assertj.core.api.Java6Assertions.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GeodeSecurityUtilTest {

  @Test
  public void testGetObjectFromConstructor(){
    String string = GeodeSecurityUtil.getObjectOfType(String.class.getName(), String.class);
    assertNotNull(string);
    CharSequence charSequence = GeodeSecurityUtil.getObjectOfType(String.class.getName(), CharSequence.class);
    assertNotNull(charSequence);

    assertThatThrownBy(() -> GeodeSecurityUtil.getObjectOfType("com.abc.testString", String.class)).isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> GeodeSecurityUtil.getObjectOfType(String.class.getName(), Boolean.class)).isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> GeodeSecurityUtil.getObjectOfType("", String.class)).isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> GeodeSecurityUtil.getObjectOfType(null, String.class)).isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> GeodeSecurityUtil.getObjectOfType("  ", String.class)).isInstanceOf(GemFireSecurityException.class);
  }

  @Test
  public void testGetObjectFromFactoryMethod(){
    String string = GeodeSecurityUtil.getObjectOfType(Factories.class.getName()+".getString", String.class);
    assertNotNull(string);
    CharSequence charSequence = GeodeSecurityUtil.getObjectOfType(Factories.class.getName()+".getString", String.class);
    assertNotNull(charSequence);

    assertThatThrownBy(() -> GeodeSecurityUtil.getObjectOfType(Factories.class.getName()+".getStringNonStatic", String.class))
      .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> GeodeSecurityUtil.getObjectOfType(Factories.class.getName()+".getNullString", String.class))
      .isInstanceOf(GemFireSecurityException.class);
  }

  private static class Factories{
    public static String getString(){
      return new String();
    }

    public static String getNullString(){
      return null;
    }

    public String getStringNonStatic(){
      return new String();
    }

    public static Boolean getBoolean(){
      return Boolean.TRUE;
    }
  }
}
