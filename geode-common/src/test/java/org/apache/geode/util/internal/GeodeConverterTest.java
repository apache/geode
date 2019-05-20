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

package org.apache.geode.util.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class GeodeConverterTest {
  @Test
  public void coercesInt() {
    assertThat(GeodeConverter.convertToActualType("8", "int")).isEqualTo(8);
    assertThat(GeodeConverter.convertToActualType("8", Integer.class.getName())).isEqualTo(8);
    assertThat(GeodeConverter.convertToActualType("-8", "int")).isEqualTo(-8);
    assertThat(GeodeConverter.convertToActualType("-8", Integer.class.getName())).isEqualTo(-8);
  }

  @Test
  public void coercesBoolean() {
    assertThat(GeodeConverter.convertToActualType("true", "boolean")).isEqualTo(true);
    assertThat(GeodeConverter.convertToActualType("true", Boolean.class.getName())).isEqualTo(true);
    assertThat(GeodeConverter.convertToActualType("false", "boolean")).isEqualTo(false);
    assertThat(GeodeConverter.convertToActualType("false", Boolean.class.getName()))
        .isEqualTo(false);
  }

  @Test
  public void coercesString() {
    assertThat(GeodeConverter.convertToActualType("foo", "string")).isEqualTo("foo");
    assertThat(GeodeConverter.convertToActualType("foo", String.class.getName())).isEqualTo("foo");
  }

  @Test
  public void coercesChar() {
    assertThat(GeodeConverter.convertToActualType("C", "char")).isEqualTo('C');
    assertThat(GeodeConverter.convertToActualType("C", Character.class.getName())).isEqualTo('C');
  }

  @Test
  public void coercesByte() {
    assertThat(GeodeConverter.convertToActualType("5", "byte")).isEqualTo((byte) 5);
    assertThat(GeodeConverter.convertToActualType("5", Byte.class.getName())).isEqualTo((byte) 5);
  }

  @Test
  public void coercesShort() {
    assertThat(GeodeConverter.convertToActualType("5", "short")).isEqualTo((short) 5);
    assertThat(GeodeConverter.convertToActualType("5", Short.class.getName())).isEqualTo((short) 5);
  }

  @Test
  public void coercesLong() {
    assertThat(GeodeConverter.convertToActualType("555555555555555555", "long"))
        .isEqualTo(555555555555555555L);
    assertThat(GeodeConverter.convertToActualType("5", Long.class.getName())).isEqualTo((long) 5);
  }

  @Test
  public void coercesFloat() {
    assertThat(GeodeConverter.convertToActualType("5.0", "float")).isEqualTo((float) 5);
    assertThat(GeodeConverter.convertToActualType("5.0", Float.class.getName()))
        .isEqualTo((float) 5);
  }

  @Test
  public void coercesDouble() {
    assertThat(GeodeConverter.convertToActualType("5.0", "double")).isEqualTo((double) 5);
    assertThat(GeodeConverter.convertToActualType("5.0", Double.class.getName()))
        .isEqualTo((double) 5);
  }
}
