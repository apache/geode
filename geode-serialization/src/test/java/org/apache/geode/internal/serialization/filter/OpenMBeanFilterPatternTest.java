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
package org.apache.geode.internal.serialization.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.rmi.MarshalledObject;

import javax.management.ObjectName;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularType;

import org.junit.Before;
import org.junit.Test;

public class OpenMBeanFilterPatternTest {

  private String defaultPattern;

  @Before
  public void setUp() {
    defaultPattern = new OpenMBeanFilterPattern().pattern();
  }

  @Test
  public void includesBoolean() {
    assertThat(defaultPattern).contains(Boolean.class.getName());
  }

  @Test
  public void includesByte() {
    assertThat(defaultPattern).contains(Byte.class.getName());
  }

  @Test
  public void includesCharacter() {
    assertThat(defaultPattern).contains(Character.class.getName());
  }

  @Test
  public void includesShort() {
    assertThat(defaultPattern).contains(Short.class.getName());
  }

  @Test
  public void includesInteger() {
    assertThat(defaultPattern).contains(Integer.class.getName());
  }

  @Test
  public void includesLong() {
    assertThat(defaultPattern).contains(Long.class.getName());
  }

  @Test
  public void includesFloat() {
    assertThat(defaultPattern).contains(Float.class.getName());
  }

  @Test
  public void includesDouble() {
    assertThat(defaultPattern).contains(Double.class.getName());
  }

  @Test
  public void includesString() {
    assertThat(defaultPattern).contains(String.class.getName());
  }

  @Test
  public void includesBigInteger() {
    assertThat(defaultPattern).contains(BigInteger.class.getName());
  }

  @Test
  public void includesBigDecimal() {
    assertThat(defaultPattern).contains(BigDecimal.class.getName());
  }

  @Test
  public void includesObjectName() {
    assertThat(defaultPattern).contains(ObjectName.class.getName());
  }

  @Test
  public void includesCompositeData() {
    assertThat(defaultPattern).contains(CompositeData.class.getName());
  }

  @Test
  public void includesTabularData() {
    assertThat(defaultPattern).contains(TabularData.class.getName());
  }

  @Test
  public void includesSimpleType() {
    assertThat(defaultPattern).contains(SimpleType.class.getName());
  }

  @Test
  public void includesCompositeType() {
    assertThat(defaultPattern).contains(CompositeType.class.getName());
  }

  @Test
  public void includesTabularType() {
    assertThat(defaultPattern).contains(TabularType.class.getName());
  }

  @Test
  public void includesArrayType() {
    assertThat(defaultPattern).contains(ArrayType.class.getName());
  }

  @Test
  public void includesMarshalledObject() {
    assertThat(defaultPattern).contains(MarshalledObject.class.getName());
  }

  @Test
  public void rejectsAllOtherTypes() {
    assertThat(defaultPattern).endsWith(";!*");
  }
}
