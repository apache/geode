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

package org.apache.geode.benchmark.jmh.profilers;

import static org.apache.geode.benchmark.jmh.profilers.ObjectSizeAgent.sizeOf;
import static org.apache.geode.benchmark.jmh.profilers.ObjectSizeAgent.sizeOfDeep;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ObjectSizeAgentTest {

  @Test
  public void object() {
    final Object object = new Object();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  @Test
  public void objectWithNoFields() {
    final ObjectWithNoFields object = new ObjectWithNoFields();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  @Test
  public void objectWithIntField() {
    final ObjectWithIntField object = new ObjectWithIntField();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  @Test
  public void objectWith2IntFields() {
    final ObjectWith2IntFields object = new ObjectWith2IntFields();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  @Test
  public void objectWith3IntFields() {
    final ObjectWith3IntFields object = new ObjectWith3IntFields();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  @Test
  public void objectWith4IntFields() {
    final ObjectWith4IntFields object = new ObjectWith4IntFields();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  @Test
  public void objectWithLongField() {
    final ObjectWithLongField object = new ObjectWithLongField();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  @Test
  public void objectWithObjectField() {
    final ObjectWithObjectField object = new ObjectWithObjectField();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object) + sizeOf(object.o1));
  }

  @Test
  public void objectWithArrayOfInt0Fields() {
    final ObjectWithArrayOfInt0Field object = new ObjectWithArrayOfInt0Field();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object) + sizeOf(object.a1));
  }

  @Test
  public void objectWithArrayOfInt100Fields() {
    final ObjectWithArrayOfInt100Field object = new ObjectWithArrayOfInt100Field();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object) + sizeOf(object.a1));
  }

  @Test
  public void arrayOfInt100() {
    final int[] array = new int[100];
    assertThat(sizeOfDeep(array)).isEqualTo(sizeOf(array));
  }

  @Test
  public void objectWithIntFieldExtendsObjectWithIntField() {
    final ObjectWithIntFieldExtendsObjectWithIntField object =
        new ObjectWithIntFieldExtendsObjectWithIntField();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  @Test
  public void objectWithArrayOfObject100Field() {
    final ObjectWithArrayOfObject100Field object = new ObjectWithArrayOfObject100Field();
    assertThat(sizeOfDeep(object))
        .isEqualTo(sizeOf(object) + sizeOf(object.a1) + sizeOf(new Object()) * 100);
  }

  @Test
  public void objectWithLoop() {
    final ObjectWithLoop object = new ObjectWithLoop();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  @SuppressWarnings("InstantiationOfUtilityClass")
  @Test
  public void objectWithStaticField() {
    final ObjectWithStaticField object = new ObjectWithStaticField();
    assertThat(sizeOfDeep(object)).isEqualTo(sizeOf(object));
  }

  private static class ObjectWithNoFields {
  }

  @SuppressWarnings("unused")
  private static class ObjectWithIntField {
    final int i1 = 0;
  }

  @SuppressWarnings("unused")
  private static class ObjectWith2IntFields {
    final int i1 = 0;
    final int i2 = 0;
  }

  @SuppressWarnings("unused")
  private static class ObjectWith3IntFields {
    final int i1 = 0;
    final int i2 = 0;
    final int i3 = 0;
  }

  @SuppressWarnings("unused")
  private static class ObjectWith4IntFields {
    final int i1 = 0;
    final int i2 = 0;
    final int i3 = 0;
    final int i4 = 0;
  }

  @SuppressWarnings("unused")
  private static class ObjectWithLongField {
    final long l1 = 0;
  }

  @SuppressWarnings("unused")
  private static class ObjectWithObjectField {
    final Object o1 = new Object();
  }

  @SuppressWarnings("unused")
  private static class ObjectWithArrayOfInt0Field {
    final int[] a1 = new int[0];
  }

  @SuppressWarnings("unused")
  private static class ObjectWithArrayOfInt100Field {
    final int[] a1 = new int[100];
  }

  @SuppressWarnings("unused")
  private static class ObjectWithIntFieldExtendsObjectWithIntField extends ObjectWithIntField {
    final int i2 = 0;
  }

  @SuppressWarnings("unused")
  private static class ObjectWithArrayOfObject100Field {
    final Object[] a1 = new Object[100];

    public ObjectWithArrayOfObject100Field() {
      for (int i = 0; i < a1.length; i++) {
        a1[i] = new Object();
      }
    }
  }

  @SuppressWarnings("unused")
  private static class ObjectWithLoop {
    final ObjectWithLoop loop = this;
  }

  @SuppressWarnings("unused")
  private static class ObjectWithStaticField {
    static final Object o1 = new Object();
  }

}
