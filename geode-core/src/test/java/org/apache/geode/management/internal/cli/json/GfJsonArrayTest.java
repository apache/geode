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

package org.apache.geode.management.internal.cli.json;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GfJsonArrayTest {

  private GfJsonArray array;

  @Before
  public void setup() {
    array = new GfJsonArray();
  }

  @Test
  public void emptyArray() {
    assertThat(array.size()).isEqualTo(0);
    assertThat(array.toString()).isEqualTo("[]");
  }

  @Test
  public void arrayFromPrimitives() throws Exception {
    array = new GfJsonArray(new String[] {"a", "b", "c"});

    assertThat(array.size()).isEqualTo(3);
    assertThat(array.get(0)).isEqualTo("a");
    assertThat(array.get(1)).isEqualTo("b");
    assertThat(array.get(2)).isEqualTo("c");
  }

  @Test
  public void addSingleObject() throws Exception {
    array.put("a");
    assertThat(array.get(0)).isEqualTo("a");
    assertThat(array.toString()).isEqualTo("[\"a\"]");
  }

  @Test
  public void addMultipleObjects() throws Exception {
    array.put("a");
    array.put("b");
    array.put("c");

    assertThat(array.get(0)).isEqualTo("a");
    assertThat(array.get(1)).isEqualTo("b");
    assertThat(array.get(2)).isEqualTo("c");
    assertThat(array.toString()).isEqualTo("[\"a\",\"b\",\"c\"]");
  }

  @Test
  public void addCollection() throws Exception {
    List<String> multiple = new ArrayList<>();
    multiple.add("a");
    multiple.add("b");
    multiple.add("c");
    array.put(multiple);

    assertThat(array.size()).isEqualTo(1);
    assertThat(array.get(0).toString()).isEqualTo("[\"a\",\"b\",\"c\"]");
  }

  @Test
  public void addMap() throws Exception {
    Map<Integer, String> multiple = new HashMap<>();
    multiple.put(1, "a");
    multiple.put(2, "b");
    multiple.put(3, "c");
    array.put(multiple);

    assertThat(array.size()).isEqualTo(1);
    assertThat(array.get(0).toString()).isEqualTo("{1=a, 2=b, 3=c}");
  }

  @Test
  public void putOutOfBoundsAddsNull() throws Exception {
    array.put(1, "a");

    assertThat(array.size()).isEqualTo(2);
    assertThat(array.toString()).isEqualTo("[null,\"a\"]");
  }

}
