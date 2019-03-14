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

import org.junit.Before;
import org.junit.Test;


public class GfJsonArrayTest {

  private GfJsonArray gfJsonArray;

  public static class Simple {
    private int key;
    private String value;

    public Simple(int key, String value) {
      this.key = key;
      this.value = value;
    }

    public int getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }
  }

  @Before
  public void setup() {
    gfJsonArray = new GfJsonArray();
  }

  @Test
  public void emptyArray() {
    assertThat(gfJsonArray.size()).isEqualTo(0);
    assertThat(gfJsonArray.toString()).isEqualTo("[]");
  }

  @Test
  public void arrayFromPrimitives() throws Exception {
    gfJsonArray = new GfJsonArray(new String[] {"a", "b", "c"});

    assertThat(gfJsonArray.size()).isEqualTo(3);
    assertThat(gfJsonArray.getString(0)).isEqualTo("a");
    assertThat(gfJsonArray.getString(1)).isEqualTo("b");
    assertThat(gfJsonArray.getString(2)).isEqualTo("c");
  }

  @Test
  public void addSingleObject() throws Exception {
    gfJsonArray.put("a");
    assertThat(gfJsonArray.getString(0)).isEqualTo("a");
    assertThat(gfJsonArray.toString()).isEqualTo("[\"a\"]");
  }

  @Test
  public void addMultipleObjects() throws Exception {
    gfJsonArray.put("a");
    gfJsonArray.put("b");
    gfJsonArray.put("c");

    assertThat(gfJsonArray.getString(0)).isEqualTo("a");
    assertThat(gfJsonArray.getString(1)).isEqualTo("b");
    assertThat(gfJsonArray.getString(2)).isEqualTo("c");
    assertThat(gfJsonArray.toString()).isEqualTo("[\"a\",\"b\",\"c\"]");
  }

  @Test
  public void putObject() throws Exception {
    GfJsonObject obj = new GfJsonObject(new Simple(1, "a"));
    gfJsonArray.put(obj);
    gfJsonArray.put(1, obj);

    assertThat(gfJsonArray.getInternalJsonObject(0).getInt("key")).isEqualTo(1);
    assertThat(gfJsonArray.getInternalJsonObject(0).getString("value")).isEqualTo("a");
    assertThat(gfJsonArray.getInternalJsonObject(1).getInt("key")).isEqualTo(1);
    assertThat(gfJsonArray.getInternalJsonObject(1).getString("value")).isEqualTo("a");
  }

  @Test
  public void putOutOfBoundsAddsNull() throws Exception {
    gfJsonArray.put(1, "a");

    assertThat(gfJsonArray.size()).isEqualTo(2);
    assertThat(gfJsonArray.toString()).isEqualTo("[null,\"a\"]");
  }


}
