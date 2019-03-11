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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;


public class GfJsonObjectTest {

  private GfJsonObject gfJsonObject;

  public static class Simple {
    private int key;
    private String value;

    public int getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public Simple(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class GetMethodReturnsObjectReferencingMe {
    private String id;
    private GetMethodReturnsObjectReferencingMe peer;

    static GetMethodReturnsObjectReferencingMe createTestInstance() {
      GetMethodReturnsObjectReferencingMe inst1 = new GetMethodReturnsObjectReferencingMe();
      inst1.id = "instance1";
      GetMethodReturnsObjectReferencingMe inst2 = new GetMethodReturnsObjectReferencingMe();
      inst2.id = "instance2";
      inst1.peer = inst2;
      inst2.peer = inst1;
      return inst1;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public GetMethodReturnsObjectReferencingMe getPeer() {
      return peer;
    }

  }


  @Before
  public void setup() {
    gfJsonObject = new GfJsonObject();
  }

  @Test
  public void emptyObject() {
    assertThat(gfJsonObject.toString()).isEqualTo("{}");
  }

  @Test
  public void addObject() {
    gfJsonObject = new GfJsonObject(new Simple(1, "a"));

    assertThat(gfJsonObject.getString("key")).isEqualTo("1");
    assertThat(gfJsonObject.getString("value")).isEqualTo("a");
  }

  @Test
  public void addBidirectionalReferenceObject() throws GfJsonException {
    // JSONObject.cyclicDepChkEnabled.set(true);
    // JSONObject.cyclicDependencySet.set(new HashSet());
    new GfJsonObject(new GetMethodReturnsObjectReferencingMe());
  }

  @Test
  public void addRawObject() throws Exception {
    gfJsonObject = new GfJsonObject("{\"key\":1}");
    assertThat(gfJsonObject.getString("key")).isEqualTo("1");
  }

  @Test
  public void accumulatePrimitives() throws Exception {
    gfJsonObject.accumulate("string", "value1");
    gfJsonObject.accumulate("string", "value2");

    assertThat(gfJsonObject.getString("string")).isEqualTo("[\"value1\",\"value2\"]");
  }

  @Test
  public void putAndGetPrimitives() throws Exception {
    gfJsonObject.put("string", "value1");
    gfJsonObject.put("int", Integer.MAX_VALUE);
    gfJsonObject.put("boolean", true);
    gfJsonObject.put("double", Double.MAX_VALUE);
    gfJsonObject.put("long", Long.MAX_VALUE);

    assertThat(gfJsonObject.getString("string")).isEqualTo("value1");

    assertThat(gfJsonObject.getString("int")).isEqualTo(Integer.toString(Integer.MAX_VALUE));
    assertThat(gfJsonObject.getInt("int")).isEqualTo(Integer.MAX_VALUE);

    assertThat(gfJsonObject.getString("boolean")).isEqualTo("true");
    assertThat(gfJsonObject.getBoolean("boolean")).isEqualTo(true);

    assertThat(gfJsonObject.getString("double")).isEqualTo(Double.toString(Double.MAX_VALUE));
    assertThat(gfJsonObject.getDouble("double")).isEqualTo(Double.MAX_VALUE);

    assertThat(gfJsonObject.getString("long")).isEqualTo(Long.toString(Long.MAX_VALUE));
    assertThat(gfJsonObject.getDouble("long")).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void appendCreatesAndAddsToArray() throws Exception {
    gfJsonObject.append("array", 1);
    gfJsonObject.append("array", 2);

    assertThat(gfJsonObject.getString("array")).isEqualTo("[1,2]");
  }

  @Test
  public void cannotAppendToExistingKey() throws Exception {
    gfJsonObject.put("wat", 1);
    assertThatThrownBy(() -> gfJsonObject.append("wat", 2)).isInstanceOf(GfJsonException.class);
  }

  @Test
  public void canGetGfJsonObject() throws Exception {
    GfJsonObject sub = new GfJsonObject();
    sub.put("foo", "bar");
    gfJsonObject.put("sub", sub);

    assertThat(gfJsonObject.getJSONObject("sub").toString()).isEqualTo("{\"foo\":\"bar\"}");
  }

  @Test
  public void canGetGfJsonArray() throws Exception {
    gfJsonObject.append("array", 1);
    gfJsonObject.append("array", "a");

    assertThat(gfJsonObject.getJSONArray("array").toString()).isEqualTo("[1,\"a\"]");
  }

  @Test
  public void canGetNames() throws Exception {
    gfJsonObject.put("string", "value1");
    gfJsonObject.put("int", Integer.MAX_VALUE);
    gfJsonObject.put("boolean", true);
    gfJsonObject.put("double", Double.MAX_VALUE);
    gfJsonObject.put("long", Long.MAX_VALUE);

    assertThat(gfJsonObject.names().size()).isEqualTo(5);
    assertThat(gfJsonObject.names().toString())
        .isEqualTo("[\"string\",\"int\",\"boolean\",\"double\",\"long\"]");
  }
}
