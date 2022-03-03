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

package org.apache.geode.management.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;

public class RealizationResultTest {

  private RealizationResult result;
  private final ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Before
  public void before() {
    result = new RealizationResult();
  }

  @Test
  public void defaultConstructor() {
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getMemberName()).isNull();
    assertThat(result.getMessage()).isEqualTo("Success.");
  }

  @Test
  public void setter() throws Exception {
    result.setMemberName("member1").setSuccess(false).setMessage("message");
    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getMemberName()).isEqualTo("member1");
    assertThat(result.getMessage()).isEqualTo("message");
  }

  @Test
  public void jsonSerialization() throws Exception {
    result.setMemberName("member1").setMessage("successfully created the region");
    String json = mapper.writeValueAsString(result);
    // make sure the additional info is unwrapped to the same level as the
    // message/membername/success
    assertThat(json).contains("\"memberName\":\"member1\"")
        .contains("\"success\":true")
        .contains("\"message\":\"successfully created the region\"");

    RealizationResult readValue = mapper.readValue(json, RealizationResult.class);
    assertThat(readValue.isSuccess()).isTrue();
    assertThat(readValue.getMemberName()).isEqualTo("member1");
    assertThat(readValue.getMessage()).isEqualTo("successfully created the region");
  }
}
