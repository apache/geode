/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.api;

import static org.apache.geode.management.api.ClusterManagementResult.StatusCode.ERROR;
import static org.apache.geode.management.api.ClusterManagementResult.StatusCode.OK;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class ClusterManagementResultTest {
  private ClusterManagementResult<?> result;

  @Before
  public void setup() {
    result = new ClusterManagementResult<>();
  }

  @Test
  public void failsWhenNotAppliedOnAllMembers() {
    result.addMemberStatus("member-1", true, "msg-1");
    result.addMemberStatus("member-2", false, "msg-2");
    result.setStatus(true, "message");
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void successfulOnlyWhenResultIsSuccessfulOnAllMembers() {
    result.addMemberStatus("member-1", true, "msg-1");
    result.addMemberStatus("member-2", true, "msg-2");
    result.setStatus(true, "message");
    assertThat(result.isSuccessful()).isTrue();
  }

  @Test
  public void emptyMemberStatus() {
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
  }


  @Test
  public void failsWhenNotPersisted() {
    result.setStatus(false, "msg-1");
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void whenNoMembersExists() {
    result.setStatus(false, "msg-1");
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void whenNoMemberExists2() {
    result.setStatus(true, "msg-1");
    assertThat(result.isSuccessful()).isTrue();
  }

  @Test
  public void errorCodeWillAlwaysBeUnSuccessful() {
    result = new ClusterManagementResult(ERROR, "message");
    assertThat(result.isSuccessful()).isFalse();

    result = new ClusterManagementResult(OK, "message");
    assertThat(result.isSuccessful()).isTrue();
  }

  @Test
  public void onlyUnsuccessfulResultHasErrorCode() {
    result = new ClusterManagementResult(true, "message");
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);

    result = new ClusterManagementResult(false, "message");
    assertThat(result.getStatusCode()).isEqualTo(ERROR);

    result = new ClusterManagementResult<>();
    result.setStatus(false, "message");
    assertThat(result.getStatusCode()).isEqualTo(ERROR);

    result = new ClusterManagementResult<>();
    result.setStatus(true, "message");
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    result.addMemberStatus("member-1", true, "message-1");
    result.addMemberStatus("member-2", false, "message-2");
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode()).isEqualTo(ERROR);
  }

  @Test
  public void deserialize() throws Exception {
    String json = "{\"statusCode\":\"OK\"}";
    ClusterManagementResult<?> result =
        GeodeJsonMapper.getMapper().readValue(json, ClusterManagementResult.class);
    assertThat(result.getResult()).isNotNull().isEmpty();
  }
}
