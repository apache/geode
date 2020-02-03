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

import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementResult.StatusCode;
import org.apache.geode.management.api.EntityGroupInfo;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.runtime.RuntimeRegionInfo;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class ClusterManagementResultTest {
  private ClusterManagementResult result;

  @Before
  public void setup() {
    result = new ClusterManagementResult();
  }

  @Test
  public void failsWhenNotAppliedOnAllMembers() {
    ClusterManagementRealizationResult result = new ClusterManagementRealizationResult();
    result.addMemberStatus(new RealizationResult().setMemberName("member-1")
        .setSuccess(true).setMessage("msg-1"));
    result.addMemberStatus(new RealizationResult().setMemberName("member-2")
        .setSuccess(false).setMessage("msg-2"));
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void successfulOnlyWhenResultIsSuccessfulOnAllMembers() {
    ClusterManagementRealizationResult result = new ClusterManagementRealizationResult();
    result.addMemberStatus(new RealizationResult().setMemberName("member-1")
        .setSuccess(true).setMessage("msg-1"));
    result.addMemberStatus(new RealizationResult().setMemberName("member-2")
        .setSuccess(true).setMessage("msg-2"));
    assertThat(result.isSuccessful()).isTrue();
  }

  @Test
  public void emptyMemberStatus() {
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(StatusCode.OK);
  }


  @Test
  public void failsWhenNotPersisted() {
    result.setStatus(StatusCode.ERROR, "msg-1");
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void whenNoMembersExists() {
    result.setStatus(StatusCode.ERROR, "msg-1");
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void whenNoMemberExists2() {
    result.setStatus(StatusCode.OK, "msg-1");
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
    result = new ClusterManagementResult(StatusCode.OK, "message");
    assertThat(result.getStatusCode()).isEqualTo(StatusCode.OK);

    result = new ClusterManagementResult(StatusCode.ERROR, "message");
    assertThat(result.getStatusCode()).isEqualTo(ERROR);

    result = new ClusterManagementResult();
    result.setStatus(StatusCode.ERROR, "message");
    assertThat(result.getStatusCode()).isEqualTo(ERROR);

    result = new ClusterManagementResult();
    result.setStatus(StatusCode.OK, "message");
    assertThat(result.getStatusCode()).isEqualTo(StatusCode.OK);
  }

  @Test
  public void unsuccessfulMemeberStatusSetsErrorCode() {
    ClusterManagementRealizationResult result =
        new ClusterManagementRealizationResult(StatusCode.OK, "message");
    result.addMemberStatus(new RealizationResult().setMemberName("member-1")
        .setSuccess(true).setMessage("message-1"));
    result.addMemberStatus(new RealizationResult().setMemberName("member-2")
        .setSuccess(false).setMessage("message-2"));
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode()).isEqualTo(ERROR);
  }

  @Test
  public void deserialize() throws Exception {
    String json = "{\"statusCode\":\"OK\"}";
    ClusterManagementListResult result =
        GeodeJsonMapper.getMapper().readValue(json, ClusterManagementListResult.class);
    assertThat(result.getResult()).isNotNull().isEmpty();
  }

  @Test
  public void serializeResult() throws Exception {
    ObjectMapper mapper = GeodeJsonMapper.getMapper();
    ClusterManagementListResult<Region, RuntimeRegionInfo> result =
        new ClusterManagementListResult<>();
    EntityGroupInfo<Region, RuntimeRegionInfo> response = new EntityGroupInfo<>();
    Region region = new Region();
    region.setName("region");
    region.setType(RegionType.REPLICATE);
    region.setGroup("group1");
    response.setConfiguration(region);

    RuntimeRegionInfo info = new RuntimeRegionInfo();
    info.setEntryCount(3);
    response.setRuntimeInfo(Collections.singletonList(info));
    result.setResult(Collections.singletonList(response));

    String json = mapper.writeValueAsString(result);
    System.out.println(json);
    ClusterManagementListResult<Region, RuntimeRegionInfo> result1 =
        mapper.readValue(json, ClusterManagementListResult.class);
    assertThat(result1.getConfigResult()).hasSize(1)
        .extracting(Region::getName).containsExactly("region");
    assertThat(result1.getRuntimeResult()).hasSize(1)
        .extracting(RuntimeRegionInfo::getEntryCount).containsExactly(3L);
  }
}
