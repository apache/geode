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

import java.util.Date;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementResult.StatusCode;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class ClusterManagementOperationResultTest {
  private ObjectMapper mapper;

  @Before
  public void setUp() {
    mapper = GeodeJsonMapper.getMapper();
  }

  @Test
  public void serialize() throws Exception {
    CompletableFuture<TestOperationResult> operationResult =
        new CompletableFuture<>();
    ClusterManagementResult result1 = new ClusterManagementResult();
    result1.setStatus(StatusCode.OK, "Success!!");
    ClusterManagementOperationResult<TestOperationResult> result =
        new ClusterManagementOperationResult<>(result1, operationResult, new Date(),
            new CompletableFuture<>(), "operator", "id");
    String json = mapper.writeValueAsString(result);
    System.out.println(json);
    ClusterManagementOperationResult value =
        mapper.readValue(json, ClusterManagementOperationResult.class);
    assertThat(value.getStatusMessage()).isEqualTo("Success!!");
  }

  static class TestOperationResult implements OperationResult {
  }
}
