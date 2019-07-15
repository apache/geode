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

import org.apache.geode.management.internal.CompletedAsyncOperationResult;
import org.apache.geode.management.runtime.RebalanceInfo;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class CompletedAsyncOperationResultTest {
  private ObjectMapper mapper;

  @Before
  public void setUp() {
    mapper = GeodeJsonMapper.getMapper();
  }

  @Test
  public void serialize() throws Exception {
    CompletedAsyncOperationResult<RebalanceInfo> operation = new CompletedAsyncOperationResult<>();
    operation.setResult(new RebalanceInfo("test2"));
    String json = mapper.writeValueAsString(operation);
    System.out.println(json);
    CompletedAsyncOperationResult<RebalanceInfo> value =
        mapper.readValue(json, CompletedAsyncOperationResult.class);
    assertThat(value.get().getRebalanceResult()).isEqualTo("test2");
  }
}
