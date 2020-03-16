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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.TestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class KeySetOperationHandlerJUnitTest
    extends OperationHandlerJUnitTest<RegionAPI.KeySetRequest, RegionAPI.KeySetResponse> {
  private final String TEST_KEY1 = "Key1";
  private final String TEST_KEY2 = "Key2";
  private final String TEST_KEY3 = "Key3";
  private final String TEST_REGION = "test region";

  @Before
  public void setUp() {
    @SuppressWarnings("unchecked")
    Region<String, Object> regionStub = mock(Region.class);
    when(regionStub.keySet())
        .thenReturn(new HashSet<>(Arrays.asList(TEST_KEY1, TEST_KEY2, TEST_KEY3)));

    when(cacheStub.<String, Object>getRegion(TEST_REGION)).thenReturn(regionStub);
    operationHandler = new KeySetOperationHandler();
  }

  @Test
  public void verifyKeySetReturnsExpectedKeys() throws Exception {
    RegionAPI.KeySetRequest request =
        RegionAPI.KeySetRequest.newBuilder().setRegionName(TEST_REGION).build();
    Result<?> result = operationHandler.process(serializationService, request,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertThat(result).isInstanceOf(Success.class);
    RegionAPI.KeySetResponse response = (RegionAPI.KeySetResponse) result.getMessage();

    List<Object> results = response.getKeysList().stream().map(serializationService::decode)
        .collect(Collectors.toList());
    assertThat(results.size()).isEqualTo(3);
    assertThat(results).containsExactlyInAnyOrder(TEST_KEY1, TEST_KEY2, TEST_KEY3);
  }
}
