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

import static org.apache.geode.internal.protocol.TestExecutionContext.getNoAuthCacheExecutionContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.GetRegionNamesRequest;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.GetRegionNamesResponse;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class GetRegionNamesRequestOperationHandlerJUnitTest
    extends OperationHandlerJUnitTest<GetRegionNamesRequest, GetRegionNamesResponse> {
  private final String TEST_REGION1 = "test region 1";
  private final String TEST_REGION2 = "test region 2";
  private final String TEST_REGION3 = "test region 3";

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    Region<String, String> region1Stub = mock(Region.class);
    when(region1Stub.getFullPath()).thenReturn(TEST_REGION1);
    Region<String, String> region2Stub = mock(Region.class);
    when(region2Stub.getFullPath()).thenReturn(TEST_REGION2);
    Region<String, String> region3Stub = mock(Region.class);
    when(region3Stub.getFullPath()).thenReturn(TEST_REGION3);

    when(cacheStub.rootRegions()).thenReturn(Collections
        .unmodifiableSet(new HashSet<>(Arrays.asList(region1Stub, region2Stub, region3Stub))));
    operationHandler = new GetRegionNamesRequestOperationHandler();
  }

  @Test
  public void processReturnsCacheRegions() throws Exception {
    Result<GetRegionNamesResponse> result = operationHandler.process(serializationService,
        ProtobufRequestUtilities.createGetRegionNamesRequest(),
        getNoAuthCacheExecutionContext(cacheStub));
    assertThat(result).isInstanceOf(Success.class);

    GetRegionNamesResponse getRegionsResponse = result.getMessage();
    assertThat(getRegionsResponse.getRegionsCount()).isEqualTo(3);
    assertThat(getRegionsResponse.getRegionsList()).hasSize(3)
        .containsExactlyInAnyOrder(TEST_REGION1, TEST_REGION2, TEST_REGION3);
  }

  @Test
  public void processReturnsNoCacheRegions() throws Exception {
    InternalCache emptyCache = mock(InternalCacheForClientAccess.class);
    doReturn(emptyCache).when(emptyCache).getCacheForProcessingClientRequests();
    when(emptyCache.rootRegions()).thenReturn(Collections.unmodifiableSet(Collections.emptySet()));
    Result<GetRegionNamesResponse> result = operationHandler.process(serializationService,
        ProtobufRequestUtilities.createGetRegionNamesRequest(),
        getNoAuthCacheExecutionContext(emptyCache));
    assertThat(result).isInstanceOf(Success.class);

    GetRegionNamesResponse getRegionsResponse = result.getMessage();
    assertThat(getRegionsResponse.getRegionsCount()).isEqualTo(0);
    assertThat(getRegionsResponse.getRegionsList()).isEmpty();
  }
}
