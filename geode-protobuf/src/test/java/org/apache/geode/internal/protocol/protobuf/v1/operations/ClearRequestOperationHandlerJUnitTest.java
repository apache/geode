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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.protocol.TestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ClearRequestOperationHandlerJUnitTest
    extends OperationHandlerJUnitTest<RegionAPI.ClearRequest, RegionAPI.ClearResponse> {
  private final String TEST_REGION = "test region";
  private final String MISSING_REGION = "missing region";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    @SuppressWarnings("unchecked")
    Region<Object, Object> regionStub = mock(Region.class);

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
    when(cacheStub.getRegion(MISSING_REGION)).thenReturn(null);
    operationHandler = new ClearRequestOperationHandler();
  }

  @Test
  public void processReturnsSuccessForValidRegion() throws Exception {
    RegionAPI.ClearRequest removeRequest =
        ProtobufRequestUtilities.createClearRequest(TEST_REGION).getClearRequest();
    Result<?> result = operationHandler.process(serializationService, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));
    assertThat(result).isInstanceOf(Success.class);
  }

  @Test
  public void processReturnsFailureForInvalidRegion() throws Exception {
    RegionAPI.ClearRequest removeRequest =
        ProtobufRequestUtilities.createClearRequest(MISSING_REGION).getClearRequest();
    assertThatThrownBy(() -> operationHandler.process(serializationService, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub)))
            .isInstanceOf(RegionDestroyedException.class);
  }
}
