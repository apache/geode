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
package org.apache.geode.protocol.protobuf.operations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.protocol.MessageUtil;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.Failure;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GetRegionRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private final String TEST_REGION1 = "test region 1";
  private Region region1Stub;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    region1Stub = mock(Region.class);
    when(region1Stub.getName()).thenReturn(TEST_REGION1);

    operationHandler = new GetRegionRequestOperationHandler();
  }

  @Test
  public void processReturnsCacheRegions() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {

    RegionAttributes regionAttributesStub = mock(RegionAttributes.class);
    when(cacheStub.getRegion(TEST_REGION1)).thenReturn(region1Stub);
    when(region1Stub.getName()).thenReturn(TEST_REGION1);
    when(region1Stub.size()).thenReturn(10);
    when(region1Stub.getAttributes()).thenReturn(regionAttributesStub);
    when(regionAttributesStub.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_REPLICATE);
    when(regionAttributesStub.getKeyConstraint()).thenReturn(String.class);
    when(regionAttributesStub.getValueConstraint()).thenReturn(Integer.class);
    when(regionAttributesStub.getScope()).thenReturn(Scope.DISTRIBUTED_ACK);


    Result<RegionAPI.GetRegionResponse> result = operationHandler.process(serializationServiceStub,
        MessageUtil.makeGetRegionRequest(TEST_REGION1), cacheStub);
    RegionAPI.GetRegionResponse response = result.getMessage();
    BasicTypes.Region region = response.getRegion();
    Assert.assertEquals(TEST_REGION1, region.getName());
    Assert.assertEquals(String.class.toString(), region.getKeyConstraint());
    Assert.assertEquals(Scope.DISTRIBUTED_ACK.toString(), region.getScope());
    Assert.assertEquals(DataPolicy.PERSISTENT_REPLICATE.toString(), region.getDataPolicy());
    Assert.assertEquals(Integer.class.toString(), region.getValueConstraint());
    Assert.assertEquals(true, region.getPersisted());
    Assert.assertEquals(10, region.getSize());
  }

  private ClientProtocol.Request createRequestMessage(RegionAPI.GetRegionRequest getRegionRequest) {
    return ClientProtocol.Request.newBuilder().setGetRegionRequest(getRegionRequest).build();
  }

  @Test
  public void processReturnsNoCacheRegions() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    Cache emptyCache = mock(Cache.class);
    when(emptyCache.rootRegions())
        .thenReturn(Collections.unmodifiableSet(new HashSet<Region<String, String>>()));
    String unknownRegionName = "UNKNOWN_REGION";
    Result<RegionAPI.GetRegionResponse> result = operationHandler.process(serializationServiceStub,
        MessageUtil.makeGetRegionRequest(unknownRegionName), emptyCache);
    Assert.assertTrue(result instanceof Failure);
    Assert.assertEquals("No region exists for name: " + unknownRegionName,
        result.getErrorMessage().getMessage());
  }
}
