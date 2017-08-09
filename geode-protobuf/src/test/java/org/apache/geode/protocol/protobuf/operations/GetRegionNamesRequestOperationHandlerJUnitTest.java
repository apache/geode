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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GetRegionNamesRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private final String TEST_REGION1 = "test region 1";
  private final String TEST_REGION2 = "test region 2";
  private final String TEST_REGION3 = "test region 3";

  @Before
  public void setUp() throws Exception {
    super.setUp();

    Region<String, String> region1Stub = mock(Region.class);
    when(region1Stub.getName()).thenReturn(TEST_REGION1);
    Region<String, String> region2Stub = mock(Region.class);
    when(region2Stub.getName()).thenReturn(TEST_REGION2);
    Region<String, String> region3Stub = mock(Region.class);
    when(region3Stub.getName()).thenReturn(TEST_REGION3);

    when(cacheStub.rootRegions()).thenReturn(Collections
        .unmodifiableSet(new HashSet<>(Arrays.asList(region1Stub, region2Stub, region3Stub))));
    operationHandler = new GetRegionNamesRequestOperationHandler();
  }

  @Test
  public void processReturnsCacheRegions() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    Result<RegionAPI.GetRegionNamesResponse> result =
        operationHandler.process(serializationServiceStub,
            ProtobufRequestUtilities.createGetRegionNamesRequest(), cacheStub);
    Assert.assertTrue(result instanceof Success);

    RegionAPI.GetRegionNamesResponse getRegionsResponse = result.getMessage();
    Assert.assertEquals(3, getRegionsResponse.getRegionsCount());

    // There's no guarantee for what order we receive the regions in from the response
    String name1 = getRegionsResponse.getRegions(0);
    String name2 = getRegionsResponse.getRegions(1);
    String name3 = getRegionsResponse.getRegions(2);
    Assert.assertTrue("The same region was returned multiple times",
        name1 != name2 && name1 != name3 && name2 != name3);
    Assert.assertTrue(name1 == TEST_REGION1 || name1 == TEST_REGION2 || name1 == TEST_REGION3);
    Assert.assertTrue(name2 == TEST_REGION1 || name2 == TEST_REGION2 || name2 == TEST_REGION3);
    Assert.assertTrue(name3 == TEST_REGION1 || name3 == TEST_REGION2 || name3 == TEST_REGION3);
  }

  @Test
  public void processReturnsNoCacheRegions() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    Cache emptyCache = mock(Cache.class);;
    when(emptyCache.rootRegions())
        .thenReturn(Collections.unmodifiableSet(new HashSet<Region<String, String>>()));
    Result<RegionAPI.GetRegionNamesResponse> result =
        operationHandler.process(serializationServiceStub,
            ProtobufRequestUtilities.createGetRegionNamesRequest(), emptyCache);
    Assert.assertTrue(result instanceof Success);

    RegionAPI.GetRegionNamesResponse getRegionsResponse = result.getMessage();
    Assert.assertEquals(0, getRegionsResponse.getRegionsCount());
  }
}
