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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class GetRegionNamesRequestOperationHandlerJUnitTest {
  public static final String TEST_REGION1 = "test region 1";
  public static final String TEST_REGION2 = "test region 2";
  public static final String TEST_REGION3 = "test region 3";
  public Cache cacheStub;
  public SerializationService serializationServiceStub;
  private GetRegionNamesRequestOperationHandler operationHandler;

  @Before
  public void setUp() throws Exception {
    serializationServiceStub = mock(SerializationService.class);
    when(serializationServiceStub.encode(BasicTypes.EncodingType.STRING, TEST_REGION1))
        .thenReturn(TEST_REGION1.getBytes(Charset.forName("UTF-8")));
    when(serializationServiceStub.encode(BasicTypes.EncodingType.STRING, TEST_REGION2))
        .thenReturn(TEST_REGION2.getBytes(Charset.forName("UTF-8")));
    when(serializationServiceStub.encode(BasicTypes.EncodingType.STRING, TEST_REGION3))
        .thenReturn(TEST_REGION3.getBytes(Charset.forName("UTF-8")));

    Region<String, String> region1Stub = mock(Region.class);
    when(region1Stub.getName()).thenReturn(TEST_REGION1);
    Region<String, String> region2Stub = mock(Region.class);
    when(region2Stub.getName()).thenReturn(TEST_REGION2);
    Region<String, String> region3Stub = mock(Region.class);
    when(region3Stub.getName()).thenReturn(TEST_REGION3);

    cacheStub = mock(Cache.class);
    when(cacheStub.rootRegions()).thenReturn(Collections.unmodifiableSet(
        new HashSet<Region<String, String>>(Arrays.asList(region1Stub, region2Stub, region3Stub))));
    operationHandler = new GetRegionNamesRequestOperationHandler();
  }

  @Test
  public void processReturnsCacheRegions() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    ClientProtocol.Response response = operationHandler.process(serializationServiceStub,
        ProtobufRequestUtilities.createGetRegionNamesRequest(), cacheStub);
    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.GETREGIONNAMESRESPONSE,
        response.getResponseAPICase());

    RegionAPI.GetRegionNamesResponse getRegionsResponse = response.getGetRegionNamesResponse();
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
    ClientProtocol.Response response = operationHandler.process(serializationServiceStub,
        ProtobufRequestUtilities.createGetRegionNamesRequest(), emptyCache);
    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.GETREGIONNAMESRESPONSE,
        response.getResponseAPICase());

    RegionAPI.GetRegionNamesResponse getRegionsResponse = response.getGetRegionNamesResponse();
    Assert.assertEquals(0, getRegionsResponse.getRegionsCount());
  }
}
