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
package org.apache.geode.internal.protocol.protobuf.operations;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.apache.geode.internal.protocol.ProtobufTestExecutionContext.getNoAuthCacheExecutionContext;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufRequestUtilities;
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
  public void processReturnsCacheRegions() throws InvalidExecutionContextException {
    Result result = operationHandler.process(serializationServiceStub,
        ProtobufRequestUtilities.createGetRegionNamesRequest(),
        getNoAuthCacheExecutionContext(cacheStub));
    Assert.assertTrue(result instanceof Success);

    RegionAPI.GetRegionNamesResponse getRegionsResponse =
        (RegionAPI.GetRegionNamesResponse) result.getMessage();
    Assert.assertEquals(3, getRegionsResponse.getRegionsCount());

    // There's no guarantee for what order we receive the regions in from the response
    String name1 = getRegionsResponse.getRegions(0);
    String name2 = getRegionsResponse.getRegions(1);
    String name3 = getRegionsResponse.getRegions(2);
    Assert.assertTrue("The same region was returned multiple times",
        !name1.equals(name2) && !name1.equals(name3) && !name2.equals(name3));
    ArrayList arrayList = new ArrayList();
    arrayList.add(TEST_REGION1);
    arrayList.add(TEST_REGION2);
    arrayList.add(TEST_REGION3);

    assertTrue(arrayList.contains(name1));
    assertTrue(arrayList.contains(name2));
    assertTrue(arrayList.contains(name3));
  }

  @Test
  public void processReturnsNoCacheRegions() throws Exception {
    Cache emptyCache = mock(Cache.class);;
    when(emptyCache.rootRegions())
        .thenReturn(Collections.unmodifiableSet(new HashSet<Region<String, String>>()));
    Result result = operationHandler.process(serializationServiceStub,
        ProtobufRequestUtilities.createGetRegionNamesRequest(),
        getNoAuthCacheExecutionContext(emptyCache));
    Assert.assertTrue(result instanceof Success);

    RegionAPI.GetRegionNamesResponse getRegionsResponse =
        (RegionAPI.GetRegionNamesResponse) result.getMessage();
    Assert.assertEquals(0, getRegionsResponse.getRegionsCount());
  }
}
