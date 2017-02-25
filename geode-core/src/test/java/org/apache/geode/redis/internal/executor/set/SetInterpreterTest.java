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
package org.apache.geode.redis.internal.executor.set;

import static org.junit.Assert.*;

import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test case for the Set interpreter test
 * 
 *
 */
@Category(UnitTest.class)
public class SetInterpreterTest {
  private ExecutionHandlerContext context;
  private RegionProvider regionProvider;

  @SuppressWarnings("rawtypes")
  private Region setRegion;


  /**
   * Setup the mock and test data
   */
  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {

    context = Mockito.mock(ExecutionHandlerContext.class);
    regionProvider = Mockito.mock(RegionProvider.class);
    setRegion = Mockito.mock(Region.class);

    Mockito.when(context.getRegionProvider()).thenReturn(regionProvider);

    Mockito.when(regionProvider.getSetRegion()).thenReturn(setRegion);

  }

  /**
   * Test the get region method
   */
  @Test
  public void testGetRegion() {
    Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region = SetInterpreter.getRegion(context);
    assertNotNull(region);

    assertEquals(setRegion, region);

  }

}
