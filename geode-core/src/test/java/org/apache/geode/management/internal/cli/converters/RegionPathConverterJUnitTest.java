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
package org.apache.geode.management.internal.cli.converters;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionPathConverterJUnitTest {

  private Mockery mockContext;

  @Before
  public void setup() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  private RegionPathConverter createMockRegionPathConverter(final String[] allRegionPaths) {

    final RegionPathConverter mockRegionPathConverter =
        mockContext.mock(RegionPathConverter.class, "RPC");
    mockContext.checking(new Expectations() {
      {
        oneOf(mockRegionPathConverter).getAllRegionPaths();
        will(returnValue(new TreeSet<String>(Arrays.asList(allRegionPaths))));
      }
    });

    return mockRegionPathConverter;
  }


  @Test
  public void testGetAllRegionPaths() throws Exception {
    String[] allRegionPaths = {"/region1", "/region2", "/rg3"};
    TreeSet<String> expectedPaths = new TreeSet<String>(Arrays.asList(allRegionPaths));

    final RegionPathConverter mockRegionPathConverter =
        createMockRegionPathConverter(allRegionPaths);

    Set<String> mocked = mockRegionPathConverter.getAllRegionPaths();

    assertEquals("mocked paths don't match expectedPaths.", mocked, expectedPaths);
  }

}
