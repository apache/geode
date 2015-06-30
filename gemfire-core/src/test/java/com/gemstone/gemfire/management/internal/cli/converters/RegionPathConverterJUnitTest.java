/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.converters;

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

import com.gemstone.gemfire.test.junit.categories.UnitTest;

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
  
  protected RegionPathConverter createMockRegionPathConverter(final String[] allRegionPaths) {
    
    final RegionPathConverter mockRegionPathConverter = mockContext.mock(RegionPathConverter.class, "RPC");
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
    String[] allRegionPaths = { "/region1", "/region2", "/rg3"};
    TreeSet<String> expectedPaths = new TreeSet<String>(Arrays.asList(allRegionPaths));
    
    final RegionPathConverter mockRegionPathConverter = createMockRegionPathConverter(allRegionPaths);
    
    Set<String> mocked = mockRegionPathConverter.getAllRegionPaths();
    
    assertEquals("mocked paths don't match expectedPaths.", mocked, expectedPaths);
    
  }

}
