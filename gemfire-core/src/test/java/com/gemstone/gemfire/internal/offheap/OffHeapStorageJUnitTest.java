package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OffHeapStorageJUnitTest {

  private final static long MEGABYTE = 1024 * 1024;
  private final static long GIGABYTE = 1024 * 1024 * 1024;

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testParseOffHeapMemorySizeNegative() {
    assertEquals(0, OffHeapStorage.parseOffHeapMemorySize("-1"));
  }
  @Test
  public void testParseOffHeapMemorySizeNull() {
    assertEquals(0, OffHeapStorage.parseOffHeapMemorySize(null));
  }
  @Test
  public void testParseOffHeapMemorySizeEmpty() {
    assertEquals(0, OffHeapStorage.parseOffHeapMemorySize(""));
  }
  @Test
  public void testParseOffHeapMemorySizeBytes() {
    assertEquals(MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("1"));
    assertEquals(Integer.MAX_VALUE * MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("" + Integer.MAX_VALUE));
  }
  @Test
  public void testParseOffHeapMemorySizeKiloBytes() {
    try {
      OffHeapStorage.parseOffHeapMemorySize("1k");
      fail("Did not receive expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
  }
  @Test
  public void testParseOffHeapMemorySizeMegaBytes() {
    assertEquals(MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("1m"));
    assertEquals(Integer.MAX_VALUE * MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("" + Integer.MAX_VALUE + "m"));
  }
  @Test
  public void testParseOffHeapMemorySizeGigaBytes() {
    assertEquals(GIGABYTE, OffHeapStorage.parseOffHeapMemorySize("1g"));
    assertEquals(Integer.MAX_VALUE * GIGABYTE, OffHeapStorage.parseOffHeapMemorySize("" + Integer.MAX_VALUE + "g"));
  }
}
