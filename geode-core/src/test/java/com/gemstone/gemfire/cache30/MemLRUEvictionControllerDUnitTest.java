/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache30;

import java.util.Properties;
import java.util.Random;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.SharedLibrary;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;
import com.gemstone.gemfire.internal.cache.lru.MemLRUCapacityController;
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer;
import com.gemstone.gemfire.internal.size.WellKnownClassSizer;

/**
 * Tests the basic functionality of the memory lru eviction controller
 * and its statistics.
 * 
 * @author David Whitlock
 * 
 * @since 3.2
 */
public class MemLRUEvictionControllerDUnitTest extends CacheTestCase
{

  private static boolean usingMain = false;

  /**
   * Creates a new <code>MemLRUEvictionControllerDUnitTest</code>
   */
  public MemLRUEvictionControllerDUnitTest(String name) {
    super(name);
  }

  /**
   * Returns the <code>LRUStatistics</code> for the given region
   */
  private LRUStatistics getLRUStats(Region region)
  {
    final LocalRegion l = (LocalRegion)region;
    return l.getEvictionController().getLRUHelper().getStats();
  }

  private int getEntryOverhead(Region region)
  {
    LocalRegion lRegion = (LocalRegion)region;
    return ((MemLRUCapacityController)lRegion.getEvictionController())
        .getPerEntryOverhead();
  }

  // ////// Test Methods

  /**
   * Carefully verifies that region operations effect the {@link LRUStatistics}
   * as expected.
   */
  public void testRegionOperations() throws CacheException
  {

    int threshold = 4;

    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
        .createLRUMemoryAttributes(threshold));

    Region region;
    if (usingMain) {
      DistributedSystem system = DistributedSystem.connect(new Properties());
      Cache cache = CacheFactory.create(system);
      region = cache.createRegion("Test", factory.create());

    }
    else {
      region = createRegion(name, factory.create());
    }

    LRUStatistics lruStats = getLRUStats(region);
    assertNotNull(lruStats);

    String sampleKey = new String("10000");
    int stringSize = SharedLibrary.getObjectHeaderSize() //String object
      + (2*4) + SharedLibrary.getReferenceSize(); //2 ints and a reference on a string
    stringSize = (int) ReflectionSingleObjectSizer.roundUpSize(stringSize);

    int charArraySize = sampleKey.length() * 2
      + SharedLibrary.getObjectHeaderSize() //char array object
      + 4; //length of char array
    charArraySize = (int) ReflectionSingleObjectSizer.roundUpSize(charArraySize);
    assertEquals(stringSize, ReflectionSingleObjectSizer.sizeof(String.class));
    assertEquals(ReflectionSingleObjectSizer.roundUpSize(SharedLibrary.getObjectHeaderSize() + 4), (new ReflectionSingleObjectSizer()).sizeof(new char[0]));
    assertEquals(charArraySize, (new ReflectionSingleObjectSizer()).sizeof(new char[5]));
    
    int keySize = stringSize + charArraySize;
    assertEquals(keySize, WellKnownClassSizer.sizeof(sampleKey));
    assertEquals(keySize, ObjectSizer.DEFAULT.sizeof(sampleKey));
    // now that keys are inlined the keySize is 0
    keySize = 0;
    byte[] sampleValue =new byte[1000]; 
    int valueSize =sampleValue.length
      + SharedLibrary.getObjectHeaderSize() //byte array object;
      + 4; //length of byte array
    valueSize = (int) ReflectionSingleObjectSizer.roundUpSize(valueSize);
    int entrySize = keySize 
        + valueSize + getEntryOverhead(region);
    assertEquals(valueSize, ObjectSizer.DEFAULT.sizeof(sampleValue));

    for (int i = 1; i <= 100; i++) {
      Object key = String.valueOf(10000 + i);
      Object value = new byte[1000];
      region.put(key, value);
      assertEquals(i * entrySize, lruStats.getCounter());
      assertEquals(0, lruStats.getEvictions());
    }

    for (int i = 100; i >= 1; i--) {
      Object key = String.valueOf(10000 + i);
      region.destroy(key);
      assertEquals((i - 1) * entrySize, lruStats.getCounter());
      assertEquals(0, lruStats.getEvictions());
    }
  }
  
  /**
   * Make sure that we only size a class the first time we 
   * see the class instance.
   * @throws CacheException
   */
  public void testSizeClassesOnce() throws CacheException
  {
    int threshold = 4;

    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
        .createLRUMemoryAttributes(threshold));

    Region region = createRegion(name, factory.create());

    LRUStatistics lruStats = getLRUStats(region);
    assertNotNull(lruStats);

    TestObject object = new TestObject(50);
    // keySize is 0 because it is inlined
    int keySize = 0; //ObjectSizer.DEFAULT.sizeof(new String("10000"));
    int valueSize = ObjectSizer.DEFAULT.sizeof(object);
    int entrySize = keySize + valueSize + getEntryOverhead(region);

    Random ran = new Random();
    for (int i = 1; i <= 100; i++) {
      Object key = String.valueOf(10000 + i);
      //Use a randomly sized object.
      Object value = new TestObject(ran.nextInt(100));
      region.put(key, value);
      assertEquals(i * entrySize, lruStats.getCounter());
      assertEquals(0, lruStats.getEvictions());
    }

    for (int i = 100; i >= 1; i--) {
      Object key = String.valueOf(10000 + i);
      region.destroy(key);
      assertEquals((i - 1) * entrySize, lruStats.getCounter());
      assertEquals(0, lruStats.getEvictions());
    }
  }

  /**
   * Prints out the number of bytes that a region entry occupies in the VM.
   */
  public void testEntryOverHead() throws Exception
  {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
        .createLRUMemoryAttributes(50));

    Region region;
    region = createRegion(name, factory.create());

    String s = "Each entry occupies " + getEntryOverhead(region) + " bytes";
    region.getCache().getDistributedSystem().getLogWriter().info(s);
  }

  /** Class used in testCustomObjectSizer
   * 
   * @author mthomas
   * @since 5.0
   */
  class CustomObjectSizer implements ObjectSizer
  {
    private int dataSize;

    private boolean wasCalled = false;

    public CustomObjectSizer(int dataSize) {
      this.dataSize = dataSize;
    }

    public int sizeof(Object o) {
      this.wasCalled = true;
      return dataSize;
    }

    public boolean wasCalled() {
      return this.wasCalled;
    }
  }
  /**
   * Validate that a custom {@link ObjectSizer} is called, configured propertly,
   * and actually limits the size of the <code>Region</code>.
   * 
   * @throws Exception
   */
  public void testCustomObjectSizer() throws Exception
  {
    final String name = this.getUniqueName();
    final int entrySize = 1024 * 1024;
    final int numEntries = 3;
    AttributesFactory factory = new AttributesFactory();
    CustomObjectSizer cs = new CustomObjectSizer(entrySize);
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(1, cs));
    Region r = createRegion(name, factory.create());
    
    for (int size = 0; size < numEntries; size++) {
      // changed to a boolean[] because byte[] does not cause a call to ObjectSizer.sizeof
      // What was calling it before was the key object. But now that keys are inlined we
      // no longer call ObjectSizer.sizeof for the key.
      r.put(new Integer(size), new boolean[entrySize]);
    }
    assertTrue("ObjectSizer was not triggered", cs.wasCalled());
    long actualSize = getLRUStats(r).getCounter();
    int bytesPut = (entrySize * numEntries);
    assertTrue("Expected bytes put: " + bytesPut + " is not < "  + actualSize, actualSize < bytesPut);  
  }

  public static void main(String[] args) throws Exception
  {
    usingMain = true;
    (new MemLRUEvictionControllerDUnitTest("test")).testRegionOperations();
  }
  
  private static class TestObject {
    private final byte[] field;
    
    public TestObject(int size) {
      this.field = new byte[size];
    }
  }
}
