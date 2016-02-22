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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheStatistics;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionReinitializedException;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.EntryExpiryTask;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.ExpiryTask;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

//import com.gemstone.gemfire.internal.util.DebuggerSupport;

// @todo davidw Test {@link CacheStatistics}
/**
 * An abstract class whose test methods test the functionality of a
 * region regardless of its scope.
 *
 * <P>
 *
 * This class also contains functionality that is used by subclasses.
 * See {@link #getRegionAttributes}.
 *
 *
 * @author David Whitlock
 * @since 3.0
 */
public abstract class RegionTestCase extends CacheTestCase {
  
  /** A <code>CacheListener</code> used by a test */
  static TestCacheListener listener;
  static TestCacheListener subrgnListener;
  
  /** A <code>CacheLoader</code> used by a test */
  static TestCacheLoader loader;
  static TestCacheLoader subrgnLoader;
  
  /** A <code>CacheWriter</code> used by a test */
  static TestCacheWriter writer;
  static TestCacheWriter subrgnWriter;
  
  /**
   * Clears fields used by a test
   */
  private static void cleanup() {
    listener = null;
    loader = null;
    writer = null;
    subrgnListener = null;
    subrgnLoader = null;
    subrgnWriter = null;
  }
  
  public RegionTestCase(String name) {
    super(name);
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    cleanup();
    Invoke.invokeInEveryVM(getClass(), "cleanup");
    postTearDownRegionTestCase();
  }
  
  protected void postTearDownRegionTestCase() throws Exception {
  }
  
  ////////  Helper methods
  
  /**
   * Returns a region with the given name and the attributes for this
   * test.
   *
   * @see #getRegionAttributes
   */
  protected final Region createRegion(String name)
  throws CacheException {
    
    return createRegion(name, getRegionAttributes());
  }

  protected final Region createRootRegion()
  throws CacheException {
    return createRootRegion(getRegionAttributes());
  }
  
  /**
   * Returns the attributes of a region to be tested by this test.
   * Note that the decision as to which attributes are used is left up
   * to the concrete subclass.
   */
  protected abstract RegionAttributes getRegionAttributes();
  
  
  /** pauses only if no ack */
  protected void pauseIfNecessary() {
  }
  
  protected void pauseIfNecessary(int ms) {
  }
  
  /**
   * Make sure all messages done on region r have
   * been processed on the remote side.
   */
  protected void flushIfNecessary(Region r) {
    // Only needed for no-ack regions
  }
  
  //////////////////////  Test Methods  //////////////////////
  
  /**
   * Tests that creating an entry in a region actually creates it
   *
   * @see Region#containsKey
   * @see Region#containsValueForKey
   */
  public void testContainsKey() throws CacheException {
    String name = this.getUniqueName();
    Region region = createRegion(name);
    Object key = name;
    
    Object value = new Integer(42);
    
    assertFalse(region.containsKey(key));
    region.create(key, null);
    assertFalse(region.containsValueForKey(key));
    
    Region.Entry entry = region.getEntry(key);
    assertNotNull(entry);
    assertEquals(entry.getKey(), key);
    assertNull(entry.getValue());
    
    region.put(key, value);
    assertTrue(region.containsValueForKey(key));
    assertEquals(entry, region.getEntry(key));
    if (entry.isLocal()) {
      assertEquals(value, entry.getValue());
    } else {
      assertEquals(value, region.getEntry(key).getValue());
    }
  }
  
  /**
   * Tests that creating or getting entries at an improper time throws
   * exceptions.
   *
   * @see Region#get
   * @see Region#getEntry
   * @see Region#create
   */
  public void testBadRegionAccess() throws CacheException {
    String name = this.getUniqueName();
    Region region = createRegion(name);
    Object key = name;
    
    assertNull(region.get(key));
    assertNull(region.getEntry(key));
    
    Integer value = new Integer(42);
    region.create(key, value);
    
    try {
      // partitioned regions are logging the EntryExistsException, so emit
      // a directive to ignore it
      region.getCache().getLogger().info("<ExpectedException action=add>"
          + "com.gemstone.gemfire.cache.EntryExistsException"
          + "</ExpectedException>");
      region.create(key, value);
      fail("Should have thrown an EntryExistsException");
      
    } catch (EntryExistsException ex) {
      // okay...
    }
    finally {
      region.getCache().getLogger().info("<ExpectedException action=remove>"
          + "com.gemstone.gemfire.cache.EntryExistsException"
          + "</ExpectedException>");
    }
  }

  /**
   * Tests that {@link Region#put} on a previously non-existent region
   * entry creates it.
   */
  public void testPutNonExistentEntry() throws CacheException {
    String name = this.getUniqueName();
    Region region = createRegion(name);
    Object key = name;
    
    assertNull(region.getEntry(key));
    
    Object value = new Integer(42);
    region.put(key, value);
    
    Region.Entry entry = region.getEntry(key);
    assertNotNull(entry);
    assertEquals(key, entry.getKey());
    assertEquals(value, entry.getValue());
    assertEquals(value, region.get(key));
    
    try {
      Collection values = region.values();
      assertEquals(1, values.size());
      assertEquals(value, values.iterator().next());
    }
    catch (UnsupportedOperationException uoe) {
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Region.values() reported UnsupportedOperation");
    }
  }
  
  /**
   * Indicate whether subregions are supported
   * @return
   */
  protected boolean supportsSubregions() {
    return true;
  }
  
  /**
   * Indicate whether localDestroy and localInvalidate are supported
   * @return true if they are supported
   */
  protected boolean supportsLocalDestroyAndLocalInvalidate() {
    return true;  
  }
  
  /**
   * Tests that sending <code>null</code> to various APIs throws the
   * appropriate exception.
   */
  public void testNulls() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    Region region = createRegion(name);
    Object key = name;
    
    try {
      region.getSubregion(null);
      fail("Should have thrown an IllegalArgumentException");
      
    } catch (IllegalArgumentException ex) {
      // pass..
    }
    
    try {
      region.createSubregion(null, region.getAttributes());
      fail("Should have thrown an IllegalArgumentException");
      
    } catch (IllegalArgumentException ex) {
      // pass..
    }
    
    try {
      region.createSubregion("TEST", null);
      fail("Should have thrown an IllegalArgumentException");
      
    } catch (IllegalArgumentException ex) {
      // pass..
    }
    
    assertEquals("/", Region.SEPARATOR);
    assertEquals('/', Region.SEPARATOR_CHAR);
    try {
      region.createSubregion("BAD/TEST", region.getAttributes());
      fail("Should have thrown an IllegalArgumentException");
      
    } catch (IllegalArgumentException ex) {
      // pass..
    }
    
    try {
      region.createSubregion("", region.getAttributes());
      fail("Should have thrown an IllegalArgumentException");
      
    } catch (IllegalArgumentException ex) {
      // pass..
    }
    
    try {
      region.getEntry(null);
      fail("Should have thrown a NullPointerException");
      
    } catch (NullPointerException ex) {
      // pass..
    }
    
    try {
      region.get(null);
      fail("Should have thrown a NullPointerException");
      
    } catch (NullPointerException ex) {
      // pass..
    }
    
    try {
      region.get(null, null);
      fail("Should have thrown a NullPointerException");
      
    } catch (NullPointerException ex) {
      // pass..
    }
    
    try {
      region.put(null, new Integer(42));
      fail("Should have thrown a NullPointerException");
      
    } catch (NullPointerException ex) {
      // pass..
    }
    
    try {
      region.put(key, null);
      fail("Should have thrown a NullPointerException");
      
    } catch (NullPointerException ex) {
      // pass..
    }
    
    try {
      region.destroy(null);
      fail("Should have thrown a NullPointerException");
      
    } catch (NullPointerException ex) {
      // pass..
    }
  }
  
  /**
   * Tests creating subregions.  Note that this tests accesses the
   * Region's {@link Region#getStatistics statistics}, so the region
   * must have been created with statistics enabled.
   */
  public void testCreateSubregions() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    
    RegionAttributes attrs = getRegionAttributes();
    AttributesFactory factory = new AttributesFactory(attrs);
    factory.setStatisticsEnabled(true);
    attrs = factory.create();
    
    Region region = createRegion(name, attrs);
//    Object key = name;
    
    attrs = region.getAttributes();
    
    CacheStatistics stats = region.getStatistics();
    long lastAccessed = stats.getLastAccessedTime();
    long lastModified = stats.getLastModifiedTime();
    
    try {
      region.createSubregion(name + "/BAD", attrs);
      fail("Should have thrown an IllegalArgumentException");
      
    } catch (IllegalArgumentException ex) {
      CacheStatistics stats2 = region.getStatistics();
      assertEquals(lastAccessed, stats2.getLastAccessedTime());
      assertEquals(lastModified, stats2.getLastModifiedTime());
    }
    
    Region subregion = region.createSubregion(name, attrs);
    assertTrue(attrs != subregion.getAttributes());
    /* @todo compare each individual attribute for equality?
    assertEquals(attrs, subregion.getAttributes());
     */
    
    Set subregions = region.subregions(false);
    assertEquals(1, subregions.size());
    assertEquals(subregion, subregions.iterator().next());
  }

  static public boolean entryIsLocal(Region.Entry re) {
    if (re instanceof EntrySnapshot) {
      return ((EntrySnapshot)re).wasInitiallyLocal();
    } else {
      return re.isLocal();
    }
  }
  
  /**
   * Tests {@link Region#destroy destroying} an entry and attempting
   * to access it afterwards.
   */
  public void testDestroyEntry() throws CacheException {
    String name = this.getUniqueName();
    Object key = name;
    Object value = new Integer(42);
    
    Region region = createRegion(name);
    
    try {
      region.destroy(key);
      fail("Should have thrown an EntryNotFoundException");
      
    } catch (EntryNotFoundException ex) {
      // pass...
    }
    
    region.put(key, value);
    
    Region.Entry entry = region.getEntry(key);
    assertNotNull(entry);
    
    region.destroy(key);
    
    Region.Entry entry2 = region.getEntry(key);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Found entry for destroyed key: " + entry2);
    assertNull(entry2);
    if (entry.isLocal()) {
      assertTrue(entry.isDestroyed());
    } else {
      assertFalse(entry.isDestroyed());
    }
    assertEquals(0, region.keySet().size());
    
    if (entry.isLocal()) {
      try {
        entry.getKey();
        fail("Should have thrown an EntryDestroyedException");
      }
      catch (EntryDestroyedException ex) {
        // pass...
      }
      try {
        entry.getRegion();
        fail("Should have thrown an EntryDestroyedException");
        
      } catch (EntryDestroyedException ex) {
        // pass...
      }
      
      try {
        entry.getStatistics();
        fail("Should have thrown an EntryDestroyedException");
        
      } catch (EntryDestroyedException ex) {
        // pass...
      }
      
      try {
        entry.getUserAttribute();
        fail("Should have thrown an EntryDestroyedException");
        
      } catch (EntryDestroyedException ex) {
        // pass...
      }
      
      try {
        entry.setUserAttribute("blah");
        fail("Should have thrown an EntryDestroyedException");
        
      } catch (EntryDestroyedException ex) {
        // pass...
      }
      
      try {
        entry.getValue();
        fail("Should have thrown an EntryDestroyedException");
        
      } catch (EntryDestroyedException ex) {
        // pass...
      }
    }
  }
  
  /**
   * Tests destroying an entire region and that accessing it after it
   * has been destory causes a {@link RegionDestroyedException}.
   *
   * @see Region#destroyRegion
   */
  public void testDestroyRegion() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";
    
    Region region = createRegion(name);
    region.put(key, value);
    
    Region.Entry entry = region.getEntry(key);
    assertNotNull(entry);
    
    region.createSubregion("SUB", region.getAttributes());
    region.destroyRegion();
    
    assertTrue(entry.isDestroyed());
    assertTrue(region.isDestroyed());
    
    try {
      region.containsKey(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.containsValueForKey(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.create(key, value);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.create(key, value, "BLAH");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.createSubregion("SUB", this.getRegionAttributes());
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.destroy(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.destroy(key, "BLAH");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.destroyRegion();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.destroyRegion("ARG");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.entries(false);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.get(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.get(key, "ARG");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.containsKey(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    region.getAttributes();
    
    try {
      region.getAttributesMutator();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.containsKey(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.getCache();
      
    } catch (RegionDestroyedException ex) {
      fail("getCache() shouldn't have thrown a RegionDestroyedException");
    }
    
    try {
      region.getEntry(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.getDistributedLock(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    assertEquals(name, region.getName());
    
    
    region.getParentRegion();
    
    assertEquals("/root/" + name, region.getFullPath());
    assertEquals(name, region.getName());
    
    try {
      region.getRegionDistributedLock();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.getStatistics();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.getSubregion("SUB");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    region.getUserAttribute();
    
    try {
      region.invalidate(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.invalidateRegion();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.keys();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.localDestroy(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.localDestroyRegion();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.localInvalidate(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.localInvalidateRegion();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.put(key, value);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.put(key, value, "ARG");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.setUserAttribute("ATTR");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.subregions(true);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.values();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
  }
  
  /**
   * Tests the {@link Region#entries} method without recursion
   */
  public void testEntries() throws CacheException {
    String name = this.getUniqueName();
    Region region = createRegion(name);
    assertEquals(0, region.entries(true).size());
    assertEquals(0, region.entries(false).size());
    
    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");
    
    {
      Set entries = region.entries(false);
      assertEquals(3, entries.size());
      
      Set keys = new HashSet(Arrays.asList(new String[] {"A", "B", "C"}));
      Iterator iter = entries.iterator();
      for (int i = 0; i < 3; i++) {
        assertTrue(iter.hasNext());
        assertTrue(keys.remove(((Region.Entry)iter.next()).getKey()));
      }
      assertFalse(iter.hasNext());
    }
    
    {
      Set entries = region.entries(true);
      assertEquals(3, entries.size());
      
      Set keys = new HashSet(Arrays.asList(new String[] {"A", "B", "C"}));
      Iterator iter = entries.iterator();
      for (int i = 0; i < 3; i++) {
        assertTrue(iter.hasNext());
        assertTrue(keys.remove(((Region.Entry) iter.next()).getKey()));
      }
      assertFalse(iter.hasNext());
    }
/* Not with ConcurrentHashMaps
    {
      Iterator iter = region.entries(false).iterator();
      iter.next();
      region.destroy("B");
 
      try {
        iter.next();
        fail("Should have thrown a ConcurrentModificationException");
 
      } catch (ConcurrentModificationException ex) {
        // pass...
      }
    }
 
    {
      Iterator iter = region.entries(false).iterator();
      iter.next();
      region.put("D", "d");
 
      try {
        iter.next();
        fail("Should have thrown a ConcurrentModificationException");
 
      } catch (ConcurrentModificationException ex) {
        // pass...
      }
    }
 */
    {
      Iterator iter = region.entries(false).iterator();
      Region.Entry entry = (Region.Entry) iter.next();
      region.destroy(entry.getKey());
      if (entry.isLocal()) {
        assertTrue(entry.isDestroyed());
      } else {
        assertFalse(entry.isDestroyed());
      }
    }
    
  }
  
  /**
   * Tests the {@link Region#entries} method with recursion
   */
  public void testEntriesRecursive() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    Region region = createRegion(name);
    
    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");
    
    Region sub =
            region.createSubregion("SUB", region.getAttributes());
    sub.put("D", "d");
    sub.put("E", "e");
    sub.put("F", "f");
    
    {
      Set entries = region.entries(true);
      assertEquals(6, entries.size());
      
      
      Set keys = new HashSet(Arrays.asList(new String[] {"A", "B", "C", "D", "E", "F"}));
      Iterator iter = entries.iterator();
      for (int i = 0; i < 6; i++) {
        assertTrue("!hasNext, i=" + i,
                iter.hasNext());
        assertTrue("remove returned false, i=" + i,
                keys.remove(((Region.Entry) iter.next()).getKey()));
      }
      assertFalse(iter.hasNext());
    }
    
    {
      Set entries = getRootRegion().entries(true);
      assertEquals(6, entries.size());
      
      
      Set keys = new HashSet(Arrays.asList(new String[] {"A", "B", "C", "D", "E", "F"}));
      Iterator iter = entries.iterator();
      for (int i = 0; i < 6; i++) {
        assertTrue("!hasNext, i=" + i,
                iter.hasNext());
        assertTrue("remove returned false, i=" + i,
                keys.remove(((Region.Entry) iter.next()).getKey()));
      }
      assertFalse(iter.hasNext());
    }
    
    {
      Iterator iter = region.entries(true).iterator();
      Region.Entry entry = (Region.Entry) iter.next();
      Object ekey = entry.getKey();
      region.destroy(ekey);
      assertEquals(false, region.containsKey(ekey));
      assertTrue(entry.isDestroyed());
    }
    
  }
  
  
  /**
   * Tests the {@link Region#getCache} method (for what it's worth)
   */
  public void testGetCache() throws CacheException {
    String name = this.getUniqueName();
    Region region = createRegion(name);
    assertSame(this.getCache(), region.getCache());
  }
  
  /**
   * Tests the {@link Region#getName} method
   */
  public void testGetName() throws CacheException {
    String name = this.getUniqueName();
    Region region = createRegion(name);
    assertEquals(name, region.getName());
    
    assertEquals("root", region.getParentRegion().getName());
  }
  
  /**
   * Tests the {@link Region#getFullPath} method
   */
  public void testGetPathFromRoot() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    
    Region region = createRegion(name);
    String fullPath = "/root/" + name;
    assertEquals(fullPath, region.getFullPath());
    assertEquals("/root", region.getParentRegion().getFullPath());
    
    Region sub =
            region.createSubregion("SUB", region.getAttributes());
    assertEquals(fullPath + "/SUB", sub.getFullPath());
  }
  
  /**
   * Tests the {@link Region#getParentRegion} method
   */
  public void testGetParentRegion() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    
    Region region = createRegion(name);
    assertEquals(getRootRegion(),
            region.getParentRegion());
    
    Region sub =
            region.createSubregion("SUB", region.getAttributes());
    assertEquals(region, sub.getParentRegion());
    assertSame(sub, region.getSubregion("SUB"));
    assertNotNull(sub.getAttributes());
  }
  
  /**
   * Tests a <code>Region</code>'s user attribute
   *
   * @see Region#setUserAttribute
   */
  public void testRegionUserAttribute() throws CacheException {
    String name = this.getUniqueName();
    Object value = "USER_ATTRIBUTE";
    
    Region region = createRegion(name);
    assertNull(region.getUserAttribute());
    
    region.setUserAttribute(value);
    assertEquals(value, region.getUserAttribute());
  }
  
  /**
   * Tests a region entry's user attribute
   */
  public void testEntryUserAttribute() throws CacheException {
    String name = this.getUniqueName();
    String key = "KEY";
    
    String attr = "USER_ATTRIBUTE";
    
    Region region = createRegion(name);
    region.create(key, null);
    
    Region.Entry entry = region.getEntry(key);
    entry.setUserAttribute(attr);
    assertEquals(attr, entry.getUserAttribute());
    
    entry = region.getEntry(key);
    assertEquals(attr, entry.getUserAttribute());
  }
  
  /**
   * Tests invalidating a region entry
   */
  public void testInvalidateEntry() throws CacheException {
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";
    
    Region region = createRegion(name);
    region.put(key, value);
    
    int beforeInvalidates =
            ((com.gemstone.gemfire.internal.cache.GemFireCacheImpl)getCache()).
            getCachePerfStats().getInvalidates();
    Region.Entry entry = region.getEntry(key);
    region.invalidate(key);
    if (entry.isLocal()) {
      assertNull(entry.getValue());
    }
    assertNull(region.get(key));
    int afterInvalidates =
              ((com.gemstone.gemfire.internal.cache.GemFireCacheImpl)getCache()).
              getCachePerfStats().getInvalidates();
    assertEquals("Invalidate CachePerfStats incorrect",
            beforeInvalidates + 1, afterInvalidates);
  }
  
  /**
   * Tests invalidating an entire region
   */
  public void testInvalidateRegion() throws CacheException {
    String name = this.getUniqueName();
    
    Region region = createRegion(name);
    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");
    
    for (int i = 0; i < 50; i++) {
      region.put("Key=" + i, "Value-" + i);
    }
    
    region.invalidateRegion();
    
    Region.Entry entry;
    entry = region.getEntry("A");
    assertNotNull(entry);
    assertNull(entry.getValue());
    
    entry = region.getEntry("B");
    assertNotNull(entry);
    assertNull(entry.getValue());
    
    entry = region.getEntry("C");
    assertNotNull(entry);
    assertNull(entry.getValue());
    
    for (int i = 0; i < 50; i++) {
      String key = "Key=" + i;
      assertFalse("containsValueForKey returned true for key " + key, region.containsValueForKey(key));
      assertTrue("containsKey returned false for key " + key, region.containsKey(key));
    }
  }
  
  /**
   * Tests the {@link Region#keys} method.
   */
  public void testKeys() throws CacheException {
    String name = this.getUniqueName();
    
    Region region = createRegion(name);
    assertEquals(0, region.keys().size());
    
    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");
    
    {
      Set keys = region.keys();
      assertEquals(3, keys.size());
      
      assertTrue(keys.contains("A"));
      assertTrue(keys.contains("B"));
      assertTrue(keys.contains("C"));
    }
    
    /* not with ConcurrentHashMap
    {
      Iterator iter = region.keys().iterator();
      iter.next();
      region.destroy("B");
     
      try {
        iter.next();
        fail("Should have thrown a ConcurrentModificationException");
     
      } catch (ConcurrentModificationException ex) {
        // pass...
      }
    }
     
    {
      Iterator iter = region.keys().iterator();
      iter.next();
      region.put("D", "d");
     
      try {
        iter.next();
        fail("Should have thrown a ConcurrentModificationException");
     
      } catch (ConcurrentModificationException ex) {
        // pass...
      }
    }
     */
  }
  
  /**
   * Tests {@link Region#localDestroy locally destroying} an entry and
   * attempting to access it afterwards.  (Not too useful with a
   * <code>LOCAL</code> region.)
   */
  public void testLocalDestroyEntry() throws CacheException {
    if (!supportsLocalDestroyAndLocalInvalidate()) {
      return;
    }
    String name = this.getUniqueName();
    Object key = name;
    Object value = new Integer(42);
    
    Region region = createRegion(name);
    
    boolean isMirrored = getRegionAttributes().getMirrorType().isMirrored();
    
    try {
      region.localDestroy(key);
      if (isMirrored) fail("Should have thrown an IllegalStateException");
      fail("Should have thrown an EntryNotFoundException");
    } catch (EntryNotFoundException ex) {
      // pass...
    } catch (IllegalStateException ex) {
      if (!isMirrored)
        throw ex;
      else
        return; // abort test
    }
    
    region.put(key, value);
    
    Region.Entry entry = region.getEntry(key);
    assertNotNull(entry);
    
    region.localDestroy(key);
    
    assertNull(region.getEntry(key));
    assertTrue(entry.isDestroyed());
    assertEquals(0, region.keys().size());
    
    try {
      entry.getKey();
      fail("Should have thrown an EntryDestroyedException");
      
    } catch (EntryDestroyedException ex) {
      // pass...
    }
    
    try {
      entry.getRegion();
      fail("Should have thrown an EntryDestroyedException");
      
    } catch (EntryDestroyedException ex) {
      // pass...
    }
    
    try {
      entry.getStatistics();
      fail("Should have thrown an EntryDestroyedException");
      
    } catch (EntryDestroyedException ex) {
      // pass...
    }
    
    try {
      entry.getUserAttribute();
      fail("Should have thrown an EntryDestroyedException");
      
    } catch (EntryDestroyedException ex) {
      // pass...
    }
    
    try {
      entry.setUserAttribute("blah");
      fail("Should have thrown an EntryDestroyedException");
      
    } catch (EntryDestroyedException ex) {
      // pass...
    }
    
    try {
      entry.getValue();
      fail("Should have thrown an EntryDestroyedException");
      
    } catch (EntryDestroyedException ex) {
      // pass...
    }
  }
  
  /**
   * Tests locally destroying an entire region and that accessing it
   * after it has been destory causes a {@link
   * RegionDestroyedException}.
   *
   * @see Region#localDestroyRegion
   */
  public void testLocalDestroyRegion() throws CacheException {
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";
    
    Region region = createRegion(name);
    region.put(key, value);
    
    Region.Entry entry = region.getEntry(key);
    assertNotNull(entry);
    
    region.createSubregion("SUB", region.getAttributes());
    region.localDestroyRegion();
    
    assertTrue(entry.isDestroyed());
    assertTrue(region.isDestroyed());
    
    try {
      region.containsKey(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.containsValueForKey(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.create(key, value);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.create(key, value, "BLAH");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.createSubregion("SUB", this.getRegionAttributes());
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.destroy(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.destroy(key, "BLAH");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.destroyRegion();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.destroyRegion("ARG");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.entries(false);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.get(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.get(key, "ARG");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.containsKey(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    region.getAttributes();
    
    try {
      region.getAttributesMutator();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.containsKey(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.getCache();
      
    } catch (RegionDestroyedException ex) {
      fail("getCache() shouldn't have thrown a RegionDestroyedException");
    }
    
    try {
      region.getEntry(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.getDistributedLock(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    assertEquals(name, region.getName());
    
    
    region.getParentRegion();
    
    assertEquals("/root/" + name, region.getFullPath());
    
    try {
      region.getRegionDistributedLock();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.getStatistics();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.getSubregion("SUB");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    region.getUserAttribute();
    
    try {
      region.invalidate(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.invalidateRegion();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.keys();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.localDestroy(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.localDestroyRegion();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.localInvalidate(key);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.localInvalidateRegion();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.put(key, value);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.put(key, value, "ARG");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.setUserAttribute("ATTR");
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.subregions(true);
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
    
    try {
      region.values();
      fail("Should have thrown a RegionDestroyedException");
      
    } catch (RegionDestroyedException ex) {
      // pass..
    }
  }
  
  /**
   * Tests closing a region, and checks different behavior when this is a disk
   * region with persistBackup.
   */
  public void testCloseRegion() throws CacheException {
    // @todo added a remote region to make sure close just does a localDestroy
    
    String name = this.getUniqueName();
            
    AttributesFactory fac = new AttributesFactory(getRegionAttributes());
    
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent event) {
        // do nothing
      }
      public void afterRegionDestroy2(RegionEvent re) {
        assertEquals(Operation.REGION_CLOSE, re.getOperation());
      }
      public void close2() {
        // okay
      }
    };
    
    fac.setCacheListener(list);
    
    RegionAttributes attrs = fac.create();
    Region region = createRegion(name, attrs);    
    
    File diskDir = null;
    if (attrs.getDataPolicy().withPersistence()) {
      diskDir = getCache().findDiskStore(attrs.getDiskStoreName()).getDiskDirs()[0];
      // @todo We no longer start with a clean slate because the DiskStore hangs around.
      // If we want a clean slate then we need to destroy the DiskStore after each
      // test completes.
      // assert that if this is a disk region, the disk dirs are empty
      // to make sure we start with a clean slate
      getCache().getLogger().info("list="+Arrays.toString(diskDir.list()));
//       assertEquals("list="+Arrays.toString(diskDir.list()),
//                    0, diskDir.list().length);
    }
    
    for (int i = 0; i < 1000; i++) {
      region.put(new Integer(i), String.valueOf(i));
    }
    
    // reset wasInvoked after creates
    assertTrue(list.wasInvoked());
    
    // assert that if this is a disk region, the disk dirs are not empty
    if (attrs.getDataPolicy().withPersistence()) {
      assertTrue(diskDir.list().length > 0);
    }
    boolean persistent = region.getAttributes().getDataPolicy().withPersistence();
    region.close();
    
    // assert that if this is a disk region, the disk dirs are not empty
    if (attrs.getDataPolicy().withPersistence()) {
      assertTrue(diskDir.list().length > 0);
    }
    
    assertTrue(list.waitForInvocation(333));
    assertTrue(list.isClosed());
    assertTrue(region.isDestroyed());
    
//    if (persistent) {
//      // remove this when bug #41049 is fixed
//      return;
//    }
    
    // if this is a disk region, then check to see if recreating the region
    // repopulates with data
    
    region = createRegion(name, attrs);
    
    if (attrs.getDataPolicy().withPersistence()) {
      for (int i = 0; i < 1000; i++) {
        Region.Entry entry = region.getEntry(new Integer(i));
        assertNotNull("entry " + i + " not found", entry);
        assertEquals(String.valueOf(i), entry.getValue());
      }
      assertEquals(1000, region.keys().size());
    }
    else {
      assertEquals(0, region.keys().size());
    }
    
    region.localDestroyRegion();
  }
  

  /**
   * Tests locally invalidating a region entry
   */
  public void testLocalInvalidateEntry() throws CacheException {
    if (!supportsLocalDestroyAndLocalInvalidate()) {
      return;
    }
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";
    
    Region region = createRegion(name);
    region.put(key, value);
    
    Region.Entry entry = region.getEntry(key);
    boolean isMirrorKeysValues = getRegionAttributes().getMirrorType().isKeysValues();
    try {
      region.localInvalidate(key);
      if (isMirrorKeysValues) fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException e) {
      if (!isMirrorKeysValues)
        throw e;
      else
        return; // abort test
    }
    assertNull(entry.getValue());
    assertNull(region.get(key));
  }
  
  /**
   * Tests locally invalidating an entire region
   */
  public void testLocalInvalidateRegion() throws CacheException {
    String name = this.getUniqueName();
    
    Region region = createRegion(name);
    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");
    
    boolean isKV = getRegionAttributes().getMirrorType().isKeysValues();
    try {
      region.localInvalidateRegion();
      if (isKV) fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException e) {
      if (!isKV)
        throw e;
      else
        return; // abort test
    }
    
    Region.Entry entry;
    
    entry = region.getEntry("A");
    assertNotNull(entry);
    assertNull(entry.getValue());
    
    entry = region.getEntry("B");
    assertNotNull(entry);
    assertNull(entry.getValue());
    
    entry = region.getEntry("C");
    assertNotNull(entry);
    assertNull(entry.getValue());
  }
  
  /**
   * Tests the {@link Region#subregions} method without recursion
   */
  public void testSubregions() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    Region region = createRegion(name);
    
    assertEquals(0, region.subregions(false).size());
    
    region.createSubregion("A", region.getAttributes());
    region.createSubregion("B", region.getAttributes());
    region.createSubregion("C", region.getAttributes());
    
    {
      Set subregions = region.subregions(false);
      assertEquals(3, subregions.size());
      
      Set names = new HashSet(Arrays.asList(new String[] {"A", "B", "C"}));
      Iterator iter = subregions.iterator();
      for (int i = 0; i < 3; i++) {
        assertTrue(iter.hasNext());
        assertTrue(names.remove(((Region)iter.next()).getName()));
      }
      assertFalse(iter.hasNext());
    }
/* not with ConcurrentHashMaps
    {
      Iterator iter = region.subregions(false).iterator();
      Region sub = (Region) iter.next();
      sub.destroyRegion();
 
      try {
        iter.next();
        fail("Should have thrown a ConcurrentModificationException");
 
      } catch (ConcurrentModificationException ex) {
        // pass...
      }
    }
 
    {
      Iterator iter = region.subregions(false).iterator();
      iter.next();
      region.createSubregion("D", region.getAttributes());
 
      try {
        iter.next();
        fail("Should have thrown a ConcurrentModificationException");
 
      } catch (ConcurrentModificationException ex) {
        // pass...
      }
    }
 */
  }
  
  /**
   * Tests the {@link Region#subregions} method with recursion
   */
  public void testSubregionsRecursive() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    Region region = createRegion(name);
    
    Region A = region.createSubregion("A", region.getAttributes());
    Region B = region.createSubregion("B", region.getAttributes());
    Region C = region.createSubregion("C", region.getAttributes());
    
    A.createSubregion("D", region.getAttributes());
    B.createSubregion("E", region.getAttributes());
    C.createSubregion("F", region.getAttributes());
    
    
    {
      Set subregions = region.subregions(true);
      assertEquals(6, subregions.size());
      
      Set names = new HashSet(Arrays.asList(new String[] {"A", "B", "C", "D", "E", "F"}));
      Iterator iter = subregions.iterator();
      for (int i = 0; i < 6; i++) {
        assertTrue(iter.hasNext());
        assertTrue(names.remove(((Region) iter.next()).getName()));
      }
      assertFalse(iter.hasNext());
    }
    /* not with ConcurrentHashMaps
    {
      Iterator iter = region.subregions(true).iterator();
      iter.next();
     
      // Destroy in the subregion should effect parent region's
      // iterator
      B.destroyRegion();
     
      try {
        iter.next();
        fail("Should have thrown a ConcurrentModificationException");
     
      } catch (ConcurrentModificationException ex) {
        // pass...
      }
    }
     
    {
      Iterator iter = region.subregions(true).iterator();
      iter.next();
     
      // Modifying the subregion should effect parent region's
      // iterator
      A.createSubregion("G", getRAs(region.getAttributes()));
     
      try {
        iter.next();
        fail("Should have thrown a ConcurrentModificationException");
     
      } catch (ConcurrentModificationException ex) {
        // pass...
      }
    }
     */
  }
  
  /**
   * Tests the {@link Region#values} method without recursion
   */
  public void testValues() throws CacheException {
    String name = this.getUniqueName();
    System.err.println("testValues region name is " +  name);
    Region region = createRegion(name);
    assertEquals(0, region.values().size());
    
    region.create("A", null);
    
    {
      Set values = new TreeSet(region.values());
      assertTrue(values.isEmpty());
      Iterator itr = values.iterator();
      assertTrue(!itr.hasNext());
      try {
        itr.next();
        fail("Should have thrown NoSuchElementException");
      } catch (NoSuchElementException e) {
        // succeed
      }
    }
    
    
    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");
    
    {
      Set values = new TreeSet(region.values());
      assertEquals(3, values.size());
      
      Iterator iter = values.iterator();
      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());
      assertTrue(iter.hasNext());
      assertEquals("b", iter.next());
      assertTrue(iter.hasNext());
      assertEquals("c", iter.next());
      assertFalse(iter.hasNext());
    }
    
    // test invalid values
    region.invalidate("B");
    {
      Set values = new TreeSet(region.values());
      assertEquals(2, values.size());
      
      Iterator iter = values.iterator();
      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());
      assertTrue(iter.hasNext());
      assertEquals("c", iter.next());
      assertFalse(iter.hasNext());
    }
  }
  
  
  // Helper functions
  ////////////////////////////
  static private final String WAIT_PROPERTY = 
    "UpdatePropagationDUnitTest.maxWaitTime";
  static private final int WAIT_DEFAULT = 60000;
  

  static private final int SLOP = 1000; // milliseconds

  private Object fetchEntryValue(Region.Entry re) {
    if (re.isLocal()) {
      return re.getValue();
    } else {
      Region r = re.getRegion();
      Object key = re.getKey();
      Region.Entry freshRE = r.getEntry(key);
      if (freshRE == null) {
        return null; // or should we throw an exception?
      }
      return freshRE.getValue();
    }
  }
  /**
   * Since <em>tilt</em> is the earliest time we expect, one must
   * check the current time <em>before</em> invoking the operation
   * intended to keep the entry alive.
   * 
   * @param entry entry we want to be invalidated
   * @param p_tilt earliest time we expect to see the invalidate
   */
  protected void waitForInvalidate(Region.Entry entry, long p_tilt) {
    waitForInvalidate(entry, p_tilt, 100);
  }
  /**
   * Since <em>tilt</em> is the earliest time we expect, one must
   * check the current time <em>before</em> invoking the operation
   * intended to keep the entry alive.
   * 
   * @param entry entry we want to be invalidated
   * @param p_tilt earliest time we expect to see the invalidate
   * @param pauseMs the number of milliseconds to pause before checking again
   */
  protected void waitForInvalidate(Region.Entry entry, long p_tilt, int pauseMs) {
    long tilt = p_tilt;
    // up until the time that the expiry fires, the entry
    // better not be null...
    if (entry == null) {
      // the entire wait routine was called very late, and
      // we have no entry?  That's ok.
      return;
    }
    for (;;) {
      boolean wasInvalidated = fetchEntryValue(entry) == null; // do this 1st
      long now = System.currentTimeMillis(); // do this 2nd
      if (now >= tilt) {
        // once this is true it is ok if it was invalidated
        break;
      }
      if (!wasInvalidated) {
        Wait.pause(pauseMs);
        continue;
      }
      if (now >= tilt - SLOP) {
        com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().warning("Entry invalidated sloppily "
            + "now=" + now + " tilt=" + tilt + " delta = " + (tilt - now));
        break;
      }
      fail("Entry invalidated prematurely "
           + "now=" + now + " tilt=" + tilt + " delta = " + (tilt - now));
    }

    // After the timeout passes, we will tolerate a slight
    // lag before the invalidate becomes visible (due to
    // system loading)
    // Slight lag? WAIT_DEFAULT is 60,000 ms. Many of our tests configure 20ms expiration.
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();
    tilt += maxWaitTime;
    for (;;) {
      if (fetchEntryValue(entry) == null) break;
      if (System.currentTimeMillis() > tilt) {
        if (fetchEntryValue(entry) == null) break;
        fail("Entry failed to invalidate");
      }
      Wait.pause(pauseMs);
    }
  }

  private boolean isEntryDestroyed(Region.Entry re) {
    if (re.isLocal()) {
      return re.isDestroyed();
    } else {
      Region r = re.getRegion();
      Object key = re.getKey();
      Region.Entry freshRE = r.getEntry(key);
      if (freshRE == null) {
        return true;
      }
      return freshRE.isDestroyed();
    }
  }
  
  /**
   * Since <em>tilt</em> is the earliest time we expect, one must
   * check the current time <em>before</em> invoking the operation
   * intended to keep the entry alive.
   * 
   * @param entry entry we want to be invalidated
   * @param p_tilt earliest time we expect to see the invalidate
   */
  protected void waitForDestroy(Region.Entry entry, long p_tilt) {
      waitForDestroy(entry, p_tilt, 100);
  }
  /**
   * Since <em>tilt</em> is the earliest time we expect, one must
   * check the current time <em>before</em> invoking the operation
   * intended to keep the entry alive.
   * 
   * @param entry entry we want to be invalidated
   * @param p_tilt earliest time we expect to see the invalidate
   * @param pauseMs the number of milliseconds to pause before checking again
   */
  protected void waitForDestroy(Region.Entry entry, long p_tilt, int pauseMs) {
    long tilt = p_tilt;
    // up until the time that the expiry fires, the entry
    // better not be null...
    for (;;) {
      long now = System.currentTimeMillis();
      if (now >= tilt)
        break;
      if (!isEntryDestroyed(entry)) {
        Wait.pause(pauseMs);
        continue;
      }
      if (now >= tilt - SLOP) {
        com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().warning("Entry destroyed sloppily "
            + "now=" + now + " tilt=" + tilt + " delta = " + (tilt - now));
        break;
      }
      fail("Entry destroyed prematurely"
          + "now=" + now + " tilt=" + tilt + " delta = " + (tilt - now));
    }

    // After the timeout passes, we will tolerate a slight
    // lag before the destroy becomes visible (due to
    // system loading)
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();
    tilt += maxWaitTime;
    for (;;) {
      if (isEntryDestroyed(entry))
        break;
      Assert.assertTrue(System.currentTimeMillis() <= tilt,
          "Entry failed to destroy");
      Wait.pause(pauseMs);
    }
  }
  
  /**
   * Since <em>tilt</em> is the earliest time we expect, one must
   * check the current time <em>before</em> invoking the operation
   * intended to keep the region alive.
   * 
   * @param region region we want to be destroyed
   * @param p_tilt earliest time we expect to see the destroy
   */

  protected void waitForRegionDestroy(Region region, long p_tilt) {
    long tilt = p_tilt;
    // up until the time that the expiry fires, the entry
    // better not be null...
    for (;;) {
      long now = System.currentTimeMillis();
      if (now >= tilt)
        break;
      if (!region.isDestroyed()) {
        Wait.pause(10);
        continue;
      }
      if (now >= tilt - SLOP) {
        com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().warning("Region destroyed sloppily "
            + "now=" + now + " tilt=" + tilt + " delta = " + (tilt - now));
        break;
      }
      fail("Region destroyed prematurely"
          + "now=" + now + " tilt=" + tilt + " delta = " + (tilt - now));
    }

    // After the timeout passes, we will tolerate a slight
    // lag before the destroy becomes visible (due to
    // system loading)
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();
    tilt += maxWaitTime;
    for (;;) {
      if (region.isDestroyed())
        break;
      Assert.assertTrue(System.currentTimeMillis() <= tilt,
          "Region failed to destroy");
      Wait.pause(10);
    }
  }  

  /**
   * Tests that an entry in a region expires with an
   * invalidation after a given time to live.
   */
  public void testEntryTtlInvalidate()
  throws CacheException {
    
    final String name = this.getUniqueName();
    final int timeout = 20; // ms!
    final String key = "KEY";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setEntryTimeToLive(expire);
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();

    Region region = null;
    /**
             * Crank up the expiration so test runs faster.
             * This property only needs to be set while the region is created
             */
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);
    } 
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    ExpiryTask.suspendExpiration();
    Region.Entry entry = null;
    long tilt;
    try {
      region.put(key, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key);
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);
  }
  
  /**
   * Verify that special entries expire but other entries in the region don't
   */
  public void testCustomEntryTtl1() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms!
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
//    factory.setEntryTimeToLive(expire);
    factory.setCustomEntryTimeToLive(new TestExpiry(key2, expire));
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();

    Region region = null;
    /**
     * Crank up the expiration so test runs faster.
     * This property only needs to be set while the region is created
     */
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);
    } 
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
    
    // Random values should not expire
    region.put(key1, value);
    Wait.pause(timeout * 2);
    assert(region.get(key1).equals(value));
    
    // key2 *should* expire
    ExpiryTask.suspendExpiration();
    Region.Entry entry = null;
    long tilt;
    try {
      region.put(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);
    
    assert(region.get(key1).equals(value));
  }
  
  
  /**
   * Verify that special entries don't expire but other entries in the region do
   */
  public void testCustomEntryTtl2() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
//    factory.setEntryIdleTimeout(expire);
    ExpirationAttributes expire2 =
      new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setCustomEntryTimeToLive(new TestExpiry(key2, expire2));
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterUpdate2(EntryEvent e) { }
      public void afterInvalidate2(EntryEvent e) { }
    };
    factory.addCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);
    } 
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value);
    
    // This value should NOT expire.
    Wait.pause(timeout * 2);
    assertTrue(region.get(key1).equals(value));
    
    // This value SHOULD expire
    
    // DebuggerSupport.waitForJavaDebugger(getLogWriter(), "Set breakpoint in invalidate");
    ExpiryTask.suspendExpiration();
    Region.Entry entry = null;
    long tilt;
    try {
      region.create(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertTrue(list.waitForInvocation(5000));
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertTrue(region.get(key1).equals(value));
    
    // Do it again with a put (I guess)
    ExpiryTask.suspendExpiration();
    try {
      region.put(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertTrue(region.get(key1).equals(value));
  }
  
  protected volatile int eventCount;

  /**
   * Expire an entry with a custom expiration.  Set a new custom expiration, create the
   * same entry again, make sure it observes the <em>new</em> expiration
   */
  public void testCustomEntryTtl3() {

    final String name = this.getUniqueName();
    final int timeout1 = 20; // ms
    final int timeout2 = 40;
    final String key1 = "KEY1";
    final String value1 = "VALUE1";
    final String value2 = "VALUE2";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire1 =
            new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
//    factory.setEntryIdleTimeout(expire);
    factory.setCustomEntryTimeToLive(new TestExpiry(key1, expire1));
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterUpdate2(EntryEvent e) { }
      public void afterInvalidate2(EntryEvent e) { eventCount ++; }
    };
    // Disk regions are VERY slow, so we need to wait for the event...
    WaitCriterion waitForEventCountToBeOne = new WaitCriterion() {
      public boolean done() {
        return eventCount == 1;
      }
      public String description() {
        return "eventCount never became 1";
      }
    };
    eventCount = 0;
    factory.addCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);

      // DebuggerSupport.waitForJavaDebugger(getLogWriter(), "Set breakpoint in
      // invalidate");
      ExpiryTask.suspendExpiration();
      Region.Entry entry = null;
      eventCount = 0;
      long tilt1;
      long tilt2;
      try {
        region.create(key1, value1);
        tilt1 = System.currentTimeMillis() + timeout1;
        entry = region.getEntry(key1);
        assertTrue(list.waitForInvocation(1000));
        Assert.assertTrue(value1.equals(entry.getValue()));
      }
      finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt1, timeout1/2);
      Wait.waitForCriterion(waitForEventCountToBeOne, 10 * 1000, 100, true);
      eventCount = 0;

      // Do it again with a put (I guess)
      ExpiryTask.suspendExpiration();
      try {
        region.put(key1, value1);
        tilt1 = System.currentTimeMillis() + timeout1;
        entry = region.getEntry(key1);
        Assert.assertTrue(value1.equals(entry.getValue()));
        assertTrue(list.waitForInvocation(10 * 1000));
      }
      finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt1, timeout1/2);
      Wait.waitForCriterion(waitForEventCountToBeOne, 10 * 1000, 100, true);
      eventCount = 0;

      // Change custom expiry for this region now...
      final String key2 = "KEY2";
      AttributesMutator mutt = region.getAttributesMutator();
      ExpirationAttributes expire2 = new ExpirationAttributes(timeout2,
          ExpirationAction.INVALIDATE);
      mutt.setCustomEntryTimeToLive(new TestExpiry(key2, expire2));

      ExpiryTask.suspendExpiration();
      try {
        region.put(key1, value1);
        region.put(key2, value2);
        tilt1 = System.currentTimeMillis() + timeout1;
        tilt2 = tilt1 + timeout2 - timeout1;
        entry = region.getEntry(key1);
        Assert.assertTrue(value1.equals(entry.getValue()));
        entry = region.getEntry(key2);
        Assert.assertTrue(value2.equals(entry.getValue()));
        assertTrue(list.waitForInvocation(1000));
      }
      finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt2, timeout2/2);
      Wait.waitForCriterion(waitForEventCountToBeOne, 10 * 1000, 100, true);
      eventCount = 0;
      // key1 should not be invalidated since we mutated to custom expiry to only expire key2
      entry = region.getEntry(key1);
      Assert.assertTrue(value1.equals(entry.getValue()));
      // now mutate back to key1 and change the action
      ExpirationAttributes expire3 = new ExpirationAttributes(timeout1, ExpirationAction.DESTROY);
      mutt.setCustomEntryTimeToLive(new TestExpiry(key1, expire3));
      waitForDestroy(entry, tilt1, timeout1/2);
    }
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  /**
   * Configure entry expiration with a ttl time.
   * Create an entry and records its scheduled expiration time.
   * Then mutate the region expiration configuration and confirm
   * that the entry's expiration time is rescheduled.
   */
  public void testEntryTtl3() {
    final String name = this.getUniqueName();
    // test no longer waits for this expiration to happen
    final int timeout1 = 500 * 1000; // ms
    final int timeout2 = 2000 * 1000; // ms
    final String key1 = "KEY1";
    final String value1 = "VALUE1";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire1 =
            new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
    factory.setEntryTimeToLive(expire1);
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterUpdate2(EntryEvent e) { }
      public void afterInvalidate2(EntryEvent e) { eventCount ++; }
    };
    eventCount = 0;
    factory.addCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    LocalRegion region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = (LocalRegion) createRegion(name, attrs);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value1);
    EntryExpiryTask eet = region.getEntryExpiryTask(key1);
    final long firstExpiryTime = eet.getExpirationTime();

    AttributesMutator mutt = region.getAttributesMutator();
    ExpirationAttributes expire2 = new ExpirationAttributes(timeout2, ExpirationAction.INVALIDATE);
    mutt.setEntryTimeToLive(expire2);
    eet = region.getEntryExpiryTask(key1);
    final long secondExpiryTime = eet.getExpirationTime();
    if ((secondExpiryTime - firstExpiryTime) <= 0) {
      fail("expiration time should have been greater after changing region config from 500 to 2000. firstExpiryTime=" + firstExpiryTime + " secondExpiryTime=" + secondExpiryTime);
    }
    
    // now set back to be more recent
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire3 = new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
    mutt.setEntryTimeToLive(expire3);
    eet = region.getEntryExpiryTask(key1);
    final long thirdExpiryTime = eet.getExpirationTime();
    assertEquals(firstExpiryTime, thirdExpiryTime);
    // confirm that it still has not expired
    assertEquals(0, eventCount);
    
    // now set it to a really short time and make sure it expires immediately
    Wait.waitForExpiryClockToChange(region);
    final Region.Entry entry = region.getEntry(key1);
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire4 = new ExpirationAttributes(1, ExpirationAction.INVALIDATE);
    mutt.setEntryTimeToLive(expire4);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return fetchEntryValue(entry) == null;
      }
      public String description() {
        return "entry never became invalid";
      }
    };
    Wait.waitForCriterion(wc, 10 * 1000, 10, true);

    WaitCriterion waitForEventCountToBeOne = new WaitCriterion() {
      public boolean done() {
        return eventCount == 1;
      }
      public String description() {
        return "eventCount never became 1";
      }
    };
    Wait.waitForCriterion(waitForEventCountToBeOne, 10 * 1000, 10, true);
    eventCount = 0;
  }

//  /**
//   * Expire an entry with a ttl time.  Set a new ttl time, create the
//   * same entry again, make sure it observes the <em>new</em> ttl time.
//   * Do this many times.
//   */
//  public void testEntryTtl4() {
//    if (!supportsExpiration()) {
//      return;
//    }
//    final String name = this.getUniqueName();
//    final int timeout1 = 200; // ms
//    final int timeout2 = 2000;
//    final String key1 = "KEY1";
//    final String value1 = "VALUE1";
//    final String value2 = "VALUE2";
//    
//    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
//    ExpirationAttributes expire1 =
//            new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
//    factory.setEntryTimeToLive(expire1);
//    factory.setStatisticsEnabled(true);
//    TestCacheListener list = new TestCacheListener() {
//      public void afterCreate2(EntryEvent e) { }
//      public void afterUpdate2(EntryEvent e) { }
//      public void afterDestroy2(EntryEvent e) { }
//      public void afterInvalidate2(EntryEvent e) { eventCount ++; }
//    };
//    eventCount = 0;
//    factory.addCacheListener(list);
//    RegionAttributes attrs = factory.create();
//    
//    Region region = null;
//    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY", "true");
//    try {
//      region = createRegion(name, attrs);
//    } finally {
//      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
//    }
//
//    for (int i = 0; i < 10; i ++) {
//      // DebuggerSupport.waitForJavaDebugger(getLogWriter(), "Set breakpoint in invalidate");
//      ExpiryTask.suspendExpiration();
//      Region.Entry entry = null;
//      long tilt;
//      try {
//        region.create(key1, value1);
//        tilt = System.currentTimeMillis() + timeout1;
//        assertTrue(list.waitForInvocation(1000));
//        entry = region.getEntry(key1);
//        Assert.assertTrue(value1.equals(entry.getValue()));
//      } finally {
//        ExpiryTask.permitExpiration();
//      }
//      waitForInvalidate(entry, tilt);
//      assertTrue(list.waitForInvocation(1000));
//      assertTrue(eventCount == 1);
//      eventCount = 0;
//
//      // Do it again with a put (I guess)
//      ExpiryTask.suspendExpiration();
//      try {
//        region.put(key1, value1);
//        tilt = System.currentTimeMillis() + timeout1;
//        entry = region.getEntry(key1);
//        Assert.assertTrue(value1.equals(entry.getValue()));
//      } finally {
//        ExpiryTask.permitExpiration();
//      }
//      waitForInvalidate(entry, tilt);
//      assertTrue(list.waitForInvocation(1000));
//      assertTrue(eventCount == 1);
//      eventCount = 0;
//      
//      // Change custom expiry for this region now...
//      AttributesMutator mutt = region.getAttributesMutator();
//      ExpirationAttributes expire2 =
//        new ExpirationAttributes(timeout2, ExpirationAction.INVALIDATE);
//      mutt.setEntryTimeToLive(expire2);
//      
//      ExpiryTask.suspendExpiration();
//      try {
//        region.put(key1, value2);
//        tilt = System.currentTimeMillis() + timeout2;
//        entry = region.getEntry(key1);
//        Assert.assertTrue(value2.equals(entry.getValue()));
//      } finally {
//        ExpiryTask.permitExpiration();
//      }
//      waitForInvalidate(entry, tilt);
//      assertTrue(list.waitForInvocation(1000));
//      assertTrue(eventCount == 1);
//      eventCount = 0;
//      
//      region.destroy(key1);
//    }
//  }


  /**
   * Tests that an entry whose value is loaded into a region
   * expires with an invalidation after a given time to live.
   */
  public void testEntryFromLoadTtlInvalidate()
  throws CacheException, InterruptedException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms!
    final String key = "KEY";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setEntryTimeToLive(expire);
    factory.setStatisticsEnabled(true);
    factory.setCacheLoader(new TestCacheLoader() {
      public Object load2(LoaderHelper helper) {
        return value;
      }
    });
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);

      // DebuggerSupport.waitForJavaDebugger(getLogWriter(), "About to get");
      ExpiryTask.suspendExpiration();
      Region.Entry entry = null;
      long tilt;
      try {
        region.get(key);
        tilt = System.currentTimeMillis() + timeout;
        entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      }
      finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt);
    }
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }
  
  /**
   * Tests that an entry in a region expires with a destroy
   * after a given time to live.
   */
  public void testEntryTtlDestroy()
  throws CacheException, InterruptedException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key = "KEY";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setEntryTimeToLive(expire);
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);

      ExpiryTask.suspendExpiration();
      Region.Entry entry = null;
      long tilt;
      try {
        region.put(key, value);
        tilt = System.currentTimeMillis();
        entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      }
      finally {
        ExpiryTask.permitExpiration();
      }
      waitForDestroy(entry, tilt);
    }
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }
  
  /**
   * Tests that a region expires with an invalidation after a
   * given time to live.
   */
  public void testRegionTtlInvalidate()
  throws CacheException, InterruptedException {

    if(getRegionAttributes().getPartitionAttributes() != null)
      return;
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    final String name = this.getUniqueName();
    
    vm0.invoke(new CacheSerializableRunnable("testRegionTtlInvalidate") {
      public void run2() throws CacheException {
        final int timeout = 22; // ms
        final Object key = "KEY";
        final Object value = "VALUE";
        
        AttributesFactory factory = new AttributesFactory(getRegionAttributes());
        ExpirationAttributes expire =
                new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
        factory.setRegionTimeToLive(expire);
        factory.setStatisticsEnabled(true);
        RegionAttributes attrs = factory.create();
        
        Region region = null;
        Region.Entry entry = null;
        long tilt;
        ExpiryTask.suspendExpiration();
        try {
          System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
          try {
            region = createRegion(name, attrs);
            region.put(key, value);
            region.put("k2", "v2");
            tilt = System.currentTimeMillis() + timeout;
            entry = region.getEntry(key);
            assertNotNull(entry.getValue());
          }
          finally {
            System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
          }
        } 
        finally {
          ExpiryTask.permitExpiration();
        }
        waitForInvalidate(entry, tilt, 10);
        waitForInvalidate(region.getEntry("k2"), tilt, 10);
      }
    });
  }
  
  /**
   * Tests that a region expires with a destruction after a
   * given time to live.
   */
  public void testRegionTtlDestroy()
  throws CacheException, InterruptedException {

    if(getRegionAttributes().getPartitionAttributes() != null)
      return;
    
    final String name = this.getUniqueName();
    final int timeout = 22; // ms
    final Object key = "KEY";
    final Object value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setRegionTimeToLive(expire);
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    long tilt;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    ExpiryTask.suspendExpiration();
    try {
      try {
        region = createRegion(name, attrs);
        assertFalse(region.isDestroyed());
        tilt = System.currentTimeMillis() + timeout;
        region.put(key, value);
        Region.Entry entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      }
      finally {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForRegionDestroy(region, tilt);
  }
  
  /**
   * Tests that an entry in a local region that remains idle for a
   * given amount of time is invalidated.
   */
  public void testEntryIdleInvalidate()
  throws CacheException, InterruptedException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key = "KEY";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setEntryIdleTimeout(expire);
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterUpdate2(EntryEvent e) { }
      public void afterInvalidate2(EntryEvent e) { }
    };
    factory.setCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);

      // DebuggerSupport.waitForJavaDebugger(getLogWriter(), "Set breakpoint in
      // invalidate");
      ExpiryTask.suspendExpiration();
      Region.Entry entry = null;
      long tilt;
      try {
        region.create(key, value);
        tilt = System.currentTimeMillis() + timeout;
        assertTrue(list.waitForInvocation(333));
        entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      }
      finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt);

      ExpiryTask.suspendExpiration();
      try {
        region.put(key, value);
        tilt = System.currentTimeMillis() + timeout;
        entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      }
      finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt);
    } 
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }
  
  protected class TestExpiry implements CustomExpiry, Declarable {
    
    final String special;
    final ExpirationAttributes specialAtt;
    
    protected TestExpiry(String flagged, ExpirationAttributes att) {
      this.special = flagged;
      this.specialAtt = att;
    }
    
    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.CustomExpiry#getExpiry(com.gemstone.gemfire.cache.Region.Entry)
     */
    public ExpirationAttributes getExpiry(Entry entry) {
//      getCache().getLogger().fine("Calculating expiry for " + entry.getKey()
//          , new Exception("here")
//      );
      if (entry.getKey().equals(special)) {
        return specialAtt;
      }
      return null;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
     */
    public void init(Properties props) {
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.CacheCallback#close()
     */
    public void close() {
    }
  }
  
  /**
   * Verify that special entries expire but other entries in the region don't
   */
  public void testCustomEntryIdleTimeout1() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
//    factory.setEntryIdleTimeout(expire);
    factory.setCustomEntryIdleTimeout(new TestExpiry(key2, expire));
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterUpdate2(EntryEvent e) { }
      public void afterInvalidate2(EntryEvent e) { }
    };
    factory.addCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);
    } 
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value);
    
    // This value should NOT expire.
    Wait.pause(timeout * 2);
    assertTrue(region.get(key1).equals(value));
    
    // This value SHOULD expire
    
    // DebuggerSupport.waitForJavaDebugger(getLogWriter(), "Set breakpoint in invalidate");
    ExpiryTask.suspendExpiration();
    Region.Entry entry = null;
    long tilt;
    try {
      region.create(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      assertTrue(list.waitForInvocation(5000));
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertTrue(region.get(key1).equals(value));
    
    // Do it again with a put (I guess)
    ExpiryTask.suspendExpiration();
    try {
      region.put(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertTrue(region.get(key1).equals(value));
  }
  
  /**
   * Verify that special entries don't expire but other entries in the region do
   */
  public void testCustomEntryIdleTimeout2() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setEntryIdleTimeout(expire);
    ExpirationAttributes expire2 =
      new ExpirationAttributes(0, ExpirationAction.INVALIDATE);
    factory.setCustomEntryIdleTimeout(new TestExpiry(key2, expire2));
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterUpdate2(EntryEvent e) { }
      public void afterInvalidate2(EntryEvent e) { }
    };
    factory.addCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);
    } 
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    region.create(key2, value);
    
    // This value should NOT expire.
    Wait.pause(timeout * 2);
    assertTrue(region.get(key2).equals(value));
    
    // This value SHOULD expire
    
    // DebuggerSupport.waitForJavaDebugger(getLogWriter(), "Set breakpoint in invalidate");
    ExpiryTask.suspendExpiration();
    Region.Entry entry = null;
    long tilt;
    try {
      region.create(key1, value);
      tilt = System.currentTimeMillis() + timeout;
      assertTrue(list.waitForInvocation(5000));
      entry = region.getEntry(key1);
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertTrue(region.get(key2).equals(value));
    
    // Do it again with a put (I guess)
    ExpiryTask.suspendExpiration();
    try {
      region.put(key1, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key1);
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertTrue(region.get(key2).equals(value));
  }
  
  /**
   * Configure custome entry expiration with an idle time.
   * Create an entry and records its scheduled expiration time.
   * Then mutate the region expiration configuration and confirm
   * that the entry's expiration time is rescheduled.
   */
  public void testCustomEntryIdleTimeout3() {
    final String name = this.getUniqueName();
    // test no longer waits for this expiration to happen
    final int timeout1 = 500 * 1000; // ms
    final int timeout2 = 2000 * 1000; // ms
    final String key1 = "KEY1";
    final String value1 = "VALUE1";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire1 =
            new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
    factory.setCustomEntryIdleTimeout(new TestExpiry(key1, expire1));
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterUpdate2(EntryEvent e) { }
      public void afterInvalidate2(EntryEvent e) { eventCount ++; }
    };
    eventCount = 0;
    factory.addCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    LocalRegion region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = (LocalRegion) createRegion(name, attrs);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value1);
    EntryExpiryTask eet = region.getEntryExpiryTask(key1);
    final long firstExpiryTime = eet.getExpirationTime();

    AttributesMutator mutt = region.getAttributesMutator();
    ExpirationAttributes expire2 = new ExpirationAttributes(timeout2, ExpirationAction.INVALIDATE);
    mutt.setCustomEntryIdleTimeout(new TestExpiry(key1, expire2));
    eet = region.getEntryExpiryTask(key1);
    final long secondExpiryTime = eet.getExpirationTime();
    if ((secondExpiryTime - firstExpiryTime) <= 0) {
      fail("expiration time should have been greater after changing region config from 500 to 2000. firstExpiryTime=" + firstExpiryTime + " secondExpiryTime=" + secondExpiryTime);
    }
    
    // now set back to be more recent
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire3 = new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
    mutt.setCustomEntryIdleTimeout(new TestExpiry(key1, expire3));
    eet = region.getEntryExpiryTask(key1);
    final long thirdExpiryTime = eet.getExpirationTime();
    assertEquals(firstExpiryTime, thirdExpiryTime);
    // confirm that it still has not expired
    assertEquals(0, eventCount);
    
    // now set it to a really short time and make sure it expires immediately
    Wait.waitForExpiryClockToChange(region);
    final Region.Entry entry = region.getEntry(key1);
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire4 = new ExpirationAttributes(1, ExpirationAction.INVALIDATE);
    mutt.setCustomEntryIdleTimeout(new TestExpiry(key1, expire4));
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return fetchEntryValue(entry) == null;
      }
      public String description() {
        return "entry never became invalid";
      }
    };
    Wait.waitForCriterion(wc, 10 * 1000, 10, true);

    WaitCriterion waitForEventCountToBeOne = new WaitCriterion() {
      public boolean done() {
        return eventCount == 1;
      }
      public String description() {
        return "eventCount never became 1";
      }
    };
    Wait.waitForCriterion(waitForEventCountToBeOne, 10 * 1000, 10, true);
    eventCount = 0;
  }

  /**
   * Configure entry expiration with a idle time.
   * Create an entry and records its scheduled expiration time.
   * Then mutate the region expiration configuration and confirm
   * that the entry's expiration time is rescheduled.
   */
  public void testEntryIdleTimeout3() {
    final String name = this.getUniqueName();
    // test no longer waits for this expiration to happen
    final int timeout1 = 500 * 1000; // ms
    final int timeout2 = 2000 * 1000; // ms
    final String key1 = "KEY1";
    final String value1 = "VALUE1";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire1 =
            new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
    factory.setEntryIdleTimeout(expire1);
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterUpdate2(EntryEvent e) { }
      public void afterInvalidate2(EntryEvent e) { eventCount ++; }
    };
    eventCount = 0;
    factory.addCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    LocalRegion region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = (LocalRegion) createRegion(name, attrs);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value1);
    EntryExpiryTask eet = region.getEntryExpiryTask(key1);
    final long firstExpiryTime = eet.getExpirationTime();

    AttributesMutator mutt = region.getAttributesMutator();
    ExpirationAttributes expire2 = new ExpirationAttributes(timeout2, ExpirationAction.INVALIDATE);
    mutt.setEntryIdleTimeout(expire2);
    eet = region.getEntryExpiryTask(key1);
    final long secondExpiryTime = eet.getExpirationTime();
    if ((secondExpiryTime - firstExpiryTime) <= 0) {
      fail("expiration time should have been greater after changing region config from 500 to 2000. firstExpiryTime=" + firstExpiryTime + " secondExpiryTime=" + secondExpiryTime);
    }
    
    // now set back to be more recent
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire3 = new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
    mutt.setEntryIdleTimeout(expire3);
    eet = region.getEntryExpiryTask(key1);
    final long thirdExpiryTime = eet.getExpirationTime();
    assertEquals(firstExpiryTime, thirdExpiryTime);
    // confirm that it still has not expired
    assertEquals(0, eventCount);
    
    // now set it to a really short time and make sure it expires immediately
    Wait.waitForExpiryClockToChange(region);
    final Region.Entry entry = region.getEntry(key1);
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire4 = new ExpirationAttributes(1, ExpirationAction.INVALIDATE);
    mutt.setEntryIdleTimeout(expire4);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return fetchEntryValue(entry) == null;
      }
      public String description() {
        return "entry never became invalid";
      }
    };
    Wait.waitForCriterion(wc, 10 * 1000, 10, true);

    WaitCriterion waitForEventCountToBeOne = new WaitCriterion() {
      public boolean done() {
        return eventCount == 1;
      }
      public String description() {
        return "eventCount never became 1";
      }
    };
    Wait.waitForCriterion(waitForEventCountToBeOne, 10 * 1000, 10, true);
    eventCount = 0;
  }

//  /**
//   * Expire an entry with a given idle time.  Set a new idle time, create the
//   * same entry again, make sure it observes the <em>new</em> idle time.  Do this
//   * many times.
//   */
//  public void testEntryIdleTimeout4() {
//    if (!supportsExpiration()) {
//      return;
//    }
//    final String name = this.getUniqueName();
//    final int timeout1 = 200; // ms
//    final int timeout2 = 2000;
//    final String key1 = "KEY1";
//    final String value1 = "VALUE1";
//    final String value2 = "VALUE2";
//    
//    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
//    ExpirationAttributes expire1 =
//            new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
//    factory.setEntryIdleTimeout(expire1);
//    factory.setStatisticsEnabled(true);
//    TestCacheListener list = new TestCacheListener() {
//      public void afterCreate2(EntryEvent e) { }
//      public void afterUpdate2(EntryEvent e) { }
//      public void afterDestroy2(EntryEvent e) { }
//      public void afterInvalidate2(EntryEvent e) { eventCount ++; }
//    };
//    eventCount = 0;
//    factory.addCacheListener(list);
//    RegionAttributes attrs = factory.create();
//    
//    Region region = null;
//    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
//    try {
//      region = createRegion(name, attrs);
//    } finally {
//      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
//    }
//
//    for (int i = 0; i < 10; i ++) {
//      // DebuggerSupport.waitForJavaDebugger(getLogWriter(), "Set breakpoint in invalidate");
//      ExpiryTask.suspendExpiration();
//      Region.Entry entry = null;
//      long tilt;
//      try {
//        region.create(key1, value1);
//        tilt = System.currentTimeMillis() + timeout1;
//        assertTrue(list.waitForInvocation(1000));
//        entry = region.getEntry(key1);
//        Assert.assertTrue(value1.equals(entry.getValue()));
//      } finally {
//        ExpiryTask.permitExpiration();
//      }
//      waitForInvalidate(entry, tilt);
//      assertTrue(list.waitForInvocation(1000));
//      assertTrue(eventCount == 1);
//      eventCount = 0;
//
//      // Do it again with a put (I guess)
//      ExpiryTask.suspendExpiration();
//      try {
//        region.put(key1, value1);
//        tilt = System.currentTimeMillis() + timeout1;
//        entry = region.getEntry(key1);
//        Assert.assertTrue(value1.equals(entry.getValue()));
//      } finally {
//        ExpiryTask.permitExpiration();
//      }
//      waitForInvalidate(entry, tilt);
//      assertTrue(list.waitForInvocation(1000));
//      assertTrue(eventCount == 1);
//      eventCount = 0;
//      
//      // Change expiry for this region now...
//      AttributesMutator mutt = region.getAttributesMutator();
//      ExpirationAttributes expire2 =
//        new ExpirationAttributes(timeout2, ExpirationAction.INVALIDATE);
//      mutt.setEntryIdleTimeout(expire2);
//      
//      ExpiryTask.suspendExpiration();
//      try {
//        region.put(key1, value2);
//        tilt = System.currentTimeMillis() + timeout2;
//        entry = region.getEntry(key1);
//        Assert.assertTrue(value2.equals(entry.getValue()));
//      } finally {
//        ExpiryTask.permitExpiration();
//      }
//      waitForInvalidate(entry, tilt);
//      assertTrue(list.waitForInvocation(1000));
//      assertTrue(eventCount == 1);
//      eventCount = 0;
//      
//      region.destroy(key1);
//    }
//  }

  static class CountExpiry implements CustomExpiry, Declarable {
    
    /**
     * Object --> CountExpiry
     * 
     * @guarded.By CountExpiry.class
     */
    static final HashMap invokeCounts = new HashMap();
    
    final String special;
    final ExpirationAttributes specialAtt;
    
    protected CountExpiry(String flagged, ExpirationAttributes att) {
      this.special = flagged;
      this.specialAtt = att;
    }
    
    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.CustomExpiry#getExpiry(com.gemstone.gemfire.cache.Region.Entry)
     */
    public ExpirationAttributes getExpiry(Entry entry) {
      Object key = entry.getKey();
      synchronized (CountExpiry.class) {
        Integer count = (Integer)invokeCounts.get(key);
        if (count == null) {
          invokeCounts.put(key, new Integer(1));
        }
        else {
          invokeCounts.put(key, new Integer(count.intValue() + 1));
        }
      } // synchronized
      if (key.equals(special)) {
        return specialAtt;
      }
      return null;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
     */
    public void init(Properties props) {
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.CacheCallback#close()
     */
    public void close() {
    }
  }
  
  /**
   * Verify that expiry is calculatod only once on an entry
   */
  public void testCustomIdleOnce() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms!
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
//    factory.setEntryTimeToLive(expire);
    factory.setCustomEntryTimeToLive(new CountExpiry(key2, expire));
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();
    synchronized (CountExpiry.class) {
      CountExpiry.invokeCounts.clear();
    }

    Region region = null;
    /**
             * Crank up the expiration so test runs faster.
             * This property only needs to be set while the region is created
             */
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);
    } 
    finally {
      if(region.getAttributes().getPartitionAttributes() == null)
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
    
    // Random values should not expire
    region.put(key1, value);
    Wait.pause(timeout * 2);
    assert(region.get(key1).equals(value));
    
    // key2 *should* expire
    ExpiryTask.suspendExpiration();
    Region.Entry entry = null;
    long tilt;
    try {
      region.put(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
      if(region.getAttributes().getPartitionAttributes() != null)
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
    waitForInvalidate(entry, tilt);
    
    assert(region.get(key1).equals(value));
    
    synchronized (CountExpiry.class) {
      if (CountExpiry.invokeCounts.size() != 2) {
        fail("CountExpiry not invoked correctly, size = " 
            + CountExpiry.invokeCounts.size());
      }
      Integer i = (Integer)CountExpiry.invokeCounts.get(key1);
      assertNotNull(i);
      assertEquals(1, i.intValue());
      
      i = (Integer)CountExpiry.invokeCounts.get(key2);
      assertNotNull(i);
      assertEquals(1, i.intValue());
    } // synchronized
  }
  
  /**
   * Verify that a get or put resets the idle time on an entry
   */
  public void testCustomEntryIdleReset() {

    final String name = this.getUniqueName();
    final int timeout = 200*1000; // ms
    final String key1 = "KEY1";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
//    factory.setEntryIdleTimeout(expire);
    factory.setCustomEntryIdleTimeout(new TestExpiry(key1, expire));
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterUpdate2(EntryEvent e) { }
      public void afterInvalidate2(EntryEvent e) { }
    };
    factory.addCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      LocalRegion region = (LocalRegion) createRegion(name, attrs);

    // DebuggerSupport.waitForJavaDebugger(getLogWriter(), "Set breakpoint in invalidate");
    ExpiryTask.suspendExpiration();
    try {
      region.create(key1, value);
      assertTrue(list.waitForInvocation(5000));
      Region.Entry entry = region.getEntry(key1);
      assertNotNull(entry.getValue());
      EntryExpiryTask eet = region.getEntryExpiryTask(key1);
      final long createExpiryTime = eet.getExpirationTime();
      Wait.waitForExpiryClockToChange(region);
      region.get(key1);
      assertSame(eet, region.getEntryExpiryTask(key1));
      final long getExpiryTime = eet.getExpirationTime();
      if (getExpiryTime - createExpiryTime <= 0L) {
        fail("get did not reset the expiration time. createExpiryTime=" + createExpiryTime + " getExpiryTime=" + getExpiryTime);
      }
      Wait.waitForExpiryClockToChange(region);
      region.put(key1, value);
      assertSame(eet, region.getEntryExpiryTask(key1));
      final long putExpiryTime = eet.getExpirationTime();
      if (putExpiryTime - getExpiryTime <= 0L) {
        fail("put did not reset the expiration time. getExpiryTime=" + getExpiryTime + " putExpiryTime=" + putExpiryTime);
      }
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    } 
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }
  
  /**
   * Tests that an entry in a region that remains idle for a
   * given amount of time is destroyed.
   */
  public void testEntryIdleDestroy()
  throws CacheException, InterruptedException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key = "KEY";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setEntryIdleTimeout(expire);
    factory.setStatisticsEnabled(true);
    TestCacheListener list = new TestCacheListener() {
      public void afterCreate2(EntryEvent e) { }
      public void afterDestroy2(EntryEvent e) { }
    };
    factory.setCacheListener(list);
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);

    ExpiryTask.suspendExpiration();
    Region.Entry entry = null;
    long tilt;
    try {
      region.create(key, null);
      tilt = System.currentTimeMillis() + timeout;
      assertTrue(list.wasInvoked());
      entry = region.getEntry(key);
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForDestroy(entry, tilt);
    assertNull(region.getEntry(key));
    
    ExpiryTask.suspendExpiration();
    try {
      region.put(key, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key);
      assertNotNull(entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForDestroy(entry, tilt);
    } 
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
    
  }

  /**
   * Verify that accessing an entry resets its idle time
   * @throws Exception
   */
  public void testEntryIdleReset() throws Exception {

    final String name = this.getUniqueName();
    // Test no longer waits for this timeout to expire
    final int timeout = 90; // seconds
    final String key = "KEY";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setEntryIdleTimeout(expire);
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();
    
    LocalRegion region = (LocalRegion) createRegion(name, attrs);
    region.create(key, null);
    EntryExpiryTask eet = region.getEntryExpiryTask(key);
    long createExpiryTime = eet.getExpirationTime();

    Wait.waitForExpiryClockToChange(region);
    region.get(key); // touch
    assertSame(eet, region.getEntryExpiryTask(key));
    long getExpiryTime = eet.getExpirationTime();
    if (getExpiryTime - createExpiryTime <= 0L) {
      fail("get did not reset the expiration time. createExpiryTime=" + createExpiryTime + " getExpiryTime=" + getExpiryTime);
    }
    
    Wait.waitForExpiryClockToChange(region);
    region.put(key, value); // touch
    assertSame(eet, region.getEntryExpiryTask(key));
    long putExpiryTime = eet.getExpirationTime();
    if (putExpiryTime - getExpiryTime <= 0L) {
      fail("put did not reset the expiration time. getExpiryTime=" + getExpiryTime + " putExpiryTime=" + putExpiryTime);
    }

    // TODO other ops that should be validated?

    // Now verify operations that do not modify the expiry time
    
    Wait.waitForExpiryClockToChange(region);
    region.invalidate(key); // touch
    assertSame(eet, region.getEntryExpiryTask(key));
    long invalidateExpiryTime = eet.getExpirationTime();
    if (region.getConcurrencyChecksEnabled()) {
      if (putExpiryTime - getExpiryTime <= 0L) {
        fail("invalidate did not reset the expiration time. putExpiryTime=" + putExpiryTime + " invalidateExpiryTime=" + invalidateExpiryTime);
      }
    } else {
      if (invalidateExpiryTime != putExpiryTime) {
        fail("invalidate did reset the expiration time. putExpiryTime=" + putExpiryTime + " invalidateExpiryTime=" + invalidateExpiryTime);
      }
    }
  }
  
  public void testEntryExpirationAfterMutate()
  throws CacheException, InterruptedException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final int hugeTimeout = Integer.MAX_VALUE;
    final ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    final ExpirationAttributes hugeExpire =
            new ExpirationAttributes(hugeTimeout, ExpirationAction.INVALIDATE);
    final String key = "KEY";
    final String value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();
    Region region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);
    } 
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    long tilt;
    region.create(key, value);
    tilt = System.currentTimeMillis() + timeout;
    
    // Now go from huge timeout to a timeout
    ExpiryTask.suspendExpiration();
    Region.Entry entry = null;
    try {
      region.getAttributesMutator().setEntryIdleTimeout(expire);
      entry = region.getEntry(key);
      assertEquals(value, entry.getValue());
    } 
    finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);
    
    // Now go from a big timeout to a short one
    region.getAttributesMutator().setEntryIdleTimeout(hugeExpire);
    region.put(key, value);
    tilt = System.currentTimeMillis() + timeout;
    entry = region.getEntry(key);
    Wait.pause(timeout * 2);
    assertEquals(value, entry.getValue());
    region.getAttributesMutator().setEntryIdleTimeout(expire);
    waitForInvalidate(entry, tilt);
  }

  /**
   * Verify that accessing an entry does not delay expiration due
   * to TTL
   */
  public void testEntryIdleTtl() {

    final String name = this.getUniqueName();
    // test no longer waits for this timeout to expire
    final int timeout = 2000; // seconds
    final String key = "IDLE_TTL_KEY";
    final String value = "IDLE_TTL_VALUE";
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expireIdle =
            new ExpirationAttributes(timeout / 2, ExpirationAction.DESTROY);
    factory.setEntryIdleTimeout(expireIdle);
    ExpirationAttributes expireTtl =
      new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setEntryTimeToLive(expireTtl);
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();
    
    LocalRegion region = (LocalRegion) createRegion(name, attrs);
   
    region.create(key, value);
    EntryExpiryTask eet = region.getEntryExpiryTask(key);
    final long firstIdleExpiryTime = eet.getIdleExpirationTime();
    final long firstTTLExpiryTime = eet.getTTLExpirationTime();
    if ((firstIdleExpiryTime - firstTTLExpiryTime) >= 0) {
      fail("idle should be less than ttl: idle=" + firstIdleExpiryTime + " ttl=" + firstTTLExpiryTime);
    }
    Wait.waitForExpiryClockToChange(region);
    region.get(key);
    eet = region.getEntryExpiryTask(key);
    final long secondIdleExpiryTime = eet.getIdleExpirationTime();
    final long secondTTLExpiryTime = eet.getTTLExpirationTime();
    // make sure the get does not change the ttl expiry time
    assertEquals(firstTTLExpiryTime, secondTTLExpiryTime);
    // and does change the idle expiry time
    if ((secondIdleExpiryTime - firstIdleExpiryTime) <= 0) {
      fail("idle should have increased: idle=" + firstIdleExpiryTime + " idle2=" + secondIdleExpiryTime);
    }
  }
  
  public void testRegionExpirationAfterMutate()
  throws CacheException, InterruptedException {

    if (getRegionAttributes().getPartitionAttributes() != null) {
      return;
    }

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();
    LocalRegion region = null;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = (LocalRegion) createRegion(name, attrs);

      region.create(key, value);

      // Now go from no timeout to a timeout
      Region.Entry entry = region.getEntry(key);
      assertEquals(value, entry.getValue());
      region.getAttributesMutator().setRegionIdleTimeout(
          new ExpirationAttributes(12000/*ms*/, ExpirationAction.INVALIDATE));
      region.put(key, value);
      long tilt = System.currentTimeMillis();

      ExpiryTask expiryTask = region.getRegionIdleExpiryTask();
      long mediumExpiryTime = expiryTask.getExpirationTime();
      region.getAttributesMutator().setRegionIdleTimeout(
          new ExpirationAttributes(999000/*ms*/, ExpirationAction.INVALIDATE));
      expiryTask = region.getRegionIdleExpiryTask();
      long hugeExpiryTime = expiryTask.getExpirationTime();
      ExpiryTask.suspendExpiration();
      long shortExpiryTime;
      try {
        region.getAttributesMutator().setRegionIdleTimeout(
            new ExpirationAttributes(20/*ms*/, ExpirationAction.INVALIDATE));
        expiryTask = region.getRegionIdleExpiryTask();
        shortExpiryTime = expiryTask.getExpirationTime();
      } finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt + 20, 10);
      assertTrue("expected hugeExpiryTime=" + hugeExpiryTime + " to be > than mediumExpiryTime=" + mediumExpiryTime,
          (hugeExpiryTime - mediumExpiryTime) > 0);
      assertTrue("expected mediumExpiryTime=" + mediumExpiryTime + " to be > than shortExpiryTime=" + shortExpiryTime,
          (mediumExpiryTime - shortExpiryTime) > 0);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }
  
  /**
   * Tests that a region that remains idle for a given amount of
   * time is invalidated.  Also tests that accessing an entry of a
   * region or a subregion counts as an access.
   */
  public void testRegionIdleInvalidate()
  throws InterruptedException, CacheException {

    if (getRegionAttributes().getPartitionAttributes() != null) {
      // PR does not support INVALID ExpirationAction
      return;
    }
    
    final String name = this.getUniqueName();
    final String subname = this.getUniqueName() + "-SUB";
    final int timeout = 22; // ms
    final Object key = "KEY";
    final Object value = "VALUE";
    
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(new CacheSerializableRunnable("testRegionIdleInvalidate") {
      public void run2() throws CacheException {
        TestCacheListener list = new TestCacheListener() {
            private int createCount = 0;
          public void afterInvalidate2(EntryEvent e) { e.getRegion().getCache().getLogger().info("invalidate2 key="+e.getKey()); }
          public void afterRegionInvalidate2(RegionEvent e) {}
          public void afterUpdate2(EntryEvent e) {
            this.wasInvoked();  // Clear the flag
          }
          public void afterCreate2(EntryEvent e) {
            this.createCount++;
            // we only expect one create; all the rest should be updates
            assertEquals(1, this.createCount);
            this.wasInvoked();  // Clear the flag
          }
        };
        AttributesFactory factory = new AttributesFactory(getRegionAttributes());
        ExpirationAttributes expire =
                new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
        factory.setRegionIdleTimeout(expire);
        factory.setStatisticsEnabled(true);
        RegionAttributes subRegAttrs = factory.create();
        factory.setCacheListener(list);
        RegionAttributes attrs = factory.create();
        
        Region region = null;
        Region sub = null;
        Region.Entry entry = null;
        long tilt;
        System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
        ExpiryTask.suspendExpiration();
        try {
          region = createRegion(name, attrs);
          region.put(key, value);
          tilt = System.currentTimeMillis() + timeout;
          entry = region.getEntry(key);
          assertEquals(value, entry.getValue());
          sub = region.createSubregion(subname, subRegAttrs);
        } 
        finally {
          System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
          ExpiryTask.permitExpiration();
        }
        waitForInvalidate(entry, tilt, 10);
        
        assertTrue(list.waitForInvocation(333));
        

        // The next phase of the test verifies that a get will cause the
        // expiration time to be extended.
        // For this phase we don't worry about actually expiring but just
        // making sure the expiration time gets extended.
        final int EXPIRATION_MS = 9000;
        region.getAttributesMutator().setRegionIdleTimeout(new ExpirationAttributes(EXPIRATION_MS, ExpirationAction.INVALIDATE));
        
        LocalRegion lr = (LocalRegion) region;
        {
          ExpiryTask expiryTask = lr.getRegionIdleExpiryTask();
          region.put(key, value);
          long createExpiry = expiryTask.getExpirationTime();
          long changeTime = Wait.waitForExpiryClockToChange(lr, createExpiry-EXPIRATION_MS);
          region.put(key, "VALUE2");
          long putExpiry = expiryTask.getExpirationTime();
          assertTrue("CLOCK went back in time! Expected putBaseExpiry=" + (putExpiry-EXPIRATION_MS) + " to be >= than changeTime=" + changeTime, (putExpiry-EXPIRATION_MS - changeTime) >= 0);
          assertTrue("expected putExpiry=" + putExpiry + " to be > than createExpiry=" + createExpiry, (putExpiry - createExpiry) > 0);
          changeTime = Wait.waitForExpiryClockToChange(lr, putExpiry-EXPIRATION_MS);
          region.get(key);
          long getExpiry = expiryTask.getExpirationTime();
          assertTrue("CLOCK went back in time! Expected getBaseExpiry=" + (getExpiry-EXPIRATION_MS) + " to be >= than changeTime=" + changeTime, (getExpiry-EXPIRATION_MS - changeTime) >= 0);
          assertTrue("expected getExpiry=" + getExpiry + " to be > than putExpiry=" + putExpiry, (getExpiry - putExpiry) > 0);
        
          changeTime = Wait.waitForExpiryClockToChange(lr, getExpiry-EXPIRATION_MS);
          sub.put(key, value);
          long subPutExpiry = expiryTask.getExpirationTime();
          assertTrue("CLOCK went back in time! Expected subPutBaseExpiry=" + (subPutExpiry-EXPIRATION_MS) + " to be >= than changeTime=" + changeTime, (subPutExpiry-EXPIRATION_MS - changeTime) >= 0);
          assertTrue("expected subPutExpiry=" + subPutExpiry + " to be > than getExpiry=" + getExpiry, (subPutExpiry - getExpiry) > 0);
          changeTime = Wait.waitForExpiryClockToChange(lr, subPutExpiry-EXPIRATION_MS);
          sub.get(key);
          long subGetExpiry = expiryTask.getExpirationTime();
          assertTrue("CLOCK went back in time! Expected subGetBaseExpiry=" + (subGetExpiry-EXPIRATION_MS) + " to be >= than changeTime=" + changeTime, (subGetExpiry-EXPIRATION_MS - changeTime) >= 0);
          assertTrue("expected subGetExpiry=" + subGetExpiry + " to be > than subPutExpiry=" + subPutExpiry, (subGetExpiry - subPutExpiry) > 0);
        }
      }
    });
  }
  
  
  /**
   * Tests that a region expires with a destruction after a
   * given idle time.
   */
  public void testRegionIdleDestroy()
  throws CacheException, InterruptedException {

    if(getRegionAttributes().getPartitionAttributes() != null)
      return;
    
    final String name = this.getUniqueName();
    final int timeout = 22; // ms
    final Object key = "KEY";
    final Object value = "VALUE";
    
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setRegionIdleTimeout(expire);
    factory.setStatisticsEnabled(true);
    RegionAttributes attrs = factory.create();
    
    Region region = null;
    long tilt;
    ExpiryTask.suspendExpiration();
    try {
      System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
      try {
        region = createRegion(name, attrs);
        region.put(key, value);
        tilt = System.currentTimeMillis() + timeout;
        assertFalse(region.isDestroyed());
      }
      finally {
        ExpiryTask.permitExpiration();
      }
      waitForRegionDestroy(region, tilt);
    }
    finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }
  
  /** Tests basic creation and loading of a snapshot from perspective of
   *  single VM
   */
  public static Region preSnapshotRegion = null;
  private final static int MAX_KEYS = 10;

  public void testSnapshot() throws IOException, CacheException, ClassNotFoundException {
    final String name = this.getUniqueName();
    
    // create region in controller
    preSnapshotRegion = createRegion(name);
    
    // create region in other VMs if distributed
    boolean isDistributed = getRegionAttributes().getScope().isDistributed();
    if (isDistributed) {
      Invoke.invokeInEveryVM(new CacheSerializableRunnable("create presnapshot region") {
        public void run2() throws CacheException {
          preSnapshotRegion = createRegion(name);
        }
      });
    }
    
    
    // add data to region in controller
    for (int i = 0; i < MAX_KEYS; i++) {
      if (i == MAX_KEYS-1) {
        // bug 33311 coverage
        preSnapshotRegion.create(String.valueOf(i), null);
      } else {
        preSnapshotRegion.create(String.valueOf(i), new Integer(i));
      }
    }
    
    // save snapshot
    File file = new File(name + ".snap");
    OutputStream out = new FileOutputStream(file);
    
    try {
      preSnapshotRegion.saveSnapshot(out);
      
      assertEquals(new Integer(5), preSnapshotRegion.get("5"));
      
      // destroy all data
      for (int i = 0; i < MAX_KEYS; i++) {
        preSnapshotRegion.destroy(String.valueOf(i));
      }
      
      assertTrue(preSnapshotRegion.keys().size() == 0);
      
      //      DebuggerSupport.waitForJavaDebugger(getLogWriter());
      InputStream in = new FileInputStream(file);
      preSnapshotRegion.loadSnapshot(in);
      
      // test postSnapshot behavior in controller
      remoteTestPostSnapshot(name, true, false);
      
      // test postSnapshot behavior in other VMs if distributed
      if (isDistributed) {
        Invoke.invokeInEveryVM(new CacheSerializableRunnable("postSnapshot") {
          public void run2() throws CacheException {
            RegionTestCase.this.remoteTestPostSnapshot(name, false, false);
          }
        });
      }
    }
    finally {
      file.delete();
    }
  }

  public void testRootSnapshot() throws IOException, CacheException, ClassNotFoundException {
    final String name = this.getUniqueName();
    
    // create region in controller
    preSnapshotRegion = createRootRegion(name, getRegionAttributes());
    
    // create region in other VMs if distributed
    boolean isDistributed = getRegionAttributes().getScope().isDistributed();
    if (isDistributed) {
      Invoke.invokeInEveryVM(new CacheSerializableRunnable("create presnapshot region") {
        public void run2() throws CacheException {
          preSnapshotRegion = createRootRegion(name, getRegionAttributes());
        }
      });
    }
    
    
    // add data to region in controller
    for (int i = 0; i < MAX_KEYS; i++) {
      if (i == MAX_KEYS-1) {
        // bug 33311 coverage
        preSnapshotRegion.create(String.valueOf(i), null);
      } else {
        preSnapshotRegion.create(String.valueOf(i), new Integer(i));
      }
    }
    
    // save snapshot
    File file = new File(name + ".snap");
    OutputStream out = new FileOutputStream(file);
    
    try {
      preSnapshotRegion.saveSnapshot(out);
      
      assertEquals(new Integer(5), preSnapshotRegion.get("5"));
      
      // destroy all data
      for (int i = 0; i < MAX_KEYS; i++) {
        preSnapshotRegion.destroy(String.valueOf(i));
      }
      
      assertTrue(preSnapshotRegion.keys().size() == 0);
      
      LogWriter log = getCache().getLogger();
      log.info("before loadSnapshot");
      //      DebuggerSupport.waitForJavaDebugger(getLogWriter());
      InputStream in = new FileInputStream(file);
      preSnapshotRegion.loadSnapshot(in);
      log.info("after loadSnapshot");
      
      // test postSnapshot behavior in controller
      log.info("before controller remoteTestPostSnapshot");
      remoteTestPostSnapshot(name, true, true);
      log.info("after controller remoteTestPostSnapshot");
      
      // test postSnapshot behavior in other VMs if distributed
      if (isDistributed) {
        log.info("before distributed remoteTestPostSnapshot");
        Invoke.invokeInEveryVM(new CacheSerializableRunnable("postSnapshot") {
          public void run2() throws CacheException {
            RegionTestCase.this.remoteTestPostSnapshot(name, false, true);
          }
        });
        log.info("after distributed remoteTestPostSnapshot");
      }
    }
    finally {
      file.delete();
    }
  }
  
  public void remoteTestPostSnapshot(String name, boolean isController, boolean isRoot)
  throws CacheException {
    assertTrue(preSnapshotRegion.isDestroyed());
    
    try {
      preSnapshotRegion.get("0");
      fail("Should have thrown a RegionReinitializedException");
    } catch (RegionReinitializedException e) {
      // pass
    }
    
    LogWriter log = getCache().getLogger();
    // get new reference to region
    Region postSnapshotRegion = isRoot ? getRootRegion(name) : getRootRegion().getSubregion(name);
    assertNotNull("Could not get reference to reinitialized region", postSnapshotRegion);
    
    boolean expectData = isController 
        || postSnapshotRegion.getAttributes().getMirrorType().isMirrored()
        || postSnapshotRegion.getAttributes().getDataPolicy().isPreloaded();
    log.info("region has " + postSnapshotRegion.keys().size() + " entries");
    assertEquals(expectData ? MAX_KEYS : 0, postSnapshotRegion.keys().size());
    // gets the data either locally or by netSearch
    assertEquals(new Integer(3), postSnapshotRegion.get("3"));
    // bug 33311 coverage
    if (expectData) {
      assertFalse(postSnapshotRegion.containsValueForKey("9"));
      assertTrue(postSnapshotRegion.containsKey("9"));
    }
  }
  
}
