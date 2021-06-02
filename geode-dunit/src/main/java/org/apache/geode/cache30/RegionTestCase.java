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
package org.apache.geode.cache30;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperties;
import static java.lang.System.setProperty;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.cache.ExpirationAction.DESTROY;
import static org.apache.geode.cache.ExpirationAction.INVALIDATE;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.Region.SEPARATOR_CHAR;
import static org.apache.geode.internal.cache.ExpiryTask.permitExpiration;
import static org.apache.geode.internal.cache.ExpiryTask.suspendExpiration;
import static org.apache.geode.internal.cache.LocalRegion.EXPIRY_MS_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.Wait.waitForExpiryClockToChange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionReinitializedException;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.EntryExpiryTask;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.ExpiryTask;
import org.apache.geode.internal.cache.ExpiryTask.ExpiryTaskListener;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * An abstract class whose test methods test the functionality of a region regardless of its scope.
 *
 * <P>
 *
 * This class also contains functionality that is used by subclasses. See
 * {@link #getRegionAttributes}.
 *
 * TODO:davidw: Test {@link CacheStatistics}
 *
 * @since GemFire 3.0
 */
public abstract class RegionTestCase extends JUnit4CacheTestCase {

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    postTearDownRegionTestCase();
  }

  protected void postTearDownRegionTestCase() throws Exception {}

  /**
   * Returns a region with the given name and the attributes for this test.
   *
   * @see #getRegionAttributes
   */
  protected <K, V> Region<K, V> createRegion(String name) throws CacheException {
    RegionFactory<K, V> regionFactory = getCache().createRegionFactory(getRegionAttributes());
    return createRegion(name, regionFactory);
  }

  protected <K, V> Region<K, V> createRootRegion() throws CacheException {
    return createRootRegion(getRegionAttributes());
  }

  /**
   * Returns the attributes of a region to be tested by this test. Note that the decision as to
   * which attributes are used is left up to the concrete subclass.
   */
  protected abstract <K, V> RegionAttributes<K, V> getRegionAttributes();

  /** pauses only if no ack */
  protected void pauseIfNecessary() {}

  protected void pauseIfNecessary(int ms) {}

  /**
   * Make sure all messages done on region r have been processed on the remote side.
   */
  protected void flushIfNecessary(Region r) {
    // Only needed for no-ack regions
  }

  /**
   * Tests that creating an entry in a region actually creates it
   *
   * @see Region#containsKey
   * @see Region#containsValueForKey
   */
  @Test
  public void testContainsKey() throws CacheException {
    String key = this.getUniqueName();
    Region<Object, Object> region = createRegion(key);
    Object value = 42;

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
   * Tests that creating or getting entries at an improper time throws exceptions.
   *
   * @see Region#get
   * @see Region#getEntry
   * @see Region#create
   */
  @Test
  public void testBadRegionAccess() throws CacheException {
    String key = this.getUniqueName();
    Region<Object, Object> region = createRegion(key);

    assertNull(region.get(key));
    assertNull(region.getEntry(key));

    Integer value = 42;
    region.create(key, value);

    try {
      // partitioned regions are logging the EntryExistsException, so emit
      // a directive to ignore it
      logger.info("<ExpectedException action=add>"
          + "org.apache.geode.cache.EntryExistsException" + "</ExpectedException>");
      region.create(key, value);
      fail("Should have thrown an EntryExistsException");

    } catch (EntryExistsException ex) {
      // okay...
    } finally {
      logger.info("<ExpectedException action=remove>"
          + "org.apache.geode.cache.EntryExistsException" + "</ExpectedException>");
    }
  }

  /**
   * Tests that {@link Region#put} on a previously non-existent region entry creates it.
   */
  @Test
  public void testPutNonExistentEntry() throws CacheException {
    String key = this.getUniqueName();
    Region<Object, Object> region = createRegion(key);

    assertNull(region.getEntry(key));

    Object value = 42;
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
    } catch (UnsupportedOperationException uoe) {
      logger
          .info("Region.values() reported UnsupportedOperation");
    }
  }

  /**
   * Indicate whether subregions are supported
   *
   */
  protected boolean supportsSubregions() {
    return true;
  }

  /**
   * Indicate whether localDestroy and localInvalidate are supported
   *
   * @return true if they are supported
   */
  protected boolean supportsLocalDestroyAndLocalInvalidate() {
    return true;
  }

  /**
   * Tests that sending <code>null</code> to various APIs throws the appropriate exception.
   */
  @Test
  public void testNulls() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String key = this.getUniqueName();
    Region<Object, Object> region = createRegion(key);

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

    assertEquals("/", SEPARATOR);
    assertEquals('/', SEPARATOR_CHAR);
    try {
      region.createSubregion("BAD" + SEPARATOR + "TEST", region.getAttributes());
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
      region.put(null, 42);
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
   * Tests creating subregions. Note that this tests accesses the Region's
   * {@link Region#getStatistics statistics}, so the region must have been created with statistics
   * enabled.
   */
  @Test
  public void testCreateSubRegions() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();

    RegionAttributes<Object, Object> attrs = getRegionAttributes();
    RegionFactory<Object, Object> regionFactory = getCache().createRegionFactory(attrs);
    regionFactory.setStatisticsEnabled(true);

    Region<Object, Object> region = createRegion(name, regionFactory);


    attrs = region.getAttributes();

    CacheStatistics stats = region.getStatistics();
    long lastAccessed = stats.getLastAccessedTime();
    long lastModified = stats.getLastModifiedTime();

    try {
      region.createSubregion(name + SEPARATOR + "BAD", attrs);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      CacheStatistics stats2 = region.getStatistics();
      assertEquals(lastAccessed, stats2.getLastAccessedTime());
      assertEquals(lastModified, stats2.getLastModifiedTime());
    }

    Region subregion = region.createSubregion(name, attrs);
    assertTrue(attrs != subregion.getAttributes());

    Set subregions = region.subregions(false);
    assertEquals(1, subregions.size());
    assertEquals(subregion, subregions.iterator().next());
  }

  public static boolean entryIsLocal(Region.Entry re) {
    if (re instanceof EntrySnapshot) {
      return ((EntrySnapshot) re).wasInitiallyLocal();
    } else {
      return re.isLocal();
    }
  }

  /**
   * Tests {@link Region#destroy destroying} an entry and attempting to access it afterwards.
   */
  @Test
  public void testDestroyEntry() throws CacheException {
    String key = this.getUniqueName();
    Object value = 42;

    Region<Object, Object> region = createRegion(key);

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
    logger
        .info("Found entry for destroyed key: " + entry2);
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
  }

  /**
   * Tests destroying an entire region and that accessing it after it has been destory causes a
   * {@link RegionDestroyedException}.
   *
   * @see Region#destroyRegion
   */
  @Test
  public void testDestroyRegion() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";

    Region<Object, Object> region = createRegion(name);
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
      region.entrySet(false);
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

    assertEquals(SEPARATOR + "root" + SEPARATOR + name, region.getFullPath());
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
      region.keySet();
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
   * Tests the {@link Region#entrySet(boolean)} method without recursion
   */
  @Test
  public void testEntries() throws CacheException {
    String name = this.getUniqueName();
    Region<Object, Object> region = createRegion(name);
    assertEquals(0, region.entrySet(true).size());
    assertEquals(0, region.entrySet(false).size());

    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");

    {
      Set entries = region.entrySet(false);
      assertEquals(3, entries.size());

      Set<Object> keys = new HashSet<>(Arrays.asList("A", "B", "C"));
      Iterator iter = entries.iterator();
      for (int i = 0; i < 3; i++) {
        assertTrue(iter.hasNext());
        assertTrue(keys.remove(((Region.Entry) iter.next()).getKey()));
      }
      assertFalse(iter.hasNext());
    }

    {
      Set entries = region.entrySet(true);
      assertEquals(3, entries.size());

      Set<Object> keys = new HashSet<>(Arrays.asList("A", "B", "C"));
      Iterator iter = entries.iterator();
      for (int i = 0; i < 3; i++) {
        assertTrue(iter.hasNext());
        assertTrue(keys.remove(((Region.Entry) iter.next()).getKey()));
      }
      assertFalse(iter.hasNext());
    }
    {
      Iterator iter = region.entrySet(false).iterator();
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
   * Tests the {@link Region#entrySet} method with recursion
   */
  @Test
  public void testEntriesRecursive() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    Region<String, String> region = createRegion(name);

    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");
    Region<String, String> sub = region.createSubregion("SUB", region.getAttributes());
    sub.put("D", "d");
    sub.put("E", "e");
    sub.put("F", "f");

    {
      Set entries = region.entrySet(true);
      assertEquals(6, entries.size());


      Set<Object> keys = new HashSet<>(Arrays.asList("A", "B", "C", "D", "E", "F"));
      Iterator iter = entries.iterator();
      for (int i = 0; i < 6; i++) {
        assertTrue("!hasNext, i=" + i, iter.hasNext());
        assertTrue("remove returned false, i=" + i,
            keys.remove(((Region.Entry) iter.next()).getKey()));
      }
      assertFalse(iter.hasNext());
    }

    {
      Set<Region.Entry<?, ?>> entries = region.entrySet(true);
      assertEquals(6, entries.size());


      Set<Object> keys = new HashSet<>(Arrays.asList("A", "B", "C", "D", "E", "F"));
      Iterator<Region.Entry<?, ?>> iter = entries.iterator();
      for (int i = 0; i < 6; i++) {
        assertTrue("!hasNext, i=" + i, iter.hasNext());
        assertTrue("remove returned false, i=" + i,
            keys.remove(iter.next().getKey()));
      }
      assertFalse(iter.hasNext());
    }

    {
      Iterator iter = region.entrySet(true).iterator();
      Region.Entry<String, String> entry = (Region.Entry) iter.next();
      String key = entry.getKey();
      region.destroy(key);
      assertFalse(region.containsKey(key));
      assertTrue(entry.isDestroyed());
    }

  }


  /**
   * Tests the {@link Region#getCache} method (for what it's worth)
   */
  @Test
  public void testGetCache() throws CacheException {
    String name = this.getUniqueName();
    Region<Object, Object> region = createRegion(name);
    assertSame(this.getCache(), region.getCache());
  }

  /**
   * Tests the {@link Region#getName} method
   */
  @Test
  public void testGetName() throws CacheException {
    String name = this.getUniqueName();
    Region<Object, Object> region = createRegion(name);
    assertEquals(name, region.getName());

    assertEquals("root", region.getParentRegion().getName());
  }

  /**
   * Tests the {@link Region#getFullPath} method
   */
  @Test
  public void testGetPathFromRoot() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();

    Region<Object, Object> region = createRegion(name);
    String fullPath = SEPARATOR + "root" + SEPARATOR + name;
    assertEquals(fullPath, region.getFullPath());
    assertEquals(SEPARATOR + "root", region.getParentRegion().getFullPath());
    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(region.getAttributes());
    Region sub = regionFactory.createSubregion(region, "SUB");
    assertEquals(fullPath + SEPARATOR + "SUB", sub.getFullPath());
  }

  /**
   * Tests the {@link Region#getParentRegion} method
   */
  @Test
  public void testGetParentRegion() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();

    Region<Object, Object> region = createRegion(name);
    assertEquals(getRootRegion(), region.getParentRegion());

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(region.getAttributes());
    Region sub = regionFactory.createSubregion(region, "SUB");
    assertEquals(region, sub.getParentRegion());
    assertSame(sub, region.getSubregion("SUB"));
    assertNotNull(sub.getAttributes());
  }

  /**
   * Tests a <code>Region</code>'s user attribute
   *
   * @see Region#setUserAttribute
   */
  @Test
  public void testRegionUserAttribute() throws CacheException {
    String name = this.getUniqueName();
    Object value = "USER_ATTRIBUTE";

    Region<Object, Object> region = createRegion(name);
    assertNull(region.getUserAttribute());

    region.setUserAttribute(value);
    assertEquals(value, region.getUserAttribute());
  }

  /**
   * Tests a region entry's user attribute
   */
  @Test
  public void testEntryUserAttribute() throws CacheException {
    String name = this.getUniqueName();
    String key = "KEY";

    String attr = "USER_ATTRIBUTE";

    Region<Object, Object> region = createRegion(name);
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
  @Test
  public void testInvalidateEntry() throws CacheException {
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";

    Region<Object, Object> region = createRegion(name);
    region.put(key, value);

    long beforeInvalidates = getCache()
        .getCachePerfStats().getInvalidates();
    Region.Entry entry = region.getEntry(key);
    region.invalidate(key);
    if (entry.isLocal()) {
      assertNull(entry.getValue());
    }
    assertNull(region.get(key));
    long afterInvalidates = getCache()
        .getCachePerfStats().getInvalidates();
    assertEquals("Invalidate CachePerfStats incorrect", beforeInvalidates + 1, afterInvalidates);
  }

  /**
   * Tests invalidating an entire region
   */
  @Test
  public void testInvalidateRegion() throws CacheException {
    String name = this.getUniqueName();

    Region<Object, Object> region = createRegion(name);
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
      assertFalse("containsValueForKey returned true for key " + key,
          region.containsValueForKey(key));
      assertTrue("containsKey returned false for key " + key, region.containsKey(key));
    }
  }

  /**
   * Tests the {@link Region#keySet()} method.
   */
  @Test
  public void testKeys() throws CacheException {
    String name = this.getUniqueName();

    Region<Object, Object> region = createRegion(name);
    assertEquals(0, region.keySet().size());

    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");

    {
      Set keys = region.keySet();
      assertEquals(3, keys.size());

      assertTrue(keys.contains("A"));
      assertTrue(keys.contains("B"));
      assertTrue(keys.contains("C"));
    }
  }

  /**
   * Tests {@link Region#localDestroy locally destroying} an entry and attempting to access it
   * afterwards. (Not too useful with a <code>LOCAL</code> region.)
   */
  @Test
  public void testLocalDestroyEntry() throws CacheException {
    if (!supportsLocalDestroyAndLocalInvalidate()) {
      return;
    }
    String key = this.getUniqueName();
    Object value = 42;

    Region<Object, Object> region = createRegion(key);

    boolean isMirrored = getRegionAttributes().getMirrorType().isMirrored();

    try {
      region.localDestroy(key);
      if (isMirrored) {
        fail("Should have thrown an IllegalStateException");
      }
      fail("Should have thrown an EntryNotFoundException");
    } catch (EntryNotFoundException ex) {
      // pass...
    } catch (IllegalStateException ex) {
      if (!isMirrored) {
        throw ex;
      } else {
        return; // abort test
      }
    }

    region.put(key, value);

    Region.Entry entry = region.getEntry(key);
    assertNotNull(entry);

    region.localDestroy(key);

    assertNull(region.getEntry(key));
    assertTrue(entry.isDestroyed());
    assertEquals(0, region.keySet().size());

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
   * Tests locally destroying an entire region and that accessing it after it has been destory
   * causes a {@link RegionDestroyedException}.
   *
   * @see Region#localDestroyRegion
   */
  @Test
  public void testLocalDestroyRegion() throws CacheException {
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";

    Region<Object, Object> region = createRegion(name);
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
      region.entrySet(false);
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

    assertEquals(SEPARATOR + "root" + SEPARATOR + name, region.getFullPath());

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
      region.keySet();
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
   * Tests closing a region, and checks different behavior when this is a disk region with
   * persistBackup.
   */
  @Test
  public void testCloseRegion() throws CacheException {
    // @todo added a remote region to make sure close just does a localDestroy

    String name = this.getUniqueName();

    RegionFactory<Object, Object> regionFactory =
        getCache().createRegionFactory(getRegionAttributes());

    TestCacheListener<Object, Object> list = new TestCacheListener<Object, Object>() {
      @Override
      public void afterCreate2(EntryEvent event) {
        // do nothing
      }

      @Override
      public void afterRegionDestroy2(RegionEvent re) {
        assertEquals(Operation.REGION_CLOSE, re.getOperation());
      }

      @Override
      public void close2() {
        // okay
      }
    };

    regionFactory.addCacheListener(list);

    Region<Object, Object> region = createRegion(name, regionFactory);

    File diskDir = null;
    if (getRegionAttributes().getDataPolicy().withPersistence()) {
      diskDir = getCache().findDiskStore(getRegionAttributes().getDiskStoreName()).getDiskDirs()[0];
      // @todo We no longer start with a clean slate because the DiskStore hangs around.
      // If we want a clean slate then we need to destroy the DiskStore after each
      // test completes.
      // assert that if this is a disk region, the disk dirs are empty
      // to make sure we start with a clean slate
      getCache().getLogger().info("list=" + Arrays.toString(diskDir.list()));
      // assertIndexDetailsEquals("list="+Arrays.toString(diskDir.list()),
      // 0, diskDir.list().length);
    }

    for (int i = 0; i < 1000; i++) {
      region.put(i, String.valueOf(i));
    }

    // reset wasInvoked after creates
    assertTrue(list.wasInvoked());

    // assert that if this is a disk region, the disk dirs are not empty
    if (getRegionAttributes().getDataPolicy().withPersistence()) {
      assertTrue(diskDir.list().length > 0);
    }
    region.getAttributes().getDataPolicy().withPersistence();
    region.close();

    // assert that if this is a disk region, the disk dirs are not empty
    if (getRegionAttributes().getDataPolicy().withPersistence()) {
      assertTrue(diskDir.list().length > 0);
    }

    assertTrue(list.waitForInvocation(333));
    assertTrue(list.isClosed());
    assertTrue(region.isDestroyed());

    // if this is a disk region, then check to see if recreating the region
    // repopulates with data
    RegionFactory<Object, Object> regionFactory2 =
        getCache().createRegionFactory(getRegionAttributes());
    region = createRegion(name, regionFactory2);

    if (getRegionAttributes().getDataPolicy().withPersistence()) {
      for (int i = 0; i < 1000; i++) {
        Region.Entry entry = region.getEntry(i);
        assertNotNull("entry " + i + " not found", entry);
        assertEquals(String.valueOf(i), entry.getValue());
      }
      assertEquals(1000, region.keySet().size());
    } else {
      assertEquals(0, region.keySet().size());
    }

    region.localDestroyRegion();
  }


  /**
   * Tests locally invalidating a region entry
   */
  @Test
  public void testLocalInvalidateEntry() throws CacheException {
    if (!supportsLocalDestroyAndLocalInvalidate()) {
      return;
    }
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";

    Region<Object, Object> region = createRegion(name);
    region.put(key, value);

    Region.Entry entry = region.getEntry(key);
    boolean isMirrorKeysValues = getRegionAttributes().getMirrorType().isKeysValues();
    try {
      region.localInvalidate(key);
      if (isMirrorKeysValues) {
        fail("Should have thrown an IllegalStateException");
      }
    } catch (IllegalStateException e) {
      if (!isMirrorKeysValues) {
        throw e;
      } else {
        return; // abort test
      }
    }
    assertNull(entry.getValue());
    assertNull(region.get(key));
  }

  /**
   * Tests locally invalidating an entire region
   */
  @Test
  public void testLocalInvalidateRegion() throws CacheException {
    String name = this.getUniqueName();

    Region<Object, Object> region = createRegion(name);
    region.put("A", "a");
    region.put("B", "b");
    region.put("C", "c");

    boolean isKV = getRegionAttributes().getMirrorType().isKeysValues();
    try {
      region.localInvalidateRegion();
      if (isKV) {
        fail("Should have thrown an IllegalStateException");
      }
    } catch (IllegalStateException e) {
      if (!isKV) {
        throw e;
      } else {
        return; // abort test
      }
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
  @Test
  public void testSubregions() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }
    String name = this.getUniqueName();
    Region<Object, Object> region = createRegion(name);

    assertEquals(0, region.subregions(false).size());

    region.createSubregion("A", region.getAttributes());
    region.createSubregion("B", region.getAttributes());
    region.createSubregion("C", region.getAttributes());

    {
      Set subregions = region.subregions(false);
      assertEquals(3, subregions.size());

      Set<Object> names = new HashSet<>(Arrays.asList("A", "B", "C"));
      Iterator iter = subregions.iterator();
      for (int i = 0; i < 3; i++) {
        assertTrue(iter.hasNext());
        assertTrue(names.remove(((Region) iter.next()).getName()));
      }
      assertFalse(iter.hasNext());
    }

  }

  /**
   * Tests the {@link Region#subregions} method with recursion
   */
  @Test
  public void testSubRegionsRecursive() throws CacheException {
    if (!supportsSubregions()) {
      return;
    }

    String name = this.getUniqueName();
    Region<Object, Object> region = createRegion(name);
    RegionFactory<Object, Object> regionFactory =
        getCache().createRegionFactory(region.getAttributes());
    Region A = regionFactory.createSubregion(region, "A");
    Region B = regionFactory.createSubregion(region, "B");
    Region C = regionFactory.createSubregion(region, "C");

    regionFactory.createSubregion(A, "D");
    regionFactory.createSubregion(B, "E");
    regionFactory.createSubregion(C, "F");


    {
      Set subRegions = region.subregions(true);
      assertEquals(6, subRegions.size());

      Set<Object> names = new HashSet<>(Arrays.asList("A", "B", "C", "D", "E", "F"));
      Iterator iter = subRegions.iterator();
      for (int i = 0; i < 6; i++) {
        assertTrue(iter.hasNext());
        assertTrue(names.remove(((Region) iter.next()).getName()));
      }
      assertFalse(iter.hasNext());
    }
  }

  /**
   * Tests the {@link Region#values} method without recursion
   */
  @Test
  public void testValues() throws CacheException {
    String name = this.getUniqueName();
    LogService.getLogger().info("testValues region name is " + name);
    Region<Object, Object> region = createRegion(name);
    assertEquals(0, region.values().size());

    region.create("A", null);

    {
      Set<Object> values = new TreeSet<>(region.values());
      assertTrue(values.isEmpty());
      Iterator itr = values.iterator();
      assertFalse(itr.hasNext());
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
      Set<Object> values = new TreeSet<>(region.values());
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
      Set<Object> values = new TreeSet<>(region.values());
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
  private static final String WAIT_PROPERTY = "UpdatePropagationDUnitTest.maxWaitTime";
  private static final int WAIT_DEFAULT = 60000;


  private static final int SLOP = 1000; // milliseconds

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
   * Since <em>tilt</em> is the earliest time we expect, one must check the current time
   * <em>before</em> invoking the operation intended to keep the entry alive.
   *
   * @param entry entry we want to be invalidated
   * @param p_tilt earliest time we expect to see the invalidate
   */
  private void waitForInvalidate(Region.Entry entry, long p_tilt) {
    waitForInvalidate(entry, p_tilt, 100);
  }

  /**
   * Since <em>tilt</em> is the earliest time we expect, one must check the current time
   * <em>before</em> invoking the operation intended to keep the entry alive.
   *
   * @param entry entry we want to be invalidated
   * @param p_tilt earliest time we expect to see the invalidate
   * @param pauseMs the number of milliseconds to pause before checking again
   */
  private void waitForInvalidate(Region.Entry entry, long p_tilt, int pauseMs) {
    long tilt = p_tilt;
    // up until the time that the expiry fires, the entry
    // better not be null...
    if (entry == null) {
      // the entire wait routine was called very late, and
      // we have no entry? That's ok.
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
        logger
            .warn("Entry invalidated sloppily " + "now=" + now + " tilt=" + tilt + " delta = "
                + (tilt - now));
        break;
      }
      fail("Entry invalidated prematurely " + "now=" + now + " tilt=" + tilt + " delta = "
          + (tilt - now));
    }

    // After the timeout passes, we will tolerate a slight
    // lag before the invalidate becomes visible (due to
    // system loading)
    // Slight lag? WAIT_DEFAULT is 60,000 ms. Many of our tests configure 20ms expiration.
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT);
    tilt += maxWaitTime;
    for (;;) {
      if (fetchEntryValue(entry) == null) {
        break;
      }
      if (System.currentTimeMillis() > tilt) {
        if (fetchEntryValue(entry) == null) {
          break;
        }
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
   * Since <em>tilt</em> is the earliest time we expect, one must check the current time
   * <em>before</em> invoking the operation intended to keep the entry alive.
   *
   * @param entry entry we want to be invalidated
   * @param p_tilt earliest time we expect to see the invalidate
   */
  private void waitForDestroy(Region.Entry entry, long p_tilt) {
    waitForDestroy(entry, p_tilt, 100);
  }

  /**
   * Since <em>tilt</em> is the earliest time we expect, one must check the current time
   * <em>before</em> invoking the operation intended to keep the entry alive.
   *
   * @param entry entry we want to be invalidated
   * @param p_tilt earliest time we expect to see the invalidate
   * @param pauseMs the number of milliseconds to pause before checking again
   */
  private void waitForDestroy(Region.Entry entry, long p_tilt, int pauseMs) {
    long tilt = p_tilt;
    // up until the time that the expiry fires, the entry
    // better not be null...
    for (;;) {
      long now = System.currentTimeMillis();
      if (now >= tilt) {
        break;
      }
      if (!isEntryDestroyed(entry)) {
        Wait.pause(pauseMs);
        continue;
      }
      if (now >= tilt - SLOP) {
        logger
            .warn("Entry destroyed sloppily " + "now=" + now + " tilt=" + tilt + " delta = "
                + (tilt - now));
        break;
      }
      fail("Entry destroyed prematurely" + "now=" + now + " tilt=" + tilt + " delta = "
          + (tilt - now));
    }

    // After the timeout passes, we will tolerate a slight
    // lag before the destroy becomes visible (due to
    // system loading)
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT);

    tilt += maxWaitTime;
    for (;;) {
      if (isEntryDestroyed(entry)) {
        break;
      }
      Assert.assertTrue(System.currentTimeMillis() <= tilt, "Entry failed to destroy");
      Wait.pause(pauseMs);
    }
  }

  /**
   * Since <em>tilt</em> is the earliest time we expect, one must check the current time
   * <em>before</em> invoking the operation intended to keep the region alive.
   *
   * @param region region we want to be destroyed
   * @param p_tilt earliest time we expect to see the destroy
   */

  private void waitForRegionDestroy(Region<Object, Object> region, long p_tilt) {
    long tilt = p_tilt;
    // up until the time that the expiry fires, the entry
    // better not be null...

    for (;;) {
      long now = System.currentTimeMillis();
      if (now >= tilt) {
        break;
      }
      if (!region.isDestroyed()) {
        Wait.pause(10);
        continue;
      }
      if (now >= tilt - SLOP) {
        logger
            .warn("Region destroyed sloppily " + "now=" + now + " tilt=" + tilt + " delta = "
                + (tilt - now));
        break;
      }
      fail("Region destroyed prematurely" + "now=" + now + " tilt=" + tilt + " delta = "
          + (tilt - now));
    }

    // After the timeout passes, we will tolerate a slight
    // lag before the destroy becomes visible (due to
    // system loading)
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT);
    tilt += maxWaitTime;
    for (;;) {
      if (region.isDestroyed()) {
        break;
      }
      Assert.assertTrue(System.currentTimeMillis() <= tilt, "Region failed to destroy");
      Wait.pause(10);
    }
  }

  /**
   * Tests that an entry in a region expires with an invalidation after a given time to live.
   */
  @Test
  public void testEntryTtlInvalidate() throws CacheException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms!
    final String key = "KEY";
    final String value = "VALUE";
    RegionFactory<Object, Object> regionFactory =
        getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    regionFactory.setEntryTimeToLive(expire);
    regionFactory.setStatisticsEnabled(true);
    Region<Object, Object> region;
    /*
     * Crank up the expiration so test runs faster. This property only needs to be set while the
     * region is created
     */

    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, regionFactory);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    ExpiryTask.suspendExpiration();
    Region.Entry entry;
    long tilt;
    try {
      region.put(key, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key);
      assertNotNull(entry.getValue());
    } finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);
  }

  /**
   * Verify that special entries expire but other entries in the region don't
   */
  @Test
  public void testCustomEntryTtl1() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms!
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";

    RegionFactory<Object, Object> regionFactory =
        getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    regionFactory.setCustomEntryTimeToLive(new TestExpiry<>(key2, expire));
    regionFactory.setStatisticsEnabled(true);

    Region<Object, Object> region;
    /*
     * Crank up the expiration so test runs faster. This property only needs to be set while the
     * region is created
     */
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, regionFactory);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    // Random values should not expire
    region.put(key1, value);
    await().atLeast(timeout * 2, MILLISECONDS).until(() -> region.get(key1).equals(value));

    // key2 *should* expire
    ExpiryTask.suspendExpiration();
    Region.Entry entry;
    long tilt;
    try {
      region.put(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    assert (region.get(key1).equals(value));
  }


  /**
   * Verify that special entries don't expire but other entries in the region do
   */
  @Test
  public void testCustomEntryTtl2() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";

    RegionFactory<Object, Object> regionFactory =
        getCache().createRegionFactory(getRegionAttributes());

    ExpirationAttributes expire2 = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    regionFactory.setCustomEntryTimeToLive(new TestExpiry<>(key2, expire2));
    regionFactory.setStatisticsEnabled(true);
    TestCacheListenerCustom list = new TestCacheListenerCustom();
    regionFactory.addCacheListener(list);

    Region<Object, Object> region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, regionFactory);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value);

    // This value should NOT expire.
    Wait.pause(timeout * 2);
    assertEquals(region.get(key1), value);

    // This value SHOULD expire

    ExpiryTask.suspendExpiration();
    Region.Entry entry;
    long tilt;
    try {
      region.create(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertTrue(list.waitForInvocation(5000));
      assertNotNull(entry.getValue());
    } finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertEquals(region.get(key1), value);

    // Do it again with a put (I guess)
    ExpiryTask.suspendExpiration();
    try {
      region.put(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertEquals(region.get(key1), value);
  }

  private final AtomicInteger eventCount = new AtomicInteger();

  /**
   * Expire an entry with a custom expiration. Set a new custom expiration, create the same entry
   * again, make sure it observes the <em>new</em> expiration
   */
  @Test
  public void testCustomEntryTtl3() {

    final String name = this.getUniqueName();
    final int timeout1 = 20; // ms
    final int timeout2 = 40;
    final String key1 = "KEY1";
    final String value1 = "VALUE1";
    final String value2 = "VALUE2";

    RegionFactory<Object, Object> regionFactory =
        getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire1 = new ExpirationAttributes(timeout1, ExpirationAction.INVALIDATE);
    regionFactory.setCustomEntryTimeToLive(new TestExpiry<>(key1, expire1));
    regionFactory.setStatisticsEnabled(true);
    TestCacheListenerEventCount list = new TestCacheListenerEventCount();
    // Disk regions are VERY slow, so we need to wait for the event...

    eventCount.set(0);
    regionFactory.addCacheListener(list);

    Region<Object, Object> region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, regionFactory);

      suspendExpiration();
      Region.Entry entry;
      eventCount.set(0);
      long tilt1;
      long tilt2;
      try {
        region.create(key1, value1);
        tilt1 = currentTimeMillis() + timeout1;
        entry = region.getEntry(key1);
        assertTrue(list.waitForInvocation(1000));
        Assert.assertTrue(value1.equals(entry.getValue()));
      } finally {
        permitExpiration();
      }
      waitForInvalidate(entry, tilt1, timeout1 / 2);
      await().alias("eventCount never became 1").until(() -> eventCount.get() == 1);
      eventCount.set(0);

      // Do it again with a put (I guess)
      suspendExpiration();
      try {
        region.put(key1, value1);
        tilt1 = currentTimeMillis() + timeout1;
        entry = region.getEntry(key1);
        Assert.assertTrue(value1.equals(entry.getValue()));
        assertTrue(list.waitForInvocation(10 * 1000));
      } finally {
        permitExpiration();
      }
      waitForInvalidate(entry, tilt1, timeout1 / 2);
      await().alias("eventCount never became 1").until(() -> eventCount.get() == 1);
      eventCount.set(0);

      // Change custom expiry for this region now...
      final String key2 = "KEY2";
      AttributesMutator mutt = region.getAttributesMutator();
      ExpirationAttributes expire2 =
          new ExpirationAttributes(timeout2, INVALIDATE);
      mutt.setCustomEntryTimeToLive(new TestExpiry<>(key2, expire2));

      suspendExpiration();
      try {
        region.put(key1, value1);
        region.put(key2, value2);
        tilt1 = currentTimeMillis() + timeout1;
        tilt2 = tilt1 + timeout2 - timeout1;
        entry = region.getEntry(key1);
        Assert.assertTrue(value1.equals(entry.getValue()));
        entry = region.getEntry(key2);
        Assert.assertTrue(value2.equals(entry.getValue()));
        assertTrue(list.waitForInvocation(1000));
      } finally {
        permitExpiration();
      }
      waitForInvalidate(entry, tilt2, timeout2 / 2);
      await().alias("eventCount never became 1").until(() -> eventCount.get() == 1);
      eventCount.set(0);
      // key1 should not be invalidated since we mutated to custom expiry to only expire key2
      entry = region.getEntry(key1);
      Assert.assertTrue(value1.equals(entry.getValue()));
      // now mutate back to key1 and change the action
      ExpirationAttributes expire3 = new ExpirationAttributes(timeout1, DESTROY);
      mutt.setCustomEntryTimeToLive(new TestExpiry<>(key1, expire3));
      waitForDestroy(entry, tilt1, timeout1 / 2);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  /**
   * Configure entry expiration with a ttl time. Create an entry and records its scheduled
   * expiration time. Then mutate the region expiration configuration and confirm that the entry's
   * expiration time is rescheduled.
   */
  @Test
  public void testEntryTtl3() {
    final String name = this.getUniqueName();
    // test no longer waits for this expiration to happen
    final int timeout1 = 500 * 1000; // ms
    final int timeout2 = 2000 * 1000; // ms
    final String key1 = "KEY1";
    final String value1 = "VALUE1";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire1 = new ExpirationAttributes(timeout1, INVALIDATE);
    factory.setEntryTimeToLive(expire1);
    factory.setStatisticsEnabled(true);
    TestCacheListenerEventCount list = new TestCacheListenerEventCount();
    eventCount.set(0);
    factory.addCacheListener(list);


    LocalRegion region;
    setProperty(EXPIRY_MS_PROPERTY, "true");
    try {
      region = (LocalRegion) createRegion(name, factory);
    } finally {
      getProperties().remove(EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value1);
    EntryExpiryTask eet = region.getEntryExpiryTask(key1);
    final long firstExpiryTime = eet.getExpirationTime();

    AttributesMutator mutt = region.getAttributesMutator();
    ExpirationAttributes expire2 = new ExpirationAttributes(timeout2, INVALIDATE);
    mutt.setEntryTimeToLive(expire2);
    eet = region.getEntryExpiryTask(key1);
    final long secondExpiryTime = eet.getExpirationTime();
    if ((secondExpiryTime - firstExpiryTime) <= 0) {
      fail(
          "expiration time should have been greater after changing region config from 500 to 2000. firstExpiryTime="
              + firstExpiryTime + " secondExpiryTime=" + secondExpiryTime);
    }

    // now set back to be more recent
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire3 = new ExpirationAttributes(timeout1, INVALIDATE);
    mutt.setEntryTimeToLive(expire3);
    eet = region.getEntryExpiryTask(key1);
    final long thirdExpiryTime = eet.getExpirationTime();
    assertEquals(firstExpiryTime, thirdExpiryTime);
    // confirm that it still has not expired
    assertEquals(0, eventCount.get());

    // now set it to a really short time and make sure it expires immediately
    waitForExpiryClockToChange(region);
    final Region.Entry entry = region.getEntry(key1);
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire4 = new ExpirationAttributes(1, INVALIDATE);
    mutt.setEntryTimeToLive(expire4);

    await().alias("entry never became invalid").until(() -> fetchEntryValue(entry) == null);
    await().alias("eventCount never became 1").until(() -> eventCount.get() == 1);

    eventCount.set(0);
  }

  /**
   * Tests that an entry whose value is loaded into a region expires with an invalidation after a
   * given time to live.
   */
  @Test
  public void testEntryFromLoadTtlInvalidate() throws CacheException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms!
    final String key = "KEY";
    final String value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setEntryTimeToLive(expire);
    factory.setStatisticsEnabled(true);
    factory.setCacheLoader(new TestCacheLoader<Object, Object>() {
      @Override
      public Object load2(LoaderHelper helper) {
        return value;
      }
    });

    Region<Object, Object> region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, factory);

      ExpiryTask.suspendExpiration();
      Region.Entry entry;
      long tilt;
      try {
        region.get(key);
        tilt = System.currentTimeMillis() + timeout;
        entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      } finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  /**
   * Tests that an entry in a region expires with a destroy after a given time to live.
   */
  @Test
  public void testEntryTtlDestroy() throws CacheException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key = "KEY";
    final String value = "VALUE";

    AttributesFactory<Object, Object> factory = new AttributesFactory<>(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setEntryTimeToLive(expire);
    factory.setStatisticsEnabled(true);
    RegionAttributes<Object, Object> attrs = factory.create();


    Region<Object, Object> region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, attrs);

      ExpiryTask.suspendExpiration();
      Region.Entry entry;
      long tilt;
      try {
        region.put(key, value);
        tilt = System.currentTimeMillis();
        entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      } finally {
        ExpiryTask.permitExpiration();
      }
      waitForDestroy(entry, tilt);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }


  /**
   * Tests that a region expires with an invalidation after a given time to live.
   */
  @Test
  public void testRegionTtlInvalidate() throws CacheException {

    if (getRegionAttributes().getPartitionAttributes() != null) {
      return;
    }

    VM vm0 = VM.getVM(0);
    final String name = this.getUniqueName();

    vm0.invoke("testRegionTtlInvalidate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        final int timeout = 22; // ms
        final Object key = "KEY";
        final Object value = "VALUE";

        RegionFactory<Object, Object> factory =
            getCache().createRegionFactory(getRegionAttributes());
        ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
        factory.setRegionTimeToLive(expire);
        factory.setStatisticsEnabled(true);


        Region<Object, Object> region;
        Region.Entry entry;
        long tilt;
        ExpiryTask.suspendExpiration();
        try {
          System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
          try {
            region = createRegion(name, factory);
            region.put(key, value);
            region.put("k2", "v2");
            tilt = System.currentTimeMillis() + timeout;
            entry = region.getEntry(key);
            assertNotNull(entry.getValue());
          } finally {
            System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
          }
        } finally {
          ExpiryTask.permitExpiration();
        }
        waitForInvalidate(entry, tilt, 10);
        waitForInvalidate(region.getEntry("k2"), tilt, 10);
      }
    });
  }

  /**
   * Tests that a region expires with a destruction after a given time to live.
   */
  @Test
  public void testRegionTtlDestroy() throws CacheException {

    if (getRegionAttributes().getPartitionAttributes() != null) {
      return;
    }

    final String name = this.getUniqueName();
    final int timeout = 22; // ms
    final Object key = "KEY";
    final Object value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setRegionTimeToLive(expire);
    factory.setStatisticsEnabled(true);


    Region<Object, Object> region;
    long tilt;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    ExpiryTask.suspendExpiration();
    try {
      try {
        region = createRegion(name, factory);
        assertFalse(region.isDestroyed());
        tilt = System.currentTimeMillis() + timeout;
        region.put(key, value);
        Region.Entry entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      } finally {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    } finally {
      ExpiryTask.permitExpiration();
    }
    waitForRegionDestroy(region, tilt);
  }

  /**
   * Tests that an entry in a local region that remains idle for a given amount of time is
   * invalidated.
   */
  @Test
  public void testEntryIdleInvalidate() throws CacheException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key = "KEY";
    final String value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setEntryIdleTimeout(expire);
    factory.setStatisticsEnabled(true);
    TestCacheListenerCustom list = new TestCacheListenerCustom();
    factory.addCacheListener(list);


    Region<Object, Object> region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, factory);

      ExpiryTask.suspendExpiration();
      Region.Entry entry;
      long tilt;
      try {
        region.create(key, value);
        tilt = System.currentTimeMillis() + timeout;
        assertTrue(list.waitForInvocation(333));
        entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      } finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt);

      ExpiryTask.suspendExpiration();
      try {
        region.put(key, value);
        tilt = System.currentTimeMillis() + timeout;
        entry = region.getEntry(key);
        assertNotNull(entry.getValue());
      } finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  static class TestExpiry<K, V> implements CustomExpiry<K, V>, Declarable {

    final String special;
    final ExpirationAttributes specialAtt;

    TestExpiry(String flagged, ExpirationAttributes att) {
      this.special = flagged;
      this.specialAtt = att;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.cache.CustomExpiry#getExpiry(org.apache.geode.cache.Region.Entry)
     */
    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      if (entry.getKey().equals(special)) {
        return specialAtt;
      }
      return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.cache.Declarable#init(java.util.Properties)
     */
    @Override
    public void init(Properties props) {}

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.cache.CacheCallback#close()
     */
    @Override
    public void close() {}
  }

  private static class TestCacheListenerCustom extends TestCacheListener<Object, Object> {
    @Override
    public void afterCreate2(EntryEvent e) {}

    @Override
    public void afterUpdate2(EntryEvent e) {}

    @Override
    public void afterInvalidate2(EntryEvent e) {}
  }

  private class TestCacheListenerEventCount extends TestCacheListenerCustom {
    @Override
    public void afterInvalidate2(EntryEvent e) {
      eventCount.incrementAndGet();
    }
  }

  /**
   * Verify that special entries expire but other entries in the region don't
   */
  @Test
  public void testCustomEntryIdleTimeout1() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setCustomEntryIdleTimeout(new TestExpiry<>(key2, expire));
    factory.setStatisticsEnabled(true);
    TestCacheListenerCustom list = new TestCacheListenerCustom();
    factory.addCacheListener(list);

    Region<Object, Object> region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, factory);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value);

    // This value should NOT expire.
    Wait.pause(timeout * 2);
    assertThat(region.get(key1)).isEqualTo(value);

    // This value SHOULD expire
    ExpiryTask.suspendExpiration();
    Region.Entry entry;
    long tilt;
    try {
      region.create(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      assertTrue(list.waitForInvocation(5000));
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertEquals(region.get(key1), value);

    // Do it again with a put (I guess)
    ExpiryTask.suspendExpiration();
    try {
      region.put(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertEquals(region.get(key1), value);
  }

  /**
   * Verify that special entries don't expire but other entries in the region do
   */
  @Test
  public void testCustomEntryIdleTimeout2() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setEntryIdleTimeout(expire);
    ExpirationAttributes expire2 = new ExpirationAttributes(0, ExpirationAction.INVALIDATE);
    factory.setCustomEntryIdleTimeout(new TestExpiry<>(key2, expire2));
    factory.setStatisticsEnabled(true);
    TestCacheListenerCustom list = new TestCacheListenerCustom();
    factory.addCacheListener(list);

    Region<Object, Object> region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, factory);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    region.create(key2, value);

    // This value should NOT expire.
    Wait.pause(timeout * 2);
    assertThat(region.get(key2)).isEqualTo(value);

    // This value SHOULD expire
    ExpiryTask.suspendExpiration();
    Region.Entry entry;
    long tilt;
    try {
      region.create(key1, value);
      tilt = System.currentTimeMillis() + timeout;
      assertTrue(list.waitForInvocation(5000));
      entry = region.getEntry(key1);
      assertNotNull(entry.getValue());
    } finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertEquals(region.get(key2), value);

    // Do it again with a put (I guess)
    ExpiryTask.suspendExpiration();
    try {
      region.put(key1, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key1);
      assertNotNull(entry.getValue());
    } finally {
      ExpiryTask.permitExpiration();
    }
    waitForInvalidate(entry, tilt);

    // First value should still be in there
    assertEquals(region.get(key2), value);
  }

  /**
   * Configure custome entry expiration with an idle time. Create an entry and records its scheduled
   * expiration time. Then mutate the region expiration configuration and confirm that the entry's
   * expiration time is rescheduled.
   */
  @Test
  public void testCustomEntryIdleTimeout3() {
    final String name = this.getUniqueName();
    // test no longer waits for this expiration to happen
    final int timeout1 = 500 * 1000; // ms
    final int timeout2 = 2000 * 1000; // ms
    final String key1 = "KEY1";
    final String value1 = "VALUE1";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire1 = new ExpirationAttributes(timeout1, INVALIDATE);
    factory.setCustomEntryIdleTimeout(new TestExpiry<>(key1, expire1));
    factory.setStatisticsEnabled(true);
    TestCacheListenerEventCount list = new TestCacheListenerEventCount();
    eventCount.set(0);
    factory.addCacheListener(list);


    LocalRegion region;
    setProperty(EXPIRY_MS_PROPERTY, "true");
    try {
      region = (LocalRegion) createRegion(name, factory);
    } finally {
      getProperties().remove(EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value1);
    EntryExpiryTask eet = region.getEntryExpiryTask(key1);
    final long firstExpiryTime = eet.getExpirationTime();

    AttributesMutator<Object, Object> mutt = region.getAttributesMutator();
    ExpirationAttributes expire2 = new ExpirationAttributes(timeout2, INVALIDATE);
    mutt.setCustomEntryIdleTimeout(new TestExpiry<>(key1, expire2));
    eet = region.getEntryExpiryTask(key1);
    final long secondExpiryTime = eet.getExpirationTime();
    if ((secondExpiryTime - firstExpiryTime) <= 0) {
      fail(
          "expiration time should have been greater after changing region config from 500 to 2000. firstExpiryTime="
              + firstExpiryTime + " secondExpiryTime=" + secondExpiryTime);
    }

    // now set back to be more recent
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire3 = new ExpirationAttributes(timeout1, INVALIDATE);
    mutt.setCustomEntryIdleTimeout(new TestExpiry<>(key1, expire3));
    eet = region.getEntryExpiryTask(key1);
    final long thirdExpiryTime = eet.getExpirationTime();
    assertEquals(firstExpiryTime, thirdExpiryTime);
    // confirm that it still has not expired
    assertEquals(0, eventCount.get());

    // now set it to a really short time and make sure it expires immediately
    waitForExpiryClockToChange(region);
    final Region.Entry entry = region.getEntry(key1);
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire4 = new ExpirationAttributes(1, INVALIDATE);
    mutt.setCustomEntryIdleTimeout(new TestExpiry<>(key1, expire4));

    await().alias("entry never became invalid").until(() -> fetchEntryValue(entry) == null);

    await().alias("eventCount never became 1").until(() -> eventCount.get() == 1);

    eventCount.set(0);
  }

  /**
   * Configure entry expiration with a idle time. Create an entry and records its scheduled
   * expiration time. Then mutate the region expiration configuration and confirm that the entry's
   * expiration time is rescheduled.
   */
  @Test
  public void testEntryIdleTimeout3() {
    final String name = this.getUniqueName();
    // test no longer waits for this expiration to happen
    final int timeout1 = 500 * 1000; // ms
    final int timeout2 = 2000 * 1000; // ms
    final String key1 = "KEY1";
    final String value1 = "VALUE1";

    AttributesFactory<Object, Object> factory = new AttributesFactory(getRegionAttributes());
    ExpirationAttributes expire1 = new ExpirationAttributes(timeout1, INVALIDATE);
    factory.setEntryIdleTimeout(expire1);
    factory.setStatisticsEnabled(true);
    TestCacheListenerEventCount list = new TestCacheListenerEventCount();
    eventCount.set(0);
    factory.setCacheListener(list);

    LocalRegion region;
    setProperty(EXPIRY_MS_PROPERTY, "true");
    try {
      region = (LocalRegion) createRegion(name, factory.create());
    } finally {
      getProperties().remove(EXPIRY_MS_PROPERTY);
    }

    region.create(key1, value1);
    EntryExpiryTask eet = region.getEntryExpiryTask(key1);
    final long firstExpiryTime = eet.getExpirationTime();

    AttributesMutator mutt = region.getAttributesMutator();
    ExpirationAttributes expire2 = new ExpirationAttributes(timeout2, INVALIDATE);
    mutt.setEntryIdleTimeout(expire2);
    eet = region.getEntryExpiryTask(key1);
    final long secondExpiryTime = eet.getExpirationTime();
    if ((secondExpiryTime - firstExpiryTime) <= 0) {
      fail(
          "expiration time should have been greater after changing region config from 500 to 2000. firstExpiryTime="
              + firstExpiryTime + " secondExpiryTime=" + secondExpiryTime);
    }

    // now set back to be more recent
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire3 = new ExpirationAttributes(timeout1, INVALIDATE);
    mutt.setEntryIdleTimeout(expire3);
    eet = region.getEntryExpiryTask(key1);
    final long thirdExpiryTime = eet.getExpirationTime();
    assertEquals(firstExpiryTime, thirdExpiryTime);
    // confirm that it still has not expired
    assertEquals(0, eventCount.get());

    // now set it to a really short time and make sure it expires immediately
    waitForExpiryClockToChange(region);
    final Region.Entry entry = region.getEntry(key1);
    mutt = region.getAttributesMutator();
    ExpirationAttributes expire4 = new ExpirationAttributes(1, INVALIDATE);
    mutt.setEntryIdleTimeout(expire4);
    await().alias("entry never became invalid").until(() -> fetchEntryValue(entry) == null);
    await().alias("eventCount never became 1").until(() -> eventCount.get() == 1);
    eventCount.set(0);
  }

  static class CountExpiry<K, V> implements CustomExpiry<K, V>, Declarable {

    /**
     * Object --> CountExpiry
     *
     * @guarded.By CountExpiry.class
     */
    static final HashMap<Object, Object> invokeCounts = new HashMap<>();

    final String special;
    final ExpirationAttributes specialAtt;

    CountExpiry(String flagged, ExpirationAttributes att) {
      this.special = flagged;
      this.specialAtt = att;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.cache.CustomExpiry#getExpiry(org.apache.geode.cache.Region.Entry)
     */
    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      Object key = entry.getKey();
      synchronized (CountExpiry.class) {
        Integer count = (Integer) invokeCounts.get(key);
        if (count == null) {
          invokeCounts.put(key, 1);
        } else {
          invokeCounts.put(key, count + 1);
        }
      } // synchronized
      if (key.equals(special)) {
        return specialAtt;
      }
      return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.cache.Declarable#init(java.util.Properties)
     */
    @Override
    public void init(Properties props) {}

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.cache.CacheCallback#close()
     */
    @Override
    public void close() {}
  }

  /**
   * Verify that expiry is calculatod only once on an entry
   */
  @Test
  public void testCustomIdleOnce() {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms!
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setCustomEntryTimeToLive(new CountExpiry<>(key2, expire));
    factory.setStatisticsEnabled(true);
    synchronized (CountExpiry.class) {
      CountExpiry.invokeCounts.clear();
    }

    Region<Object, Object> region = null;
    /*
     * Crank up the expiration so test runs faster. This property only needs to be set while the
     * region is created
     */
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, factory);
    } finally {
      if (region.getAttributes().getPartitionAttributes() == null) {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    }

    // Random values should not expire
    region.put(key1, value);
    Wait.pause(timeout * 2);
    assert (region.get(key1).equals(value));

    // key2 *should* expire
    ExpiryTask.suspendExpiration();
    Region.Entry entry;
    long tilt;
    try {
      region.put(key2, value);
      tilt = System.currentTimeMillis() + timeout;
      entry = region.getEntry(key2);
      assertNotNull(entry.getValue());
    } finally {
      ExpiryTask.permitExpiration();
      if (region.getAttributes().getPartitionAttributes() != null) {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    }
    waitForInvalidate(entry, tilt);

    assert (region.get(key1).equals(value));

    synchronized (CountExpiry.class) {
      if (CountExpiry.invokeCounts.size() != 2) {
        fail("CountExpiry not invoked correctly, size = " + CountExpiry.invokeCounts.size());
      }
      Integer i = (Integer) CountExpiry.invokeCounts.get(key1);
      assertNotNull(i);
      assertEquals(1, i.intValue());

      i = (Integer) CountExpiry.invokeCounts.get(key2);
      assertNotNull(i);
      assertEquals(1, i.intValue());
    } // synchronized
  }

  /**
   * Verify that a get or put resets the idle time on an entry
   */
  @Test
  public void testCustomEntryIdleReset() {

    final String name = this.getUniqueName();
    final int timeout = 200 * 1000; // ms
    final String key1 = "KEY1";
    final String value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    factory.setCustomEntryIdleTimeout(new TestExpiry<>(key1, expire));
    factory.setStatisticsEnabled(true);
    TestCacheListenerCustom list = new TestCacheListenerCustom();
    factory.addCacheListener(list);


    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      LocalRegion region = (LocalRegion) createRegion(name, factory);

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
          fail("get did not reset the expiration time. createExpiryTime=" + createExpiryTime
              + " getExpiryTime=" + getExpiryTime);
        }
        Wait.waitForExpiryClockToChange(region);
        region.put(key1, value);
        assertSame(eet, region.getEntryExpiryTask(key1));
        final long putExpiryTime = eet.getExpirationTime();
        if (putExpiryTime - getExpiryTime <= 0L) {
          fail("put did not reset the expiration time. getExpiryTime=" + getExpiryTime
              + " putExpiryTime=" + putExpiryTime);
        }
      } finally {
        ExpiryTask.permitExpiration();
      }
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  class ExpiryCallbacks implements ExpiryTaskListener {
    @Override
    public void afterCancel(ExpiryTask et) {
      getCache().getLogger().info("ExpiryCallbacks.afterCancel",
          new RuntimeException("TaskCanceled"));
    }

    @Override
    public void afterSchedule(ExpiryTask et) {
      printState(et, "ExpiryCallbacks.afterSchedule ");
    }

    @Override
    public void afterTaskRan(ExpiryTask et) {
      printState(et, "ExpiryCallbacks.afterTaskRan ");
    }

    void printState(ExpiryTask et, String callback) {
      Date now = new Date();
      Date ttl = now;
      try {
        ttl = new Date(et.getExpirationTime());
      } catch (EntryNotFoundException ignored) {
        // ignore
      }
      Date idleExpTime = now;
      try {
        idleExpTime = new Date(et.getIdleExpirationTime());
      } catch (EntryNotFoundException ignored) {
        // ignore
      }
      Date ttlTime = new Date(et.getTTLExpirationTime());
      Date getNow = new Date(et.calculateNow());
      Date scheduleETime = new Date(et.scheduledExecutionTime());
      getCache().getLogger()
          .info(callback + " now: " + getCurrentTimeStamp(now) + " ttl:" + getCurrentTimeStamp(ttl)
              + " idleExpTime:" + getCurrentTimeStamp(idleExpTime) + " ttlTime:"
              + getCurrentTimeStamp(ttlTime) + " getNow:" + getCurrentTimeStamp(getNow)
              + " scheduleETime:" + getCurrentTimeStamp(scheduleETime) + " getKey:" + et.getKey()
              + " isPending:" + et.isPending() + " et :" + et + " Task reference "
              + System.identityHashCode(et));
    }

    String getCurrentTimeStamp(Date d) {
      return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(d);
    }

    @Override
    public void afterReschedule(ExpiryTask et) {
      printState(et, "ExpiryCallbacks.afterReschedule");
    }

    @Override
    public void afterExpire(ExpiryTask et) {
      printState(et, "ExpiryCallbacks.afterExpire");
    }

  }

  /**
   * Tests that an entry in a region that remains idle for a given amount of time is destroyed.
   */
  @Test
  public void testEntryIdleDestroy() {

    EntryExpiryTask.expiryTaskListener = new ExpiryCallbacks();
    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final String key = "KEY";
    final String value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setEntryIdleTimeout(expire);
    factory.setStatisticsEnabled(true);
    TestCacheListener<Object, Object> list = new TestCacheListener<Object, Object>() {
      @Override
      public void afterCreate2(EntryEvent e) {}

      @Override
      public void afterDestroy2(EntryEvent e) {}
    };
    factory.addCacheListener(list);


    Region<Object, Object> region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, factory);

      ExpiryTask.suspendExpiration();
      Region.Entry entry;
      long tilt;
      try {
        region.create(key, null);
        tilt = System.currentTimeMillis() + timeout;
        assertTrue(list.wasInvoked());
        entry = region.getEntry(key);
      } finally {
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
      } finally {
        ExpiryTask.permitExpiration();
      }
      waitForDestroy(entry, tilt);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      EntryExpiryTask.expiryTaskListener = null;
    }

  }

  /**
   * Verify that accessing an entry resets its idle time
   *
   */
  @Test
  public void testEntryIdleReset() {

    final String name = this.getUniqueName();
    // Test no longer waits for this timeout to expire
    final int timeout = 90; // seconds
    final String key = "KEY";
    final String value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setEntryIdleTimeout(expire);
    factory.setStatisticsEnabled(true);


    LocalRegion region = (LocalRegion) createRegion(name, factory);
    region.create(key, null);
    EntryExpiryTask eet = region.getEntryExpiryTask(key);
    long createExpiryTime = eet.getExpirationTime();

    Wait.waitForExpiryClockToChange(region);
    region.get(key); // touch
    assertSame(eet, region.getEntryExpiryTask(key));
    long getExpiryTime = eet.getExpirationTime();
    if (getExpiryTime - createExpiryTime <= 0L) {
      fail("get did not reset the expiration time. createExpiryTime=" + createExpiryTime
          + " getExpiryTime=" + getExpiryTime);
    }

    Wait.waitForExpiryClockToChange(region);
    region.put(key, value); // touch
    assertSame(eet, region.getEntryExpiryTask(key));
    long putExpiryTime = eet.getExpirationTime();
    if (putExpiryTime - getExpiryTime <= 0L) {
      fail("put did not reset the expiration time. getExpiryTime=" + getExpiryTime
          + " putExpiryTime=" + putExpiryTime);
    }

    // TODO other ops that should be validated?

    // Now verify operations that do not modify the expiry time

    Wait.waitForExpiryClockToChange(region);
    region.invalidate(key); // touch
    assertSame(eet, region.getEntryExpiryTask(key));
    long invalidateExpiryTime = eet.getExpirationTime();
    if (region.getConcurrencyChecksEnabled()) {
      if (putExpiryTime - getExpiryTime <= 0L) {
        fail("invalidate did not reset the expiration time. putExpiryTime=" + putExpiryTime
            + " invalidateExpiryTime=" + invalidateExpiryTime);
      }
    } else {
      if (invalidateExpiryTime != putExpiryTime) {
        fail("invalidate did reset the expiration time. putExpiryTime=" + putExpiryTime
            + " invalidateExpiryTime=" + invalidateExpiryTime);
      }
    }
  }

  @Test
  public void testEntryExpirationAfterMutate() throws CacheException {

    final String name = this.getUniqueName();
    final int timeout = 20; // ms
    final int hugeTimeout = Integer.MAX_VALUE;
    final ExpirationAttributes expire =
        new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    final ExpirationAttributes hugeExpire =
        new ExpirationAttributes(hugeTimeout, ExpirationAction.INVALIDATE);
    final String key = "KEY";
    final String value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    factory.setStatisticsEnabled(true);

    Region<Object, Object> region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = createRegion(name, factory);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }

    long tilt;
    region.create(key, value);
    tilt = System.currentTimeMillis() + timeout;

    // Now go from huge timeout to a timeout
    ExpiryTask.suspendExpiration();
    Region.Entry entry;
    try {
      region.getAttributesMutator().setEntryIdleTimeout(expire);
      entry = region.getEntry(key);
      assertEquals(value, entry.getValue());
    } finally {
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
   * Verify that accessing an entry does not delay expiration due to TTL
   */
  @Test
  public void testEntryIdleTtl() {

    final String name = this.getUniqueName();
    // test no longer waits for this timeout to expire
    final int timeout = 2000; // seconds
    final String key = "IDLE_TTL_KEY";
    final String value = "IDLE_TTL_VALUE";
    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expireIdle =
        new ExpirationAttributes(timeout / 2, ExpirationAction.DESTROY);
    factory.setEntryIdleTimeout(expireIdle);
    ExpirationAttributes expireTtl = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setEntryTimeToLive(expireTtl);
    factory.setStatisticsEnabled(true);


    LocalRegion region = (LocalRegion) createRegion(name, factory);

    region.create(key, value);
    EntryExpiryTask eet = region.getEntryExpiryTask(key);
    final long firstIdleExpiryTime = eet.getIdleExpirationTime();
    final long firstTTLExpiryTime = eet.getTTLExpirationTime();
    if ((firstIdleExpiryTime - firstTTLExpiryTime) >= 0) {
      fail("idle should be less than ttl: idle=" + firstIdleExpiryTime + " ttl="
          + firstTTLExpiryTime);
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
      fail("idle should have increased: idle=" + firstIdleExpiryTime + " idle2="
          + secondIdleExpiryTime);
    }
  }

  @Test
  public void testRegionExpirationAfterMutate() throws CacheException {

    if (getRegionAttributes().getPartitionAttributes() != null) {
      return;
    }

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    factory.setStatisticsEnabled(true);

    LocalRegion region;
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      region = (LocalRegion) createRegion(name, factory);

      region.create(key, value);

      // Now go from no timeout to a timeout
      Region.Entry entry = region.getEntry(key);
      assertEquals(value, entry.getValue());
      region.getAttributesMutator().setRegionIdleTimeout(
          new ExpirationAttributes(12000/* ms */, ExpirationAction.INVALIDATE));
      region.put(key, value);
      long tilt = System.currentTimeMillis();

      ExpiryTask expiryTask = region.getRegionIdleExpiryTask();
      long mediumExpiryTime = expiryTask.getExpirationTime();
      region.getAttributesMutator().setRegionIdleTimeout(
          new ExpirationAttributes(999000/* ms */, ExpirationAction.INVALIDATE));
      expiryTask = region.getRegionIdleExpiryTask();
      long hugeExpiryTime = expiryTask.getExpirationTime();
      ExpiryTask.suspendExpiration();
      long shortExpiryTime;
      try {
        region.getAttributesMutator().setRegionIdleTimeout(
            new ExpirationAttributes(20/* ms */, ExpirationAction.INVALIDATE));
        expiryTask = region.getRegionIdleExpiryTask();
        shortExpiryTime = expiryTask.getExpirationTime();
      } finally {
        ExpiryTask.permitExpiration();
      }
      waitForInvalidate(entry, tilt + 20, 10);
      assertTrue("expected hugeExpiryTime=" + hugeExpiryTime + " to be > than mediumExpiryTime="
          + mediumExpiryTime, (hugeExpiryTime - mediumExpiryTime) > 0);
      assertTrue("expected mediumExpiryTime=" + mediumExpiryTime + " to be > than shortExpiryTime="
          + shortExpiryTime, (mediumExpiryTime - shortExpiryTime) > 0);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  /**
   * Tests that a region that remains idle for a given amount of time is invalidated. Also tests
   * that accessing an entry of a region or a subregion counts as an access.
   */
  @Test
  public void testRegionIdleInvalidate() throws CacheException {

    if (getRegionAttributes().getPartitionAttributes() != null) {
      // PR does not support INVALID ExpirationAction
      return;
    }

    final String name = this.getUniqueName();
    final String subname = this.getUniqueName() + "-SUB";
    final int timeout = 22; // ms
    final Object key = "KEY";
    final Object value = "VALUE";


    VM vm0 = VM.getVM(0);
    vm0.invoke("testRegionIdleInvalidate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        TestCacheListener<Object, Object> list = new TestCacheListener<Object, Object>() {
          private int createCount = 0;

          @Override
          public void afterInvalidate2(EntryEvent e) {
            e.getRegion().getCache().getLogger().info("invalidate2 key=" + e.getKey());
          }

          @Override
          public void afterRegionInvalidate2(RegionEvent e) {}

          @Override
          public void afterUpdate2(EntryEvent e) {
            this.wasInvoked(); // Clear the flag
          }

          @Override
          public void afterCreate2(EntryEvent e) {
            this.createCount++;
            // we only expect one create; all the rest should be updates
            assertEquals(1, this.createCount);
            this.wasInvoked(); // Clear the flag
          }
        };
        RegionFactory<Object, Object> factory =
            getCache().createRegionFactory(getRegionAttributes());
        ExpirationAttributes expire =
            new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
        factory.setRegionIdleTimeout(expire);
        factory.setStatisticsEnabled(true);
        factory.addCacheListener(list);

        Region<Object, Object> region;
        Region<Object, Object> sub;
        Region.Entry entry;
        long tilt;
        System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
        ExpiryTask.suspendExpiration();
        try {
          region = createRegion(name, factory);
          region.put(key, value);
          tilt = System.currentTimeMillis() + timeout;
          entry = region.getEntry(key);
          assertEquals(value, entry.getValue());
          sub = factory.createSubregion(region, subname);
        } finally {
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
        region.getAttributesMutator().setRegionIdleTimeout(
            new ExpirationAttributes(EXPIRATION_MS, ExpirationAction.INVALIDATE));

        LocalRegion lr = (LocalRegion) region;
        {
          ExpiryTask expiryTask = lr.getRegionIdleExpiryTask();
          region.put(key, value);
          long createExpiry = expiryTask.getExpirationTime();
          long changeTime = Wait.waitForExpiryClockToChange(lr, createExpiry - EXPIRATION_MS);
          region.put(key, "VALUE2");
          long putExpiry = expiryTask.getExpirationTime();
          assertTrue(
              "CLOCK went back in time! Expected putBaseExpiry=" + (putExpiry - EXPIRATION_MS)
                  + " to be >= than changeTime=" + changeTime,
              (putExpiry - EXPIRATION_MS - changeTime) >= 0);
          assertTrue(
              "expected putExpiry=" + putExpiry + " to be > than createExpiry=" + createExpiry,
              (putExpiry - createExpiry) > 0);
          changeTime = Wait.waitForExpiryClockToChange(lr, putExpiry - EXPIRATION_MS);
          region.get(key);
          long getExpiry = expiryTask.getExpirationTime();
          assertTrue(
              "CLOCK went back in time! Expected getBaseExpiry=" + (getExpiry - EXPIRATION_MS)
                  + " to be >= than changeTime=" + changeTime,
              (getExpiry - EXPIRATION_MS - changeTime) >= 0);
          assertTrue("expected getExpiry=" + getExpiry + " to be > than putExpiry=" + putExpiry,
              (getExpiry - putExpiry) > 0);

          changeTime = Wait.waitForExpiryClockToChange(lr, getExpiry - EXPIRATION_MS);
          sub.put(key, value);
          long subPutExpiry = expiryTask.getExpirationTime();
          assertTrue(
              "CLOCK went back in time! Expected subPutBaseExpiry=" + (subPutExpiry - EXPIRATION_MS)
                  + " to be >= than changeTime=" + changeTime,
              (subPutExpiry - EXPIRATION_MS - changeTime) >= 0);
          assertTrue(
              "expected subPutExpiry=" + subPutExpiry + " to be > than getExpiry=" + getExpiry,
              (subPutExpiry - getExpiry) > 0);
          changeTime = Wait.waitForExpiryClockToChange(lr, subPutExpiry - EXPIRATION_MS);
          sub.get(key);
          long subGetExpiry = expiryTask.getExpirationTime();
          assertTrue(
              "CLOCK went back in time! Expected subGetBaseExpiry=" + (subGetExpiry - EXPIRATION_MS)
                  + " to be >= than changeTime=" + changeTime,
              (subGetExpiry - EXPIRATION_MS - changeTime) >= 0);
          assertTrue("expected subGetExpiry=" + subGetExpiry + " to be > than subPutExpiry="
              + subPutExpiry, (subGetExpiry - subPutExpiry) > 0);
        }
      }
    });
  }


  /**
   * Tests that a region expires with a destruction after a given idle time.
   */
  @Test
  public void testRegionIdleDestroy() throws CacheException {

    if (getRegionAttributes().getPartitionAttributes() != null) {
      return;
    }

    final String name = this.getUniqueName();
    final int timeout = 22; // ms
    final Object key = "KEY";
    final Object value = "VALUE";

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    factory.setRegionIdleTimeout(expire);
    factory.setStatisticsEnabled(true);


    Region<Object, Object> region;
    long tilt;
    ExpiryTask.suspendExpiration();
    try {
      System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
      try {
        region = createRegion(name, factory);
        region.put(key, value);
        tilt = System.currentTimeMillis() + timeout;
        assertFalse(region.isDestroyed());
      } finally {
        ExpiryTask.permitExpiration();
      }
      waitForRegionDestroy(region, tilt);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  /**
   * Tests basic creation and loading of a snapshot from perspective of single VM
   */
  public static Region<Object, Object> preSnapshotRegion = null;
  private static final int MAX_KEYS = 10;

  @Test
  public void testSnapshot() throws IOException, CacheException, ClassNotFoundException {
    final String name = this.getUniqueName();

    // create region in controller
    preSnapshotRegion = createRegion(name);

    // create region in other VMs if distributed
    boolean isDistributed = getRegionAttributes().getScope().isDistributed();
    if (isDistributed) {
      invokeInEveryVM("create presnapshot region", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          preSnapshotRegion = createRegion(name);
        }
      });
    }

    // add data to region in controller
    for (int i = 0; i < MAX_KEYS; i++) {
      if (i == MAX_KEYS - 1) {
        // bug 33311 coverage
        preSnapshotRegion.create(String.valueOf(i), null);
      } else {
        preSnapshotRegion.create(String.valueOf(i), i);
      }
    }

    // save snapshot
    File file = new File(name + ".snap");
    OutputStream out = new FileOutputStream(file);

    try {
      preSnapshotRegion.saveSnapshot(out);

      assertEquals(5, preSnapshotRegion.get("5"));

      // destroy all data
      for (int i = 0; i < MAX_KEYS; i++) {
        preSnapshotRegion.destroy(String.valueOf(i));
      }

      assertEquals(0, preSnapshotRegion.keySet().size());

      InputStream in = new FileInputStream(file);
      preSnapshotRegion.loadSnapshot(in);

      // test postSnapshot behavior in controller
      remoteTestPostSnapshot(name, true, false);

      // test postSnapshot behavior in other VMs if distributed
      if (isDistributed) {
        invokeInEveryVM("postSnapshot", new CacheSerializableRunnable() {
          @Override
          public void run2() throws CacheException {
            RegionTestCase.this.remoteTestPostSnapshot(name, false, false);
          }
        });
      }
    } finally {
      file.delete();
    }
  }

  @Test
  public void testRootSnapshot() throws IOException, CacheException, ClassNotFoundException {
    final String name = this.getUniqueName();

    // create region in controller
    preSnapshotRegion = createRootRegion(name, getRegionAttributes());

    // create region in other VMs if distributed
    boolean isDistributed = getRegionAttributes().getScope().isDistributed();
    if (isDistributed) {
      invokeInEveryVM("create presnapshot region", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          preSnapshotRegion = createRootRegion(name, getRegionAttributes());
        }
      });
    }


    // add data to region in controller
    for (int i = 0; i < MAX_KEYS; i++) {
      if (i == MAX_KEYS - 1) {
        // bug 33311 coverage
        preSnapshotRegion.create(String.valueOf(i), null);
      } else {
        preSnapshotRegion.create(String.valueOf(i), i);
      }
    }

    // save snapshot
    File file = new File(name + ".snap");
    OutputStream out = new FileOutputStream(file);

    try {
      preSnapshotRegion.saveSnapshot(out);

      assertThat(preSnapshotRegion.get("5")).isEqualTo(5);

      // destroy all data
      for (int i = 0; i < MAX_KEYS; i++) {
        preSnapshotRegion.destroy(String.valueOf(i));
      }

      assertThat(preSnapshotRegion.keySet().size()).isEqualTo(0);


      InputStream in = new FileInputStream(file);
      preSnapshotRegion.loadSnapshot(in);

      // test postSnapshot behavior in controller
      remoteTestPostSnapshot(name, true, true);

      // test postSnapshot behavior in other VMs if distributed
      if (isDistributed) {
        invokeInEveryVM("postSnapshot", new CacheSerializableRunnable() {
          @Override
          public void run2() throws CacheException {
            RegionTestCase.this.remoteTestPostSnapshot(name, false, true);
          }
        });
      }
    } finally {
      file.delete();
    }
  }

  private void remoteTestPostSnapshot(String name, boolean isController, boolean isRoot)
      throws CacheException {
    assertTrue(preSnapshotRegion.isDestroyed());

    try {
      preSnapshotRegion.get("0");
      fail("Should have thrown a RegionReinitializedException");
    } catch (RegionReinitializedException e) {
      // pass
    }


    // get new reference to region
    Region postSnapshotRegion = isRoot ? getRootRegion(name) : getRootRegion().getSubregion(name);
    assertNotNull("Could not get reference to reinitialized region", postSnapshotRegion);

    boolean expectData =
        isController || postSnapshotRegion.getAttributes().getMirrorType().isMirrored()
            || postSnapshotRegion.getAttributes().getDataPolicy().isPreloaded();

    assertEquals(expectData ? MAX_KEYS : 0, postSnapshotRegion.keySet().size());
    // gets the data either locally or by netSearch
    assertEquals(3, postSnapshotRegion.get("3"));
    // bug 33311 coverage
    if (expectData) {
      assertFalse(postSnapshotRegion.containsValueForKey("9"));
      assertTrue(postSnapshotRegion.containsKey("9"));
    }
  }

}
