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
package org.apache.geode.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;

import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.compression.SnappyCompressor;

/**
 * Tests the functionality of the {@link AttributesFactory} class.
 *
 * @since GemFire 3.0
 */
public class AttributesFactoryJUnitTest {

  @Test
  public void testCopyConstructor() {
    AttributesFactory f1 = new AttributesFactory();
    f1.setLockGrantor(true);

    RegionAttributes origAttrs = f1.create();
    assertEquals(true, origAttrs.isLockGrantor());

    AttributesFactory f2 = new AttributesFactory(origAttrs);
    RegionAttributes attrs = f2.create();

    assertEquals(true, attrs.isLockGrantor());
  }

  /**
   * Tests the {@link AttributesFactory#create} throws the appropriate exception with
   * poorly-configured factory.
   */
  @Test
  public void testInvalidConfigurations() {
    AttributesFactory factory;

    ExpirationAttributes invalidate =
        new ExpirationAttributes(1, ExpirationAction.LOCAL_INVALIDATE);
    ExpirationAttributes destroy = new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY);

    // DataPolicy.REPLICATE is incompatible with
    // ExpirationAction.LOCAL_INVALIDATE
    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setEntryIdleTimeout(invalidate);
    factory.setStatisticsEnabled(true);
    {
      RegionAttributes ra = factory.create();
      assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
      assertEquals(new SubscriptionAttributes(InterestPolicy.ALL), ra.getSubscriptionAttributes());
    }

    // DataPolicy.REPLICATE is incompatible with
    // ExpirationAction.LOCAL_DESTROY.
    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setEntryIdleTimeout(destroy);
    factory.setStatisticsEnabled(true);
    {
      RegionAttributes ra = factory.create();
      assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
      assertEquals(new SubscriptionAttributes(InterestPolicy.ALL), ra.getSubscriptionAttributes());
    }

    // MirrorType KEYS is incompatible with
    // ExpirationAction.LOCAL_INVALIDATE
    factory = new AttributesFactory();
    factory.setMirrorType(MirrorType.KEYS);
    factory.setEntryIdleTimeout(destroy);
    factory.setStatisticsEnabled(true);
    {
      RegionAttributes ra = factory.create();
      assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
      assertEquals(new SubscriptionAttributes(InterestPolicy.ALL), ra.getSubscriptionAttributes());
    }

    // MirrorType.KEYS are incompatible with
    // ExpirationAction.LOCAL_DESTROY.
    factory = new AttributesFactory();
    factory.setMirrorType(MirrorType.KEYS);
    factory.setEntryIdleTimeout(destroy);
    factory.setStatisticsEnabled(true);
    {
      RegionAttributes ra = factory.create();
      assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
      assertEquals(new SubscriptionAttributes(InterestPolicy.ALL), ra.getSubscriptionAttributes());
    }

    // Entry idle expiration requires that
    // statistics are enabled
    factory = new AttributesFactory();
    factory.setEntryIdleTimeout(destroy);
    factory.setStatisticsEnabled(false);
    try {
      factory.create();
      fail("Should have thrown an IllegalStateException");

    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalStateException);
      // pass...
    }

    // Entry ttl expiration requires that
    // statistics are enabled
    factory = new AttributesFactory();
    factory.setEntryTimeToLive(destroy);
    factory.setStatisticsEnabled(false);
    try {
      factory.create();
      fail("Should have thrown an IllegalStateException");

    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalStateException);
      // pass...
    }

    // MembershipAttributes (required roles)
    // requires distributed scope
    factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    MembershipAttributes ra = new MembershipAttributes(new String[] {"A"});
    factory.setMembershipAttributes(ra);
    try {
      factory.create();
      fail("Should have thrown an IllegalStateException");

    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalStateException);
      // pass...
    }

    // Used mixed mode API for disk store and DWA
    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskStoreName("ds1");
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    try {
      factory.setDiskWriteAttributes(dwaf.create());
      fail("Should have thrown an IllegalStateException");
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalStateException);
      // pass...
    }

    // Used mixed mode API for disk store and DWA
    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    DiskWriteAttributesFactory dwaf2 = new DiskWriteAttributesFactory();
    factory.setDiskWriteAttributes(dwaf2.create());
    try {
      factory.setDiskStoreName("ds1");
      fail("Should have thrown an IllegalStateException");
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalStateException);
      // pass...
    }

    // Used mixed mode API for disk store and DWA
    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    File[] dirs1 = new File[] {new File("").getAbsoluteFile()};
    factory.setDiskStoreName("ds1");

    try {
      factory.setDiskDirs(dirs1);
      fail("Should have thrown an IllegalStateException");
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalStateException);
      // pass...
    }

    // Used mixed mode API for disk store and DWA
    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    File[] dirs2 = new File[] {new File("").getAbsoluteFile()};
    factory.setDiskDirs(dirs2);
    try {
      factory.setDiskStoreName("ds1");
      fail("Should have thrown an IllegalStateException");
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalStateException);
      // pass...
    }

    // Cloning cannot be disabled when a compressor is set
    factory = new AttributesFactory();
    factory.setCompressor(SnappyCompressor.getDefaultInstance());
    factory.setCloningEnabled(false);
    try {
      RegionAttributes ccra = factory.create();
      fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException expected) {
      // Expected
    }
  }

  /**
   * Tests that the {@link AttributesFactory#AttributesFactory() default} attributes factory has the
   * advertised default configuration.
   */
  @Test
  public void testDefaultConfiguration() {
    AttributesFactory factory = new AttributesFactory();
    RegionAttributes attrs = factory.create();
    assertNull(attrs.getCacheLoader());
    assertNull(attrs.getCacheWriter());
    assertNull(attrs.getCacheListener());
    assertEquals(Arrays.asList(new CacheListener[0]), Arrays.asList(attrs.getCacheListeners()));
    assertEquals(0, attrs.getRegionTimeToLive().getTimeout());
    assertEquals(0, attrs.getRegionIdleTimeout().getTimeout());
    assertEquals(0, attrs.getEntryTimeToLive().getTimeout());
    assertEquals(null, attrs.getCustomEntryTimeToLive());
    assertEquals(0, attrs.getEntryIdleTimeout().getTimeout());
    assertEquals(null, attrs.getCustomEntryIdleTimeout());
    assertEquals(Scope.DISTRIBUTED_NO_ACK, attrs.getScope());
    assertEquals(DataPolicy.DEFAULT, attrs.getDataPolicy());
    assertEquals(InterestPolicy.DEFAULT, attrs.getSubscriptionAttributes().getInterestPolicy());
    assertEquals(MirrorType.NONE, attrs.getMirrorType());
    assertEquals(null, attrs.getDiskStoreName());
    assertEquals(AttributesFactory.DEFAULT_DISK_SYNCHRONOUS, attrs.isDiskSynchronous());
    assertNull(attrs.getKeyConstraint());
    assertEquals(16, attrs.getInitialCapacity());
    assertEquals(0.75, attrs.getLoadFactor(), 0.0);
    assertFalse(attrs.getStatisticsEnabled());
    assertFalse(attrs.getPersistBackup());
    DiskWriteAttributes dwa = attrs.getDiskWriteAttributes();
    assertNotNull(dwa);
    {
      DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
      dwaf.setSynchronous(AttributesFactory.DEFAULT_DISK_SYNCHRONOUS);
      assertEquals(dwaf.create(), dwa);
    }

    assertNull(attrs.getDiskStoreName());
    File[] diskDirs = attrs.getDiskDirs();
    assertNotNull(diskDirs);
    assertEquals(1, diskDirs.length);
    assertEquals(new File("."), diskDirs[0]);
    int[] diskSizes = attrs.getDiskDirSizes();
    assertNotNull(diskSizes);
    assertEquals(1, diskSizes.length);
    assertEquals(DiskStoreFactory.DEFAULT_DISK_DIR_SIZE, diskSizes[0]);
    assertTrue(attrs.getConcurrencyChecksEnabled());
  }

  @Test
  public void testDiskSynchronous() {
    {
      AttributesFactory factory = new AttributesFactory();
      factory.setDiskSynchronous(true);
      RegionAttributes attrs = factory.create();
      assertEquals(true, attrs.isDiskSynchronous());
      assertEquals(true, attrs.getDiskWriteAttributes().isSynchronous());
    }
    {
      AttributesFactory factory = new AttributesFactory();
      factory.setDiskSynchronous(false);
      RegionAttributes attrs = factory.create();
      assertEquals(false, attrs.isDiskSynchronous());
      assertEquals(false, attrs.getDiskWriteAttributes().isSynchronous());
    }
    // Test backwards compat interaction with diskSync.
    // If the old apis are used then we should get the old default of async.
    {
      DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
      AttributesFactory factory = new AttributesFactory();
      factory.setDiskWriteAttributes(dwaf.create());
      RegionAttributes attrs = factory.create();
      assertEquals(false, attrs.getDiskWriteAttributes().isSynchronous());
      assertEquals(false, attrs.isDiskSynchronous());
    }
    {
      AttributesFactory factory = new AttributesFactory();
      factory.setDiskDirs(new File[] {new File("").getAbsoluteFile()});
      RegionAttributes attrs = factory.create();
      assertEquals(false, attrs.getDiskWriteAttributes().isSynchronous());
      assertEquals(false, attrs.isDiskSynchronous());
    }
    {
      AttributesFactory factory = new AttributesFactory();
      factory.setDiskDirsAndSizes(new File[] {new File("").getAbsoluteFile()}, new int[] {100});
      RegionAttributes attrs = factory.create();
      assertEquals(false, attrs.getDiskWriteAttributes().isSynchronous());
      assertEquals(false, attrs.isDiskSynchronous());
    }
  }

  /**
   * Tests the cacheListener functionality
   *
   * @since GemFire 5.0
   */
  @Test
  public void testCacheListeners() {
    RegionAttributes ra;
    CacheListener cl1 = new MyCacheListener();
    CacheListener cl2 = new MyCacheListener();
    assertFalse(cl1.equals(cl2));
    assertFalse((Arrays.asList(new CacheListener[] {cl1, cl2}))
        .equals(Arrays.asList(new CacheListener[] {cl2, cl1})));

    AttributesFactory factory = new AttributesFactory();
    try {
      factory.addCacheListener(null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      factory.initCacheListeners(new CacheListener[] {null});
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }

    ra = factory.create();
    assertEquals(null, ra.getCacheListener());
    assertEquals(Arrays.asList(new CacheListener[0]), Arrays.asList(ra.getCacheListeners()));

    factory.setCacheListener(cl1);
    ra = factory.create();
    assertEquals(cl1, ra.getCacheListener());
    assertEquals(Arrays.asList(new CacheListener[] {cl1}), Arrays.asList(ra.getCacheListeners()));

    factory.setCacheListener(cl2);
    ra = factory.create();
    assertEquals(cl2, ra.getCacheListener());
    assertEquals(Arrays.asList(new CacheListener[] {cl2}), Arrays.asList(ra.getCacheListeners()));

    factory.setCacheListener(null);
    ra = factory.create();
    assertEquals(null, ra.getCacheListener());
    assertEquals(Arrays.asList(new CacheListener[0]), Arrays.asList(ra.getCacheListeners()));

    factory.setCacheListener(cl1);
    factory.initCacheListeners(new CacheListener[] {cl1, cl2});
    ra = factory.create();
    try {
      ra.getCacheListener();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    assertEquals(Arrays.asList(new CacheListener[] {cl1, cl2}),
        Arrays.asList(ra.getCacheListeners()));

    factory.initCacheListeners(null);
    ra = factory.create();
    assertEquals(Arrays.asList(new CacheListener[0]), Arrays.asList(ra.getCacheListeners()));

    factory.initCacheListeners(new CacheListener[0]);
    ra = factory.create();
    assertEquals(Arrays.asList(new CacheListener[0]), Arrays.asList(ra.getCacheListeners()));

    factory.addCacheListener(cl1);
    ra = factory.create();
    assertEquals(Arrays.asList(new CacheListener[] {cl1}), Arrays.asList(ra.getCacheListeners()));
    factory.addCacheListener(cl2);
    ra = factory.create();
    assertEquals(Arrays.asList(new CacheListener[] {cl1, cl2}),
        Arrays.asList(ra.getCacheListeners()));
    factory.initCacheListeners(new CacheListener[] {cl2});
    ra = factory.create();
    assertEquals(Arrays.asList(new CacheListener[] {cl2}), Arrays.asList(ra.getCacheListeners()));
  }

  /**
   * @since GemFire 5.7
   */
  @Test
  public void testConnectionPool() {
    CacheLoader cl = new CacheLoader() {
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        return null;
      }

      public void close() {}
    };

    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName("mypool");

    factory = new AttributesFactory();
    factory.setCacheWriter(new CacheWriterAdapter());
    factory.setPoolName("mypool");

    factory = new AttributesFactory();
    factory.setCacheLoader(cl);
    factory.setPoolName("mypool");
  }

  /**
   * Trivial cache listener impl
   *
   * @since GemFire 5.0
   */
  private static class MyCacheListener extends CacheListenerAdapter {
    // empty impl
  }
}
