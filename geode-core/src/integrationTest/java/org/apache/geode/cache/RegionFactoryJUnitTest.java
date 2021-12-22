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

import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.apache.geode.cache.RegionShortcut.LOCAL_HEAP_LRU;
import static org.apache.geode.cache.RegionShortcut.LOCAL_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.LOCAL_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.LOCAL_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_HEAP_LRU;
import static org.apache.geode.cache.RegionShortcut.PARTITION_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PROXY;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PROXY_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_HEAP_LRU;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_HEAP_LRU;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_TTL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.CancelException;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.LocalRegion;

/**
 * Unit test for the RegionFactory class
 *
 * @since GemFire 5.0
 */
public class RegionFactoryJUnitTest {

  @Rule
  public TestName testName = new TestName();

  private final String key = "key";
  private final Integer val = new Integer(1);
  private final String r1Name = "r1";
  private final String r2Name = "r2";
  private final String r3Name = "r3";
  private final String r1sr1Name = "sr1";
  private Cache cache = null;

  private DistributedSystem distSys = null;

  private Region r1;
  private Region r2;
  private Region r3;
  private Region r1sr1;

  @After
  public void tearDown() throws Exception {
    try {
      if (r1 != null) {
        cleanUpRegion(r1);
      }
      if (r2 != null) {
        cleanUpRegion(r2);
      }
      if (r3 != null) {
        cleanUpRegion(r3);
      }
      if (r1sr1 != null) {
        cleanUpRegion(r1sr1);
      }
      Cache c = cache;
      DistributedSystem d = distSys;
      if (c != null && !c.isClosed()) {
        d = c.getDistributedSystem();
        c.close();
      }
      if (d != null && d.isConnected()) {
        d.disconnect();
      }
    } finally {
      r1 = null;
      r2 = null;
      r3 = null;
      r1sr1 = null;
      cache = null;
      distSys = null;
    }
  }

  /**
   * Tests create, destroy and recreate Region
   */
  @Test
  public void testCreateDestroyCreateRegions() throws Exception {
    // Assert basic region creation when no DistributedSystem or Cache exists
    RegionFactory factory = new RegionFactory(createGemFireProperties());
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);

    // Assert duplicate creation failure
    try {
      factory.create(r1Name);
      fail("Expected RegionExistsException");
    } catch (RegionExistsException expected) {
    }

    r1sr1 = factory.createSubregion(r1, r1sr1Name);
    assertBasicRegionFunctionality(r1sr1, r1sr1Name);
    try {
      factory.createSubregion(r1, r1sr1Name);
      fail("Expected RegionExistsException");
    } catch (RegionExistsException expected) {
    }
    r1sr1.destroyRegion();

    r2 = factory.create(r2Name);
    assertBasicRegionFunctionality(r2, r2Name);
    r2.destroyRegion();

    try {
      factory.createSubregion(r2, "shouldNotBePossible");
      fail("Expected a RegionDestroyedException");
    } catch (RegionDestroyedException expected) {
    }

    r1.destroyRegion();
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);

    r1.destroyRegion();
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);

    r1.destroyRegion();
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);
  }

  /**
   * Test RegionFactory with Cache close and DistributedSystem disconnect
   */
  @Test
  public void testRegionFactoryAndCacheClose() throws Exception {
    // Assert basic region creation when no DistributedSystem or Cache exists
    RegionFactory factory = new RegionFactory(createGemFireProperties());
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);

    // Assert duplicate creation failure
    try {
      factory.create(r1Name);
      fail("Expected RegionExistsException");
    } catch (RegionExistsException expected) {
    }

    r2 = factory.create(r2Name);
    assertBasicRegionFunctionality(r2, r2Name);
    r2.destroyRegion();

    // as of 6.5 if the cache that was used to create a regionFactory is closed
    // then the factory is out of business
    Cache c = r1.getCache();
    r1.destroyRegion();
    c.close();
    try {
      r1 = factory.create(r1Name);
      fail("Use of RegionFactory after cache close should throw CancelException");
    } catch (CancelException expected) {
      // passed
    }

    factory = new RegionFactory(createGemFireProperties());
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);

    // as of 6.5 if the ds that was used to create a regionFactory is disconnected
    // then the factory is out of business
    // Assert we can handle a disconnected disributed system
    DistributedSystem d = r1.getCache().getDistributedSystem();
    r1.destroyRegion();
    d.disconnect();
    try {
      r1 = factory.create(r1Name);
    } catch (CancelException expected) {
      // passed
    }

    factory = new RegionFactory(createGemFireProperties());
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);

    // as of 6.5 if the ds that was used to create a regionFactory is disconnected
    // then the factory is out of business
    // Assert we can handle both a closed Cache and a disconnected system
    c = r1.getCache();
    d = c.getDistributedSystem();
    r1.destroyRegion();
    c.close();
    d.disconnect();
    r1 = new RegionFactory(createGemFireProperties()).create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);
  }

  @Test
  public void testAfterConnect() throws Exception {
    // Assert basic region creation when a Distributed system exists
    distSys = DistributedSystem.connect(createGemFireProperties()); // for teardown

    RegionFactory factory = new RegionFactory();
    r1 = factory.create(r1Name);
    cache = r1.getCache(); // for teardown
    assertBasicRegionFunctionality(r1, r1Name);
  }

  @Test
  public void testAfterConnectWithDifferentProperties() throws Exception {
    // Assert failure when a Distributed system exists but with different properties
    distSys = DistributedSystem.connect(createGemFireProperties()); // for teardown
    final Properties failed = new Properties();
    failed.put(MCAST_TTL, "64");

    try {
      new RegionFactory(failed);
      fail("Expected exception");
    } catch (IllegalStateException expected) {
      // passed
    }
  }

  @Test
  public void testAfterCacheCreate() throws Exception {
    // Assert basic region creation when a Distributed and Cache exist
    DistributedSystem ds = DistributedSystem.connect(createGemFireProperties());
    CacheFactory.create(ds);

    RegionFactory factory = new RegionFactory();
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);
  }

  @Test
  public void testAfterCacheClosed() throws Exception {
    // Assert basic region creation when a Distributed and Cache exist but the cache is closed
    Properties gemfireProps = createGemFireProperties();
    DistributedSystem ds = DistributedSystem.connect(gemfireProps);
    cache = CacheFactory.create(ds);
    cache.close();

    RegionFactory factory = new RegionFactory(gemfireProps);
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);
  }

  /**
   * Test method for 'org.apache.geode.cache.RegionFactory.RegionFactory(RegionAttributes)'
   */
  @Test
  public void testRegionFactoryRegionAttributes() throws Exception {
    Properties gemfireProps = createGemFireProperties();
    r1 = new RegionFactory(gemfireProps).setScope(Scope.LOCAL).setConcurrencyLevel(1)
        .setLoadFactor(0.8F).setKeyConstraint(String.class).setStatisticsEnabled(true)
        .create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);

    final RegionFactory factory = new RegionFactory(gemfireProps, r1.getAttributes());
    r2 = factory.create(r2Name);
    assertBasicRegionFunctionality(r2, r2Name);
    assertRegionAttributes(r1.getAttributes(), r2.getAttributes());

    r3 = factory.create(r3Name);
    assertRegionAttributes(r2.getAttributes(), r3.getAttributes());
  }

  /**
   * Test method for 'org.apache.geode.cache.RegionFactory.RegionFactory(String)'
   */
  @Test
  public void testRegionFactoryString() throws Exception {
    // TODO: create subdir using getName() and create cache.xml there instead of using
    // DEFAULT_CACHE_XML_FILE
    DistributionConfig.DEFAULT_CACHE_XML_FILE.delete();
    try {
      DistributionConfig.DEFAULT_CACHE_XML_FILE.createNewFile();
      FileWriter f = new FileWriter(DistributionConfig.DEFAULT_CACHE_XML_FILE);
      f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n"
          + "<!DOCTYPE cache PUBLIC\n  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.0//EN\"\n"
          + "  \"http://www.gemstone.com/dtd/cache7_0.dtd\">\n" + "<cache>\n"
          + " <region-attributes id=\"" + getName()
          + "\" statistics-enabled=\"true\" scope=\"distributed-ack\">\n" + "  <key-constraint>"
          + String.class.getName() + "</key-constraint>\n" + "  <value-constraint>"
          + Integer.class.getName() + "</value-constraint>\n"
          + "    <entry-idle-time><expiration-attributes timeout=\"60\"/></entry-idle-time>\n"
          + " </region-attributes>\n" + "</cache>");
      f.close();

      RegionFactory factory = new RegionFactory(createGemFireProperties(), getName());
      r1 = factory.create(r1Name);
      assertBasicRegionFunctionality(r1, r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ra.getStatisticsEnabled(), true);
      assertEquals(ra.getScope().isDistributedAck(), true);
      assertEquals(ra.getValueConstraint(), Integer.class);
      assertEquals(ra.getKeyConstraint(), String.class);
      assertEquals(ra.getEntryIdleTimeout().getTimeout(), 60);
    } finally {
      DistributionConfig.DEFAULT_CACHE_XML_FILE.delete();
    }
  }

  /**
   * Test method for 'org.apache.geode.cache.RegionFactory.RegionFactory(Properties)'
   */
  @Test
  public void testRegionFactoryProperties() throws Exception {
    final Properties gemfireProperties = createGemFireProperties();
    gemfireProperties.put(MCAST_TTL, "64");
    RegionFactory factory = new RegionFactory(gemfireProperties);
    r1 = factory.create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);
    assertEquals(gemfireProperties.get(MCAST_TTL),
        r1.getCache().getDistributedSystem().getProperties().get(MCAST_TTL));
  }

  /**
   * Test method for 'org.apache.geode.cache.RegionFactory.RegionFactory(Properties,
   * RegionAttributes)'
   */
  @Ignore
  @Test
  public void testRegionFactoryPropertiesRegionAttributes() {
    // TODO: implement test
  }

  /**
   * Test method for 'org.apache.geode.cache.RegionFactory.RegionFactory(Properties, String)'
   */
  @Test
  public void testRegionFactoryPropertiesString() throws Exception {
    File xmlFile = null;
    try {
      final Properties gemfireProperties = createGemFireProperties();
      gemfireProperties.put(MCAST_TTL, "64");
      final String xmlFileName = getName() + "-cache.xml";
      gemfireProperties.put(CACHE_XML_FILE, xmlFileName);
      xmlFile = new File(xmlFileName);
      xmlFile.delete();
      xmlFile.createNewFile();
      FileWriter f = new FileWriter(xmlFile);
      final String attrsId = getName() + "-attrsId";
      f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n"
          + "<!DOCTYPE cache PUBLIC\n  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.0//EN\"\n"
          + "  \"http://www.gemstone.com/dtd/cache7_0.dtd\">\n" + "<cache>\n"
          + " <region-attributes id=\"" + attrsId
          + "\" statistics-enabled=\"true\" scope=\"distributed-ack\">\n" + "  <key-constraint>"
          + String.class.getName() + "</key-constraint>\n" + "  <value-constraint>"
          + Integer.class.getName() + "</value-constraint>\n"
          + "    <entry-idle-time><expiration-attributes timeout=\"60\"/></entry-idle-time>\n"
          + " </region-attributes>\n" + "</cache>");
      f.close();

      RegionFactory factory = new RegionFactory(gemfireProperties, attrsId);
      r1 = factory.create(r1Name);
      assertBasicRegionFunctionality(r1, r1Name);
      assertEquals(gemfireProperties.get(MCAST_TTL),
          r1.getCache().getDistributedSystem().getProperties().get(MCAST_TTL));
      assertEquals(gemfireProperties.get(CACHE_XML_FILE),
          r1.getCache().getDistributedSystem().getProperties().get(CACHE_XML_FILE));
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ra.getStatisticsEnabled(), true);
      assertEquals(ra.getScope().isDistributedAck(), true);
      assertEquals(ra.getValueConstraint(), Integer.class);
      assertEquals(ra.getKeyConstraint(), String.class);
      assertEquals(ra.getEntryIdleTimeout().getTimeout(), 60);
    } finally {
      if (xmlFile != null) {
        xmlFile.delete();
      }
    }
  }


  /**
   * Ensure that the RegionFactory set methods mirrors those found in RegionAttributes
   *
   */
  @Test
  public void testAttributesFactoryConformance() throws Exception {
    Method[] af = AttributesFactory.class.getDeclaredMethods();
    Method[] rf = RegionFactory.class.getDeclaredMethods();
    Method am, rm;

    ArrayList afDeprected = new ArrayList(); // hack to ignore deprecated methods
    afDeprected.add("setCacheListener");
    afDeprected.add("setMirrorType");
    afDeprected.add("setPersistBackup");
    afDeprected.add("setBucketRegion");
    afDeprected.add("setEnableWAN");
    afDeprected.add("setEnableBridgeConflation");
    afDeprected.add("setEnableConflation");
    ArrayList methodsToImplement = new ArrayList();

    // Since the RegionFactory has an AttributesFactory member,
    // we only need to make sure the RegionFactory class adds proxies for the
    // 'set' and 'add' methods added to the AttributesFactory. The java complier
    // will notify the
    // developer if a method is removed from AttributesFactory.
    String amName;
    boolean hasMethod = false;
    assertTrue(af.length != 0);
    for (final Method value : af) {
      am = value;
      amName = am.getName();
      if (!afDeprected.contains(amName) && (amName.startsWith("set") || amName.startsWith("add"))) {
        for (final Method method : rf) {
          rm = method;
          if (rm.getName().equals(am.getName())) {
            Class[] rparams = rm.getParameterTypes();
            Class[] aparams = am.getParameterTypes();
            if (rparams.length == aparams.length) {
              boolean hasAllParams = true;
              for (int k = 0; k < rparams.length; k++) {
                if (aparams[k] != rparams[k]) {
                  hasAllParams = false;
                  break;
                }
              } // parameter check
              if (hasAllParams) {
                hasMethod = true;
              }
            }
          }
        } // region factory methods
        if (!hasMethod) {
          methodsToImplement.add(am);
        } else {
          hasMethod = false;
        }
      }
    } // attributes methods

    if (methodsToImplement.size() > 0) {
      fail("RegionFactory does not conform to AttributesFactory, its should proxy these methods "
          + methodsToImplement);
    }
  }

  @Test
  public void testPARTITION() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testPARTITION_REDUNDANT() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testPARTITION_PERSISTENT() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_PERSISTENT);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testPARTITION_REDUNDANT_PERSISTENT() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT_PERSISTENT);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testPARTITION_OVERFLOW() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_REDUNDANT_OVERFLOW() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_PERSISTENT_OVERFLOW() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_PERSISTENT_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_REDUNDANT_PERSISTENT_OVERFLOW() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT_PERSISTENT_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_HEAP_LRU() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_HEAP_LRU);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_REDUNDANT_HEAP_LRU() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT_HEAP_LRU);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testREPLICATE() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
  }

  @Test
  public void testREPLICATE_PERSISTENT() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE_PERSISTENT);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
  }

  @Test
  public void testREPLICATE_OVERFLOW() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testREPLICATE_PERSISTENT_OVERFLOW() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE_PERSISTENT_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testREPLICATE_HEAP_LRU() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE_HEAP_LRU);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
    assertEquals(new SubscriptionAttributes(InterestPolicy.ALL), ra.getSubscriptionAttributes());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testLOCAL() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
  }

  @Test
  public void testLOCAL_PERSISTENT() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL_PERSISTENT);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
  }

  @Test
  public void testLOCAL_HEAP_LRU() throws Exception {
    Cache c = new CacheFactory(createGemFireProperties()).create();
    RegionFactory factory = c.createRegionFactory(LOCAL_HEAP_LRU);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testLOCAL_OVERFLOW() throws Exception {
    Cache c = new CacheFactory(createGemFireProperties()).create();
    RegionFactory factory = c.createRegionFactory(LOCAL_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testLOCAL_PERSISTENT_OVERFLOW() throws Exception {
    Cache c = new CacheFactory(createGemFireProperties()).create();
    RegionFactory factory = c.createRegionFactory(LOCAL_PERSISTENT_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_PROXY() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_PROXY);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(0, ra.getPartitionAttributes().getLocalMaxMemory());
  }

  @Test
  public void testPARTITION_PROXY_REDUNDANT() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_PROXY_REDUNDANT);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(0, ra.getPartitionAttributes().getLocalMaxMemory());
  }

  @Test
  public void testREPLICATE_PROXY() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.EMPTY, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
  }

  @Test
  public void testSetCacheLoader() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
    CacheLoader cl = new MyCacheLoader();
    r1 = factory.setCacheLoader(cl).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(cl, ra.getCacheLoader());
  }

  @Test
  public void testSetCacheWriter() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
    CacheWriter cw = new MyCacheWriter();
    r1 = factory.setCacheWriter(cw).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(cw, ra.getCacheWriter());
  }

  @Test
  public void testAddCacheListener() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
    CacheListener cl = new MyCacheListener();
    r1 = factory.addCacheListener(cl).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(cl, ra.getCacheListener());
  }

  @Test
  public void testInitCacheListener() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
    CacheListener cl1 = new MyCacheListener();
    CacheListener cl2 = new MyCacheListener();
    r1 = factory.initCacheListeners(new CacheListener[] {cl1, cl2}).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, Arrays.equals(new CacheListener[] {cl1, cl2}, ra.getCacheListeners()));
  }

  @Test
  public void testSetEvictionAttributes() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(77))
        .create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(EvictionAttributes.createLRUEntryAttributes(77), ra.getEvictionAttributes());
  }

  @Test
  public void testSetEntryIdleTimeout() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setEntryIdleTimeout(ea).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ea, ra.getEntryIdleTimeout());
  }

  @Test
  public void testSetCustomEntryIdleTimeout() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    MyCustomExpiry ce = new MyCustomExpiry();
    r1 = factory.setCustomEntryIdleTimeout(ce).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ce, ra.getCustomEntryIdleTimeout());
  }

  @Test
  public void testSetEntryTimeToLive() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setEntryTimeToLive(ea).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ea, ra.getEntryTimeToLive());
  }

  @Test
  public void testSetCustomEntryTimeToLive() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    MyCustomExpiry ce = new MyCustomExpiry();
    r1 = factory.setCustomEntryTimeToLive(ce).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ce, ra.getCustomEntryTimeToLive());
  }

  @Test
  public void testSetRegionIdleTimeout() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setRegionIdleTimeout(ea).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ea, ra.getRegionIdleTimeout());
  }

  @Test
  public void testSetRegionTimeToLive() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setRegionTimeToLive(ea).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ea, ra.getRegionTimeToLive());
  }

  @Test
  public void testSetScope() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setScope(Scope.GLOBAL).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(Scope.GLOBAL, ra.getScope());
  }

  @Test
  public void testSetDataPolicy() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setDataPolicy(DataPolicy.REPLICATE).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
  }

  @Test
  public void testSetEarlyAck() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setEarlyAck(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.getEarlyAck());
  }

  @Test
  public void testSetMulticastEnabled() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setMulticastEnabled(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.getMulticastEnabled());
  }

  @Test
  public void testSetEnableSubscriptionConflation() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setEnableSubscriptionConflation(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.getEnableSubscriptionConflation());
  }

  @Test
  public void testSetKeyConstraint() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setKeyConstraint(String.class).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(String.class, ra.getKeyConstraint());
  }

  @Test
  public void testSetValueConstraint() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setValueConstraint(String.class).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(String.class, ra.getValueConstraint());
  }

  @Test
  public void testSetInitialCapacity() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setInitialCapacity(777).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(777, ra.getInitialCapacity());
  }

  @Test
  public void testSetLoadFactor() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setLoadFactor(77.7f).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(77.7f, ra.getLoadFactor(), 0);
  }

  @Test
  public void testSetConcurrencyLevel() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setConcurrencyLevel(7).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(7, ra.getConcurrencyLevel());
  }

  @Test
  public void testSetDiskStoreName() throws Exception {
    Cache c = createCache();
    c.createDiskStoreFactory().create("ds");
    RegionFactory factory = c.createRegionFactory(LOCAL_PERSISTENT);
    r1 = factory.setDiskStoreName("ds").create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals("ds", ra.getDiskStoreName());
  }

  @Test
  public void testSetDiskSynchronous() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL_PERSISTENT);
    r1 = factory.setDiskSynchronous(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.isDiskSynchronous());
  }

  @Test
  public void testSetPartitionAttributes() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory();
    PartitionAttributes pa = new PartitionAttributesFactory().setTotalNumBuckets(77).create();
    r1 = factory.setPartitionAttributes(pa).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(pa, ra.getPartitionAttributes());
  }

  @Test
  public void testSetMembershipAttributes() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory();
    MembershipAttributes ma = new MembershipAttributes(new String[] {"role1", "role2"});
    r1 = factory.setMembershipAttributes(ma).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ma, ra.getMembershipAttributes());
  }

  @Test
  public void testSetIndexMaintenanceSynchronous() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE);
    r1 = factory.setIndexMaintenanceSynchronous(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.getIndexMaintenanceSynchronous());
  }

  @Test
  public void testSetStatisticsEnabled() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setStatisticsEnabled(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.getStatisticsEnabled());
  }

  @Test
  public void testSetIgnoreJTA() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE);
    r1 = factory.setIgnoreJTA(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.getIgnoreJTA());
  }

  @Test
  public void testSetLockGrantor() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE);
    r1 = factory.setScope(Scope.GLOBAL).setLockGrantor(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.isLockGrantor());
  }

  @Test
  public void testSetSubscriptionAttributes() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(REPLICATE);
    SubscriptionAttributes sa = new SubscriptionAttributes(InterestPolicy.ALL);
    r1 = factory.setSubscriptionAttributes(sa).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(sa, ra.getSubscriptionAttributes());
  }

  @Test
  public void testSetCloningEnabled() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setCloningEnabled(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.getCloningEnabled());
  }

  @Test
  public void testSetPoolName() throws Exception {
    Cache c = createCache();
    PoolManager.createFactory().addServer("127.0.0.1", 7777).create("myPool");
    RegionFactory factory = c.createRegionFactory(LOCAL);
    r1 = factory.setPoolName("myPool").create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals("myPool", ra.getPoolName());
  }

  @Test
  public void testBug45749() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT);
    factory.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(5).create());
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(5, ra.getPartitionAttributes().getTotalNumBuckets());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testBug45749part2() throws Exception {
    Cache c = createCache();
    RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT);
    factory.setPartitionAttributes(
        new PartitionAttributesFactory().setTotalNumBuckets(5).setRedundantCopies(2).create());
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(5, ra.getPartitionAttributes().getTotalNumBuckets());
    assertEquals(2, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testCacheCloseUsingTryWithResources() throws Exception {
    Cache extCache;

    // try block creates and close cache
    try (Cache cache = new CacheFactory(createGemFireProperties()).create()) {
      extCache = cache;
      assertEquals(false, cache.isClosed());
    }

    if (extCache != null) {
      assertEquals(true, extCache.isClosed());
    } else {
      fail("CacheFactory must be created.");
    }
  }

  @Test
  public void testRegionOffHeapAttributeInherent() throws Exception {
    File xmlFile = null;
    try {
      final Properties gemfireProperties = createGemFireProperties();
      gemfireProperties.put(MCAST_TTL, "64");
      gemfireProperties.put("off-heap-memory-size", "4096m");
      final String xmlFileName = getName() + "-cache.xml";
      gemfireProperties.put(CACHE_XML_FILE, xmlFileName);
      xmlFile = new File(xmlFileName);
      xmlFile.delete();
      xmlFile.createNewFile();
      FileWriter f = new FileWriter(xmlFile);
      final String attrsId1 = getName() + "-attrsId1";
      final String attrsId2 = getName() + "-attrsId2";

      f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n"
          + "<cache "
          + "    xmlns=\"http://geode.apache.org/schema/cache\"\n"
          + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
          + "    xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\"\n"
          + "    version=\"1.0\">\n"
          + " <region-attributes id=\"" + attrsId1
          + "\" off-heap=\"true\" statistics-enabled=\"true\" scope=\"distributed-ack\">\n"
          + " </region-attributes>\n"
          + " <region-attributes id=\"" + attrsId2 + "\" refid=\"" + attrsId1 + "\">\n"
          + " </region-attributes>\n"
          + "</cache>");
      f.close();

      RegionFactory factory = new RegionFactory(gemfireProperties, attrsId2);
      r1 = factory.create(r1Name);
      assertBasicRegionFunctionality(r1, r1Name);
      assertEquals(gemfireProperties.get(MCAST_TTL),
          r1.getCache().getDistributedSystem().getProperties().get(MCAST_TTL));
      assertEquals(gemfireProperties.get(CACHE_XML_FILE),
          r1.getCache().getDistributedSystem().getProperties().get(CACHE_XML_FILE));
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ra.getStatisticsEnabled(), true);
      assertEquals(ra.getScope().isDistributedAck(), true);
      assertEquals(ra.getOffHeap(), true);
    } finally {
      if (xmlFile != null) {
        xmlFile.delete();
      }
    }
  }


  private String getName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  private Properties createGemFireProperties() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(LOCATORS, "");
    return props;
  }

  private Cache createCache() {
    return new CacheFactory(createGemFireProperties()).create();
  }

  private void cleanUpRegion(Region r) {
    if (r != null && !r.getCache().isClosed() && !r.isDestroyed()
        && r.getCache().getDistributedSystem().isConnected()) {
      cache = r.getCache();
      distSys = cache.getDistributedSystem();
      r.localDestroyRegion();
    }
  }

  private void assertBasicRegionFunctionality(Region r, String name) {
    assertEquals(r.getName(), name);
    r.put(key, val);
    assertEquals(r.getEntry(key).getValue(), val);
  }

  private static void assertRegionAttributes(RegionAttributes ra1, RegionAttributes ra2) {
    assertEquals(ra1.getScope(), ra2.getScope());
    assertEquals(ra1.getKeyConstraint(), ra2.getKeyConstraint());
    assertEquals(ra1.getValueConstraint(), ra2.getValueConstraint());
    assertEquals(ra1.getCacheListener(), ra2.getCacheListener());
    assertEquals(ra1.getCacheWriter(), ra2.getCacheWriter());
    assertEquals(ra1.getCacheLoader(), ra2.getCacheLoader());
    assertEquals(ra1.getStatisticsEnabled(), ra2.getStatisticsEnabled());
    assertEquals(ra1.getConcurrencyLevel(), ra2.getConcurrencyLevel());
    assertEquals(ra1.getInitialCapacity(), ra2.getInitialCapacity());
    assertTrue(ra1.getLoadFactor() == ra2.getLoadFactor());
    assertEquals(ra1.getEarlyAck(), ra2.getEarlyAck());
    assertEquals(ra1.isDiskSynchronous(), ra2.isDiskSynchronous());
    assertEquals(ra1.getDiskStoreName(), ra2.getDiskStoreName());
  }

  private static class MyCacheListener extends CacheListenerAdapter {
  }

  private static class MyCacheLoader implements CacheLoader {
    @Override
    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return null;
    }

    @Override
    public void close() {}
  }

  private static class MyCacheWriter extends CacheWriterAdapter {
  }

  private static class MyCustomExpiry implements CustomExpiry {
    @Override
    public ExpirationAttributes getExpiry(Region.Entry entry) {
      return null;
    }

    @Override
    public void close() {}
  }
}
