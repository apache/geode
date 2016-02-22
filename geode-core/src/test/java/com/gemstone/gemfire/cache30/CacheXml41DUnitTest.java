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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.xml.sax.SAXException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheXmlException;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.IgnoredException;

/**
 * Tests the declarative caching functionality introduced in GemFire 4.1.
 * 
 * @author David Whitlock
 * @since 4.1
 */

public class CacheXml41DUnitTest extends CacheXml40DUnitTest
{

  // ////// Constructors

  public CacheXml41DUnitTest(String name) {
    super(name);
  }

  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_4_1;
  }

  // ////// Test methods

  
  public void setBridgeAttributes(CacheServer bridge1)
  {
    super.setBridgeAttributes(bridge1);
    bridge1.setMaximumTimeBetweenPings(12345);
    bridge1.setNotifyBySubscription(true);
    bridge1.setSocketBufferSize(98765);
  }

  /**
   * Tests that named region attributes are registered when the cache is
   * created.
   */
  public void testRegisteringNamedRegionAttributes()
  {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs;

    String id1 = "id1";
    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setMirrorType(MirrorType.KEYS);
    cache.setRegionAttributes(id1, attrs);

    String id2 = "id2";
    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_NO_ACK);
    attrs.setMirrorType(MirrorType.KEYS_VALUES);
    attrs.setConcurrencyLevel(15);
    cache.setRegionAttributes(id2, attrs);

    String id3 = "id3";
    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.LOCAL);
    attrs.setValueConstraint(Integer.class);
    cache.setRegionAttributes(id3, attrs);

    testXml(cache);
  }

  /**
   * Tests that a region created with a named attributes has the correct
   * attributes.
   */
  public void testNamedAttributes() throws CacheException
  {
    setXmlFile(findFile("namedAttributes.xml"));

    Class keyConstraint = String.class;
    Class valueConstraint = Integer.class;
    String id = "id1";
    String regionName = "root";

    Cache cache = getCache();
    RegionAttributes attrs = cache.getRegionAttributes(id);
    assertEquals(keyConstraint, attrs.getKeyConstraint());
    assertEquals(valueConstraint, attrs.getValueConstraint());
    assertEquals(45, attrs.getEntryIdleTimeout().getTimeout());
    assertEquals(ExpirationAction.INVALIDATE, attrs.getEntryIdleTimeout().getAction());

    Region region = cache.getRegion(regionName);
    assertNotNull(region);

    attrs = region.getAttributes();
    assertEquals(keyConstraint, attrs.getKeyConstraint());
    assertEquals(valueConstraint, attrs.getValueConstraint());
    assertEquals(45, attrs.getEntryIdleTimeout().getTimeout());
    assertEquals(ExpirationAction.INVALIDATE, attrs.getEntryIdleTimeout().getAction());

    // Make sure that attributes can be "overridden"
    Region subregion = region.getSubregion("subregion");
    assertNotNull(subregion);

    attrs = subregion.getAttributes();
    assertEquals(keyConstraint, attrs.getKeyConstraint());
    assertEquals(Long.class, attrs.getValueConstraint());
    assertEquals(90, attrs.getEntryIdleTimeout().getTimeout());
    assertEquals(ExpirationAction.DESTROY, attrs.getEntryIdleTimeout().getAction());

    // Make sure that a named region attributes used in a region
    // declaration is registered
    assertNotNull(cache.getRegionAttributes("id2"));
  }

  /**
   * Tests that trying to parse an XML file that declares a region whose
   * attributes refer to an unknown named region attributes throws an
   * {@link IllegalStateException}.
   */
  public void testUnknownNamedAttributes()
  {
    setXmlFile(findFile("unknownNamedAttributes.xml"));

    IgnoredException expectedException = IgnoredException.addIgnoredException(LocalizedStrings.RegionAttributesCreation_CANNOT_REFERENCE_NONEXISTING_REGION_ATTRIBUTES_NAMED_0.toLocalizedString());
    try {
      getCache();
      fail("Should have thrown an IllegalStateException");

    }
    catch (IllegalStateException ex) {
      // pass...
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests to make sure that we cannot create the same region multiple times in
   * a <code>cache.xml</code> file.
   */
  public void testCreateSameRegionTwice() throws CacheException
  {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    String name = "root";

    cache.createRegion(name, attrs);

    try {
      cache.createRegion(name, attrs);
      fail("Should have thrown a RegionExistsException");

    }
    catch (RegionExistsException ex) {
      // pass...
    }

    setXmlFile(findFile("sameRootRegion.xml"));

    IgnoredException expectedException = IgnoredException.addIgnoredException("While reading Cache XML file");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    }
    catch (CacheXmlException ex) {
      Throwable cause = ex.getCause();
      assertTrue(cause instanceof SAXException);
      cause = ((SAXException)cause).getException();
      if (!(cause instanceof RegionExistsException)) {
        Assert.fail("Expected a RegionExistsException, not a "
            + cause.getClass().getName(), cause);
      }
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests to make sure that we cannot create the same subregion multiple times
   * in a <code>cache.xml</code> file.
   */
  public void testCreateSameSubregionTwice() throws CacheException
  {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    String name = this.getUniqueName();

    Region root = cache.createRegion("root", attrs);

    root.createSubregion(name, attrs);

    try {
      root.createSubregion(name, attrs);
      fail("Should have thrown a RegionExistsException");

    }
    catch (RegionExistsException ex) {
      // pass...
    }

    setXmlFile(findFile("sameSubregion.xml"));

    IgnoredException expectedException = IgnoredException.addIgnoredException("While reading Cache XML file");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    }
    catch (CacheXmlException ex) {
      Throwable cause = ex.getCause();
      assertTrue(cause instanceof SAXException);
      cause = ((SAXException)cause).getException();
      if (!(cause instanceof RegionExistsException)) {
        Assert.fail("Expected a RegionExistsException, not a "
            + cause.getClass().getName(), cause);
      }
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Generates XML from the given <code>CacheCreation</code> and returns an
   * <code>InputStream</code> for reading that XML.
   */
  public InputStream generate(CacheCreation creation)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    final boolean useSchema = getUseSchema();
    final String version = getGemFireVersion();

    PrintWriter pw = new PrintWriter(new OutputStreamWriter(baos), true);
    CacheXmlGenerator.generate(creation, pw, useSchema, version);
    pw.close();

    byte[] bytes = baos.toByteArray();
    return new ByteArrayInputStream(bytes);
  }

  /**
   * Tests that loading cache XML effects mutable cache attributes.
   */
  public void testModifyCacheAttributes() throws CacheException
  {
    boolean copyOnRead1 = false;
    boolean isServer1 = true;
    int lockLease1 = 123;
    int lockTimeout1 = 345;
    int searchTimeout1 = 567;

    CacheCreation creation = new CacheCreation();
    creation.setCopyOnRead(copyOnRead1);
    creation.setIsServer(isServer1);
    creation.setLockLease(lockLease1);
    creation.setLockTimeout(lockTimeout1);
    creation.setSearchTimeout(searchTimeout1);

    testXml(creation);

    Cache cache = getCache();
    assertEquals(copyOnRead1, cache.getCopyOnRead());
    assertEquals(isServer1, cache.isServer());
    assertEquals(lockLease1, cache.getLockLease());
    assertEquals(lockTimeout1, cache.getLockTimeout());
    assertEquals(searchTimeout1, cache.getSearchTimeout());

    boolean copyOnRead2 = true;
    boolean isServer2 = false;
    int lockLease2 = 234;
    int lockTimeout2 = 456;
    int searchTimeout2 = 678;

    creation = new CacheCreation();
    creation.setCopyOnRead(copyOnRead2);
    creation.setIsServer(isServer2);
    creation.setLockLease(lockLease2);
    creation.setLockTimeout(lockTimeout2);
    creation.setSearchTimeout(searchTimeout2);

    cache.loadCacheXml(generate(creation));

    assertEquals(copyOnRead2, cache.getCopyOnRead());
    assertEquals(isServer2, cache.isServer());
    assertEquals(lockLease2, cache.getLockLease());
    assertEquals(lockTimeout2, cache.getLockTimeout());
    assertEquals(searchTimeout2, cache.getSearchTimeout());
  }

  /**
   * Tests that loading cache XML can create a region.
   */
  public void testAddRegionViaCacheXml() throws CacheException
  {
    CacheCreation creation = new CacheCreation();

    testXml(creation);

    Cache cache = getCache();
    assertTrue(cache.rootRegions().isEmpty());

    creation = new CacheCreation();

    RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.GLOBAL);
    attrs.setKeyConstraint(Integer.class);
    attrs.setCacheListener(new MyTestCacheListener());
    Region root = creation.createRegion("root", attrs);

    attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);
    attrs.setEarlyAck(true);
    attrs.setValueConstraint(String.class);
    Region subregion = root.createSubregion("subregion", attrs);

    cache.loadCacheXml(generate(creation));

    root = cache.getRegion("root");
    assertNotNull(root);
    assertEquals(Scope.GLOBAL, root.getAttributes().getScope());
    assertEquals(Integer.class, root.getAttributes().getKeyConstraint());
    assertTrue(root.getAttributes().getCacheListener() instanceof MyTestCacheListener);

    subregion = root.getSubregion("subregion");
    assertNotNull(subregion);
    assertEquals(Scope.LOCAL, subregion.getAttributes().getScope());
    assertTrue(subregion.getAttributes().getEarlyAck());
    assertFalse(subregion.getAttributes().getMulticastEnabled());
    assertEquals(String.class, subregion.getAttributes().getValueConstraint());

    // Create a subregion of a region that already exists

    creation = new CacheCreation();
    attrs = new RegionAttributesCreation(creation);
    root = creation.createRegion("root", attrs);

    attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setEarlyAck(false);
    attrs.setValueConstraint(Long.class);
    Region subregion2 = root.createSubregion("subregion2", attrs);

    cache.loadCacheXml(generate(creation));

    subregion2 = root.getSubregion("subregion2");
    assertNotNull(subregion2);
    assertEquals(Scope.DISTRIBUTED_ACK, subregion2.getAttributes().getScope());
    assertTrue(!subregion2.getAttributes().getEarlyAck());
    assertEquals(Long.class, subregion2.getAttributes().getValueConstraint());
  }

  /**
   * Tests that loading cache XML can modify a region.
   */
  public void testModifyRegionViaCacheXml() throws CacheException
  {
    CacheCreation creation = new CacheCreation();

    int timeout1a = 123;
    ExpirationAction action1a = ExpirationAction.LOCAL_DESTROY;
    int timeout1b = 456;
    ExpirationAction action1b = ExpirationAction.DESTROY;

    RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setStatisticsEnabled(true);
    attrs.setEntryIdleTimeout(new ExpirationAttributes(timeout1a, action1a));
    Region root = creation.createRegion("root", attrs);

    attrs = new RegionAttributesCreation(creation);
    attrs.setStatisticsEnabled(true);
    attrs.setEntryIdleTimeout(new ExpirationAttributes(timeout1b, action1b));
    Region subregion = root.createSubregion("subregion", attrs);

    testXml(creation);

    Cache cache = getCache();

    root = cache.getRegion("root");
    assertEquals(timeout1a, root.getAttributes().getEntryIdleTimeout()
        .getTimeout());
    assertEquals(action1a, root.getAttributes().getEntryIdleTimeout()
        .getAction());

    subregion = root.getSubregion("subregion");
    assertEquals(timeout1b, subregion.getAttributes().getEntryIdleTimeout()
        .getTimeout());
    assertEquals(action1b, subregion.getAttributes().getEntryIdleTimeout()
        .getAction());

    creation = new CacheCreation();

    int timeout2a = 234;
    ExpirationAction action2a = ExpirationAction.LOCAL_INVALIDATE;
    int timeout2b = 567;
    ExpirationAction action2b = ExpirationAction.INVALIDATE;

    attrs = new RegionAttributesCreation(creation);
    attrs.setStatisticsEnabled(true);
    attrs.setEntryIdleTimeout(new ExpirationAttributes(timeout2a, action2a));
    attrs.setCacheListener(new MyTestCacheListener());
    root = creation.createRegion("root", attrs);

    attrs = new RegionAttributesCreation(creation);
    attrs.setStatisticsEnabled(true);
    attrs.setEntryIdleTimeout(new ExpirationAttributes(timeout2b, action2b));
    subregion = root.createSubregion("subregion", attrs);

    cache.loadCacheXml(generate(creation));

    root = cache.getRegion("root");
    subregion = root.getSubregion("subregion");

    assertEquals(timeout2a, root.getAttributes().getEntryIdleTimeout()
        .getTimeout());
    assertEquals(action2a, root.getAttributes().getEntryIdleTimeout()
        .getAction());
    assertTrue(root.getAttributes().getCacheListener() instanceof MyTestCacheListener);

    assertEquals(timeout2b, subregion.getAttributes().getEntryIdleTimeout()
        .getTimeout());
    assertEquals(action2b, subregion.getAttributes().getEntryIdleTimeout()
        .getAction());
  }

  /**
   * Tests that loading cache XML can add/update entries to a region.
   */
  public void testAddEntriesViaCacheXml() throws CacheException
  {
    String key1 = "KEY1";
    String value1 = "VALUE1";

    CacheCreation creation = new CacheCreation();

    RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);

    Region root = creation.createRegion("root", attrs);
    root.put(key1, value1);

    testXml(creation);

    Cache cache = getCache();
    root = cache.getRegion("root");
    assertEquals(1, root.entries(false).size());
    assertEquals(value1, root.get(key1));

    creation = new CacheCreation();

    attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);

    String value2 = "VALUE2";
    String key2 = "KEY2";
    String value3 = "VALUE3";

    root = creation.createRegion("root", attrs);
    root.put(key1, value2);
    root.put(key2, value3);

    cache.loadCacheXml(generate(creation));

    root = cache.getRegion("root");
    assertEquals(2, root.entries(false).size());
    assertEquals(value2, root.get(key1));
    assertEquals(value3, root.get(key2));

  }
  
  // this tests an aspect of the CapacityController interface, which is no longer
  // available as of 5.0
  //public void testHeapLRUCapacityController() throws Exception {
  //  final String name = getUniqueName();
  //  beginCacheXml();
  //  AttributesFactory factory = new AttributesFactory();
  //  factory.setScope(Scope.LOCAL);
  //  factory.setCapacityController(new HeapLRUCapacityController(42, 32, LRUAlgorithm.OVERFLOW_TO_DISK));
  //  createRegion(name, factory.create());
  //  finishCacheXml(getUniqueName());
  //  
  //  Region r = getRootRegion().getSubregion(name);
  //  
  //  HeapLRUCapacityController hlcc = (HeapLRUCapacityController) r.getAttributes().getCapacityController();
  //  assertEquals(hlcc.getEvictionAction(), LRUAlgorithm.OVERFLOW_TO_DISK);
  //  
  //  Properties p = hlcc.getProperties();
  //  assertEquals(42, Integer.parseInt(p.getProperty(HeapLRUCapacityController.HEAP_PERCENTAGE)));
  //  assertEquals(32, Long.parseLong(p.getProperty(HeapLRUCapacityController.EVICTOR_INTERVAL)));
  //  assertEquals(LRUAlgorithm.OVERFLOW_TO_DISK, p.getProperty(HeapLRUCapacityController.EVICTION_ACTION));
  //}
  /**
   * Test Publisher region attribute
   * @since 4.2.3
   * @deprecated as of GemFire 6.5.
   */
  public void testPublisherAttribute() throws CacheException {

//    CacheCreation cache = new CacheCreation();
//    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
//    attrs.setPublisher(true);
//    cache.createRegion("root", attrs);
//    testXml(cache);
//    assertEquals(true, cache.getRegion("root").getAttributes().getPublisher());
  }

  /**
   * Test EnableBridgeConflation region attribute
   * @since 4.2
   */
  public void testEnableBridgeConflationAttribute() throws CacheException {

    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setEnableBridgeConflation(true);
    cache.createRegion("root", attrs);
    testXml(cache);
    assertEquals(true, cache.getRegion("root").getAttributes().getEnableBridgeConflation());
  }

  /**
   * Test EnableAsyncConflation region attribute
   * @since 4.2
   */
  public void testEnableAsyncConflationAttribute() throws CacheException {

    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setEnableAsyncConflation(true);
    cache.createRegion("root", attrs);
    testXml(cache);
    assertEquals(true, cache.getRegion("root").getAttributes().getEnableAsyncConflation());
  }
  /**
   * @since 4.3
   */
  public void testDynamicRegionFactoryDefault() throws CacheException {
    CacheCreation cache = new CacheCreation();
    cache.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config());
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    cache.createRegion("root", attrs);
    // note that testXml can't check if they are same because enabling
    // dynamic regions causes a meta region to be produced.
    testXml(cache, false);
    assertEquals(true, DynamicRegionFactory.get().getConfig().getRegisterInterest());
    assertEquals(true, DynamicRegionFactory.get().getConfig().getPersistBackup());
    assertEquals(true, DynamicRegionFactory.get().isOpen());
    assertEquals(null, DynamicRegionFactory.get().getConfig().getDiskDir());
    Region dr = getCache().getRegion("__DynamicRegions");    
    if(dr != null) {
        dr.localDestroyRegion();      
    }
    
  }
  public void testDynamicRegionFactoryNonDefault() throws CacheException {
    CacheCreation cache = new CacheCreation();
    cache.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config((File)null, null, false, false));
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    cache.createRegion("root", attrs);
    // note that testXml can't check if they are same because enabling
    // dynamic regions causes a meta region to be produced.
    testXml(cache, false);
    assertEquals(false, DynamicRegionFactory.get().getConfig().getRegisterInterest());
    assertEquals(false, DynamicRegionFactory.get().getConfig().getPersistBackup());
    assertEquals(true, DynamicRegionFactory.get().isOpen());
    assertEquals(null, DynamicRegionFactory.get().getConfig().getDiskDir());
    Region dr = getCache().getRegion("__DynamicRegions");    
    if(dr != null) {
        dr.localDestroyRegion();      
    }
    
  }

  /**
   * @since 4.3
   */
  public void testDynamicRegionFactoryDiskDir() throws CacheException {
    CacheCreation cache = new CacheCreation();
    File f = new File("diskDir");
    f.mkdirs();
    cache.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config(f, null, true, true));
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    cache.createRegion("root", attrs);
    // note that testXml can't check if they are same because enabling
    // dynamic regions causes a meta region to be produced.
    testXml(cache, false);
    assertEquals(true, DynamicRegionFactory.get().isOpen());
    assertEquals(f.getAbsoluteFile(), DynamicRegionFactory.get().getConfig().getDiskDir());
    Region dr =getCache().getRegion("__DynamicRegions");    
    if(dr != null) {
        dr.localDestroyRegion();      
    }
  }

  /**
   * Remove this override when bug #52052 is fixed.
   */
  public void testExampleCacheXmlFile() {
    return;
  }
}
