/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import java.util.Properties;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.ResourceManagerCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.SerializerCreation;

import dunit.DistributedTestCase;
import dunit.Host;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Tests 6.1 cache.xml features.
 * 
 * @author aingle, skumar
 * @since 6.1
 */

public class CacheXml61DUnitTest extends CacheXml60DUnitTest {
  
  // ////// Constructors

  public CacheXml61DUnitTest(String name) {
    super(name);
  }

  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_6_1;
  }


  /**
   * Tests that a region created with a named attributes set programmatically
   * for delta propogation has the correct attributes.
   * 
   */
  public void testRegionAttributesForRegionEntryCloning() throws CacheException
  {
    final String rNameBase = getUniqueName();
    final String r1 = rNameBase + "1";

    // Setting multi-cast via nested region attributes
    CacheCreation creation = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);
    attrs.setEarlyAck(false);
    attrs.setCloningEnable(false);
    attrs.setMulticastEnabled(true);
    creation.createRegion(r1, attrs);
    
    testXml(creation);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    Region reg1 = c.getRegion(r1);
    assertNotNull(reg1);
    assertEquals(Scope.LOCAL, reg1.getAttributes().getScope());
    assertFalse(reg1.getAttributes().getEarlyAck());
    assertTrue(reg1.getAttributes().getMulticastEnabled());
    assertFalse(reg1.getAttributes().getCloningEnabled());
    
    //  changing Clonned setting
    reg1.getAttributesMutator().setCloningEnabled(true);
    assertTrue(reg1.getAttributes().getCloningEnabled());

    reg1.getAttributesMutator().setCloningEnabled(false);
    assertFalse(reg1.getAttributes().getCloningEnabled());
    
    // for sub region - a child attribute should be inherited
    String sub = "subRegion";
    RegionAttributesCreation attrsSub = new RegionAttributesCreation(creation);
    attrsSub.setScope(Scope.LOCAL);
    reg1.createSubregion(sub, attrsSub);
    Region subRegion = reg1.getSubregion(sub);
    assertFalse(subRegion.getAttributes().getCloningEnabled());
    subRegion.getAttributesMutator().setCloningEnabled(true);
    assertTrue(subRegion.getAttributes().getCloningEnabled());
  }
}
