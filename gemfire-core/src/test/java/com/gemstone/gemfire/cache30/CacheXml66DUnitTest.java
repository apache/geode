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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.util.TransactionListenerAdapter;
import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.partitioned.fixed.QuarterPartitionResolver;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheTransactionManagerCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.ClientCacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Tests 7.0 cache.xml feature : Fixed Partitioning.
 * 
 * @author kbachhav
 * @since 6.6
 */
public class CacheXml66DUnitTest extends CacheXml65DUnitTest{
  
//////// Constructors

  public CacheXml66DUnitTest(String name) {
    super(name);
  }

  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_6_6;
  }

  
  /**
   * Tests that a partitioned region is created with FixedPartitionAttributes
   * set programatically and correct cache.xml is generated with the same
   * FixedPartitionAttributes
   * 
   */
  public void testFixedPartitioning() throws CacheException {

    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation();
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition("Q1");
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition("Q2", true);
    FixedPartitionAttributes fpa3 = FixedPartitionAttributes
        .createFixedPartition("Q3", 3);
    FixedPartitionAttributes fpa4 = FixedPartitionAttributes
        .createFixedPartition("Q4", false, 3);
    List<FixedPartitionAttributes> fpattrsList = new ArrayList<FixedPartitionAttributes>();
    fpattrsList.add(fpa1);
    fpattrsList.add(fpa2);
    fpattrsList.add(fpa3);
    fpattrsList.add(fpa4);

    QuarterPartitionResolver resolver = new QuarterPartitionResolver();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setPartitionResolver(resolver)
        .addFixedPartitionAttributes(fpa1).addFixedPartitionAttributes(fpa2)
        .addFixedPartitionAttributes(fpa3).addFixedPartitionAttributes(fpa4);

    attrs.setPartitionAttributes(paf.create());
    cache.createRegion("Quarter", attrs);
    Region r = cache.getRegion("Quarter");
    validateAttributes(r, fpattrsList, resolver, false);

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);
    Region region = c.getRegion("Quarter");
    assertNotNull(region);
    validateAttributes(region, fpattrsList, resolver, false);
  }

  public void testFixedPartitioning_colocation_WithAttributes()
      throws CacheException {
    CacheCreation cache = new CacheCreation();
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition("Q1");
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition("Q2", true);
    FixedPartitionAttributes fpa3 = FixedPartitionAttributes
        .createFixedPartition("Q3", 3);
    FixedPartitionAttributes fpa4 = FixedPartitionAttributes
        .createFixedPartition("Q4", false, 3);
    List<FixedPartitionAttributes> fpattrsList = new ArrayList<FixedPartitionAttributes>();
    fpattrsList.add(fpa1);
    fpattrsList.add(fpa2);
    fpattrsList.add(fpa3);
    fpattrsList.add(fpa4);
    QuarterPartitionResolver resolver = new QuarterPartitionResolver();
    Region customerRegion = null;
    Region orderRegion = null;

    {
      RegionAttributesCreation attrs = new RegionAttributesCreation();
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setPartitionResolver(resolver)
          .addFixedPartitionAttributes(fpa1).addFixedPartitionAttributes(fpa2)
          .addFixedPartitionAttributes(fpa3).addFixedPartitionAttributes(fpa4);
      attrs.setPartitionAttributes(paf.create());
      cache.createRegion("Customer", attrs);
    }
    customerRegion = cache.getRegion("Customer");
    validateAttributes(customerRegion, fpattrsList, resolver, false);

    try {
      RegionAttributesCreation attrs = new RegionAttributesCreation();
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setPartitionResolver(resolver)
          .addFixedPartitionAttributes(fpa1).addFixedPartitionAttributes(fpa2)
          .addFixedPartitionAttributes(fpa3).addFixedPartitionAttributes(fpa4)
          .setColocatedWith("Customer");

      attrs.setPartitionAttributes(paf.create());
      cache.createRegion("Order", attrs);
      orderRegion = cache.getRegion("Order");
      validateAttributes(orderRegion, fpattrsList, resolver, true);
    }
    catch (Exception illegal) {
      if (!((illegal instanceof IllegalStateException) && (illegal.getMessage()
          .contains("can not be specified in PartitionAttributesFactory")))) {
        fail("Expected IllegalStateException ", illegal);
      }

      RegionAttributesCreation attrs = new RegionAttributesCreation();
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setPartitionResolver(resolver)
          .setColocatedWith("Customer");

      attrs.setPartitionAttributes(paf.create());
      cache.createRegion("Order", attrs);
      orderRegion = cache.getRegion("Order");
      validateAttributes(orderRegion, fpattrsList, resolver, true);
    }

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);
    customerRegion = c.getRegion("Customer");
    assertNotNull(customerRegion);
    validateAttributes(customerRegion, fpattrsList, resolver, false);

    orderRegion = c.getRegion("Order");
    assertNotNull(orderRegion);
    validateAttributes(orderRegion, fpattrsList, resolver, true);
  }

  private void validateAttributes(Region region,
      List<FixedPartitionAttributes> fpattrsList,
      QuarterPartitionResolver resolver, boolean isColocated) {
    RegionAttributes regionAttrs = region.getAttributes();
    PartitionAttributes pa = regionAttrs.getPartitionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertNotNull(pa.getPartitionResolver().getClass());
    assertEquals(pa.getPartitionResolver(), resolver);
    List<FixedPartitionAttributesImpl> fixedPartitionsList = pa
        .getFixedPartitionAttributes();
    if (isColocated) {
      assertNull(fixedPartitionsList);
      assertNotNull(pa.getColocatedWith());
    }
    else {
      assertNull(pa.getColocatedWith());
      assertEquals(fixedPartitionsList.size(), 4);
      assertEquals(fixedPartitionsList.containsAll(fpattrsList), true);
      for (FixedPartitionAttributes fpa : fixedPartitionsList) {
        if (fpa.getPartitionName().equals("Q1")) {
          assertEquals(fpa.getNumBuckets(), 1);
          assertEquals(fpa.isPrimary(), false);
        }
        if (fpa.getPartitionName().equals("Q2")) {
          assertEquals(fpa.getNumBuckets(), 1);
          assertEquals(fpa.isPrimary(), true);
        }
        if (fpa.getPartitionName().equals("Q3")) {
          assertEquals(fpa.getNumBuckets(), 3);
          assertEquals(fpa.isPrimary(), false);
        }
        if (fpa.getPartitionName().equals("Q4")) {
          assertEquals(fpa.getNumBuckets(), 3);
          assertEquals(fpa.isPrimary(), false);
        }
      }
    }

  }
  
  
  public void testPdxDefaults() {
    CacheCreation creation = new CacheCreation();
    testXml(creation);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    assertEquals(null, c.getPdxDiskStore());
    assertEquals(null, c.getPdxSerializer());
    assertEquals(false, c.getPdxPersistent());
    assertEquals(false, c.getPdxReadSerialized());
    assertEquals(false, c.getPdxIgnoreUnreadFields());
  }
  
  public void testPdxAttributes() {
    CacheCreation creation = new CacheCreation();
    creation.setPdxPersistent(true);
    creation.setPdxReadSerialized(true);
    creation.setPdxIgnoreUnreadFields(true);
    creation.setPdxDiskStore("my_disk_store");
    TestPdxSerializer serializer = new TestPdxSerializer();
    Properties props = new Properties();
    props.setProperty("hello", "there");
    serializer.init(props);
    creation.setPdxSerializer(serializer);
    testXml(creation);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    assertEquals("my_disk_store", c.getPdxDiskStore());
    assertEquals(serializer, c.getPdxSerializer());
    assertEquals(true, c.getPdxPersistent());
    assertEquals(true, c.getPdxReadSerialized());
    assertEquals(true, c.getPdxIgnoreUnreadFields());
    
    //test that we can override the cache.xml attributes
    {
      closeCache();
      CacheFactory cf = new CacheFactory();
      cf.setPdxDiskStore("new disk store");
      c = getCache(cf);
      assertTrue(c instanceof GemFireCacheImpl);
      c.loadCacheXml(generate(creation));
      
      assertEquals("new disk store", c.getPdxDiskStore());
      assertEquals(serializer, c.getPdxSerializer());
      assertEquals(true, c.getPdxPersistent());
      assertEquals(true, c.getPdxReadSerialized());
      assertEquals(true, c.getPdxIgnoreUnreadFields());
    }
    
    {
      closeCache();
      CacheFactory cf = new CacheFactory();
      cf.setPdxPersistent(false);
      cf.setPdxIgnoreUnreadFields(false);
      c = getCache(cf);
      assertTrue(c instanceof GemFireCacheImpl);
      c.loadCacheXml(generate(creation));
      
      assertEquals("my_disk_store", c.getPdxDiskStore());
      assertEquals(serializer, c.getPdxSerializer());
      assertEquals(false, c.getPdxPersistent());
      assertEquals(true, c.getPdxReadSerialized());
      assertEquals(false, c.getPdxIgnoreUnreadFields());
    }
    
    {
      closeCache();
      CacheFactory cf = new CacheFactory();
      cf.setPdxSerializer(null);
      c = getCache(cf);
      assertTrue(c instanceof GemFireCacheImpl);
      c.loadCacheXml(generate(creation));
      
      assertEquals("my_disk_store", c.getPdxDiskStore());
      assertEquals(null, c.getPdxSerializer());
      assertEquals(true, c.getPdxPersistent());
      assertEquals(true, c.getPdxReadSerialized());
      assertEquals(true, c.getPdxIgnoreUnreadFields());
    }
    
    {
      closeCache();
      CacheFactory cf = new CacheFactory();
      cf.setPdxReadSerialized(false);
      c = getCache(cf);
      assertTrue(c instanceof GemFireCacheImpl);
      c.loadCacheXml(generate(creation));
      
      assertEquals("my_disk_store", c.getPdxDiskStore());
      assertEquals(serializer, c.getPdxSerializer());
      assertEquals(true, c.getPdxPersistent());
      assertEquals(false, c.getPdxReadSerialized());
      assertEquals(true, c.getPdxIgnoreUnreadFields());
    }
    
  }
  
  public void testTXManagerOnClientCache() {
    ClientCacheCreation cc = new ClientCacheCreation();
    //CacheCreation cc = new CacheCreation();
    CacheTransactionManagerCreation txMgrCreation = new CacheTransactionManagerCreation();
    txMgrCreation.addListener(new TestTXListener());
    cc.addCacheTransactionManagerCreation(txMgrCreation);
    testXml(cc);
    
    Cache c = getCache();
    assertTrue(c instanceof ClientCache);
    c.loadCacheXml(generate(cc));
    
    ClientCache clientC = (ClientCache) c;
    CacheTransactionManager mgr = clientC.getCacheTransactionManager();
    assertNotNull(mgr);
    assertTrue(mgr.getListeners()[0] instanceof TestTXListener);
    
  }
  
  public void testNoTXWriterOnClient() {
  //test writer is not created
    ClientCacheCreation cc = new ClientCacheCreation();
    CacheTransactionManagerCreation txMgrCreation = new CacheTransactionManagerCreation();
    txMgrCreation.setWriter(new TestTransactionWriter());
    cc.addCacheTransactionManagerCreation(txMgrCreation);
    ExpectedException expectedException = addExpectedException(LocalizedStrings.TXManager_NO_WRITER_ON_CLIENT.toLocalizedString());
    try {
      testXml(cc);
      fail("expected exception not thrown");
    } catch (IllegalStateException e) {
    } finally {
      expectedException.remove();
    }
  }
  
  public static class TestTXListener extends TransactionListenerAdapter implements Declarable {
    public void init(Properties props) {
    }
    @Override
    public boolean equals(Object other) {
      return other instanceof TestTXListener;
    }
  }
}
