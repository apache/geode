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

import com.company.app.DBLoader;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.*;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

import java.io.*;
import java.util.*;

/**
 * Tests the declarative caching functionality introduced in GemFire
 * 5.0 (i.e. congo1). Don't be confused by the 45 in my name :-)
 *
 * @author Darrel Schneider
 * @since 5.0
 */
public class CacheXml45DUnitTest extends CacheXml41DUnitTest {

  ////////  Constructors

  public CacheXml45DUnitTest(String name) {
    super(name);
  }

  ////////  Helper methods

  protected String getGemFireVersion() {
    return CacheXml.VERSION_5_0;
  }

  ////////  Test methods
  

  
  public void setBridgeAttributes(CacheServer bridge1)
  {
    super.setBridgeAttributes(bridge1);
    bridge1.setMaxConnections(100);
  }

  public void testDataPolicy() throws CacheException {
    CacheCreation cache = new CacheCreation();

    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.NORMAL);
      cache.createRegion("rootNORMAL", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.NORMAL);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootNORMAL_ALL", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setMirrorType(MirrorType.KEYS_VALUES);
      cache.createRegion("rootREPLICATE", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      cache.createRegion("rootPERSISTENT_REPLICATE", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.EMPTY);
      cache.createRegion("rootEMPTY", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.EMPTY);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootEMPTY_ALL", attrs);
    }

    testXml(cache);
  }

  /**
   * These properties, if any, will be added to the properties used for getSystem calls
   */
  protected Properties xmlProps = null;

  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    if (this.xmlProps != null) {
      for (Iterator iter = this.xmlProps.entrySet().iterator();
           iter.hasNext(); ) {
        Map.Entry entry = (Map.Entry) iter.next();
        String key = (String) entry.getKey();
        String value = (String) entry.getValue();
        props.setProperty(key, value);
      }
    }
    return props;
  }

  /**
   * Test xml support of MembershipAttributes.
   */
  public void testMembershipAttributes() throws Exception {
    final String MY_ROLES = "Foo, Bip, BAM";
    final String[][] roles = new String[][] {{"Foo"}, {"Bip", "BAM"}};

    final LossAction[] policies = (LossAction[])
      LossAction.VALUES.toArray(
      new LossAction[LossAction.VALUES.size()]);

    final ResumptionAction[] actions = (ResumptionAction[])
      ResumptionAction.VALUES.toArray(
      new ResumptionAction[ResumptionAction.VALUES.size()]);

    CacheCreation cache = new CacheCreation();

    // for each policy, try each action and each role...
    for (int policy = 0; policy < policies.length; policy++) {
      for (int action = 0; action < actions.length; action++) {
        for (int role = 0; role < roles.length; role++) {
          String[] theRoles = roles[role];
          LossAction thePolicy = policies[policy];
          ResumptionAction theAction = actions[action];

          //if (theRoles.length == 0 && (thePolicy != LossAction.NONE || theAction != ResumptionAction.NONE

          RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
          MembershipAttributes ra = new MembershipAttributes(
              theRoles, thePolicy, theAction);
          attrs.setMembershipAttributes(ra);
          String region = "rootMEMBERSHIP_ATTRIBUTES_" +
                          policy + "_" + action + "_" + role;
          cache.createRegion(region, attrs);
        }
      }
    }

    {
      // make our system play the roles used by this test so the create regions
      // will not think the a required role is missing
      Properties config = new Properties();
      config.setProperty("roles", MY_ROLES);
      this.xmlProps = config;
    }
    DistributedRegion.ignoreReconnect = true;
    try {
      testXml(cache);
    } finally {
      this.xmlProps = null;
      try {
        preTearDown();
      } finally {
        DistributedRegion.ignoreReconnect = false;
      }
    }
  }

  /**
   * Tests multiple cache listeners on one region
   * @since 5.0
   */
  public void testMultipleCacheListener() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);

    CacheListener l1 = new MyTestCacheListener();
    CacheListener l2 = new MySecondTestCacheListener();
    attrs.addCacheListener(l1);
    attrs.addCacheListener(l2);

    cache.createRegion("root", attrs);

    testXml(cache);
    {
      Cache c = getCache();
      Region r = c.getRegion("root");
      assertEquals(Arrays.asList(new CacheListener[]{l1, l2}), Arrays.asList(r.getAttributes().getCacheListeners()));
      AttributesMutator am = r.getAttributesMutator();
      am.removeCacheListener(l2);
      assertEquals(Arrays.asList(new CacheListener[]{l1}), Arrays.asList(r.getAttributes().getCacheListeners()));
      am.removeCacheListener(l1);
      assertEquals(Arrays.asList(new CacheListener[]{}), Arrays.asList(r.getAttributes().getCacheListeners()));
      am.addCacheListener(l1);
      assertEquals(Arrays.asList(new CacheListener[]{l1}), Arrays.asList(r.getAttributes().getCacheListeners()));
      am.addCacheListener(l1);
      assertEquals(Arrays.asList(new CacheListener[]{l1}), Arrays.asList(r.getAttributes().getCacheListeners()));
      am.addCacheListener(l2);
      assertEquals(Arrays.asList(new CacheListener[]{l1,l2}), Arrays.asList(r.getAttributes().getCacheListeners()));
      am.removeCacheListener(l1);
      assertEquals(Arrays.asList(new CacheListener[]{l2}), Arrays.asList(r.getAttributes().getCacheListeners()));
      am.removeCacheListener(l1);
      assertEquals(Arrays.asList(new CacheListener[]{l2}), Arrays.asList(r.getAttributes().getCacheListeners()));
      am.initCacheListeners(new CacheListener[]{l1,l2});
      assertEquals(Arrays.asList(new CacheListener[]{l1,l2}), Arrays.asList(r.getAttributes().getCacheListeners()));
    }
  }

  /**
   * A <code>CacheListener</code> that is
   * <code>Declarable</code>, but not <code>Declarable2</code>.
   */
  public static class MySecondTestCacheListener
    extends TestCacheListener implements Declarable {

    public void init(Properties props) { }

    public boolean equals(Object o) {
      return o instanceof MySecondTestCacheListener;
    }
  }

  public void testHeapLRUEviction() throws Exception {
    final String name = getUniqueName();
    beginCacheXml();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    EvictionAttributes ev = EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK);
    factory.setEvictionAttributes(ev);
//    RegionAttributes atts = factory.create();
    createRegion(name, factory.create());
    finishCacheXml(getUniqueName(), getUseSchema(), getGemFireVersion());
    Region r = getRootRegion().getSubregion(name);

    EvictionAttributes hlea = r.getAttributes().getEvictionAttributes();
    assertEquals(EvictionAction.OVERFLOW_TO_DISK, hlea.getAction());
  }

  /**
   * Tests multiple transaction listeners
   * @since 5.0
   */
  public void testMultipleTXListener() throws CacheException {
    CacheCreation cache = new CacheCreation();
    CacheTransactionManagerCreation txMgrCreation = new CacheTransactionManagerCreation();
    TransactionListener l1 = new MyTestTransactionListener();
    TransactionListener l2 = new MySecondTestTransactionListener();
    txMgrCreation.addListener(l1);
    txMgrCreation.addListener(l2);
    cache.addCacheTransactionManagerCreation(txMgrCreation);
    testXml(cache);
    {
      CacheTransactionManager tm = getCache().getCacheTransactionManager();
      assertEquals(Arrays.asList(new TransactionListener[]{l1, l2}), Arrays.asList(tm.getListeners()));
      tm.removeListener(l2);
      assertEquals(Arrays.asList(new TransactionListener[]{l1}), Arrays.asList(tm.getListeners()));
      tm.removeListener(l1);
      assertEquals(Arrays.asList(new TransactionListener[]{}), Arrays.asList(tm.getListeners()));
      tm.addListener(l1);
      assertEquals(Arrays.asList(new TransactionListener[]{l1}), Arrays.asList(tm.getListeners()));
      tm.addListener(l1);
      assertEquals(Arrays.asList(new TransactionListener[]{l1}), Arrays.asList(tm.getListeners()));
      tm.addListener(l2);
      assertEquals(Arrays.asList(new TransactionListener[]{l1,l2}), Arrays.asList(tm.getListeners()));
      tm.removeListener(l1);
      assertEquals(Arrays.asList(new TransactionListener[]{l2}), Arrays.asList(tm.getListeners()));
      tm.removeListener(l1);
      assertEquals(Arrays.asList(new TransactionListener[]{l2}), Arrays.asList(tm.getListeners()));
      tm.initListeners(new TransactionListener[]{l1,l2});
      assertEquals(Arrays.asList(new TransactionListener[]{l1,l2}), Arrays.asList(tm.getListeners()));
    }
  }

  /**
   * A <code>TransactionListener</code> that is
   * <code>Declarable</code>, but not <code>Declarable2</code>.
   */
  public static class MySecondTestTransactionListener
    extends TestTransactionListener implements Declarable {

    public void init(Properties props) { }

    public boolean equals(Object o) {
      return o instanceof MySecondTestTransactionListener;
    }
  }

  /**
   * Tests that a region created with a named attributes has the correct
   * attributes.
   */
  public void testPartitionedRegionXML() throws CacheException
  {
    setXmlFile(findFile("partitionedRegion.xml"));
    final String regionName = "pRoot";

    Cache cache = getCache();
    Region region = cache.getRegion(regionName);
    assertNotNull(region);
    
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    
    CacheSerializableRunnable init = new CacheSerializableRunnable("initUsingPartitionedRegionXML") {
      public void run2() throws CacheException
      {
        final Cache cache;
        try {
          CacheXml30DUnitTest.lonerDistributedSystem = false;
          cache = getCache();
        }
        finally {
          CacheXml30DUnitTest.lonerDistributedSystem = true;
        }
        Region region = cache.getRegion(regionName);
        assertNotNull(region);
        RegionAttributes attrs = region.getAttributes();
        assertNotNull(attrs.getPartitionAttributes());

        PartitionAttributes pa = attrs.getPartitionAttributes();
        // assertNull(pa.getCacheWriter());
        assertEquals(pa.getRedundantCopies(), 1);
        assertEquals(pa.getGlobalProperties().getProperty(
            PartitionAttributesFactory.GLOBAL_MAX_MEMORY_PROPERTY), "96");
        assertEquals(pa.getLocalProperties().getProperty(
            PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY), "32");
      }
    };
    
    init.run2();
    vm0.invoke(init);
    vm1.invoke(init);
    vm0.invoke(new CacheSerializableRunnable("putUsingPartitionedRegionXML1") {
      public void run2() throws CacheException
      {
        final String val = "prValue0";
        final Integer key = new Integer(10);
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        assertNotNull(region);
        region.put(key, val);
        assertEquals(val, region.get(key));
      }
    });
    vm1.invoke(new CacheSerializableRunnable("putUsingPartitionedRegionXML2") {
      public void run2() throws CacheException
      {
        final String val = "prValue1";
        final Integer key = new Integer(14);
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        assertNotNull(region);
        region.put(key, val);
        assertEquals(val, region.get(key));
      }
    });
  }

  /**
   * Tests that a region created with a named attributes has the correct
   * attributes.
   * 
   */
  public void testPartitionedRegionInstantiation() throws CacheException
  {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);

    Properties gp = new Properties();
    gp.setProperty(PartitionAttributesFactory.GLOBAL_MAX_MEMORY_PROPERTY, "2");
    gp.setProperty(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY, "17");
    Properties lp = new Properties();
    lp.setProperty(PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY, "4");
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    
    
//    paf.setEntryTimeToLive(
//        new ExpirationAttributes(10, ExpirationAction.DESTROY)).setCacheLoader(
    paf.setLocalProperties(lp).setGlobalProperties(gp);
    attrs.setCacheLoader(new DBLoader());
    attrs.setPartitionAttributes(paf.create());
    cache.createRegion("pRoot", attrs);

    testXml(cache);
  }
}

