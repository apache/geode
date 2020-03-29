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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Assert;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.distributed.DistributedSystem;


/**
 * This helper class is used by other test. This has functions to create region.
 *
 *
 */

public class PartitionedRegionTestHelper

{
  static InternalCache cache = null;

  /**
   * This method creates a partitioned region with all the default values. The cache created is a
   * loner, so this is only suitable for single VM tests.
   *
   */

  public static Region createPartionedRegion(String regionname) throws RegionExistsException {
    return createPartionedRegion(regionname, new PartitionAttributesFactory().create());
  }

  /**
   * This method creates a partitioned region with the given PR attributes. The cache created is a
   * loner, so this is only suitable for single VM tests.
   */
  public static Region createPartionedRegion(String regionname, PartitionAttributes prattribs)
      throws RegionExistsException {
    AttributesFactory attribFactory = new AttributesFactory();
    attribFactory.setDataPolicy(DataPolicy.PARTITION);
    attribFactory.setPartitionAttributes(prattribs);
    RegionAttributes regionAttribs = attribFactory.create();

    Region partitionedregion = createCache().createRegion(regionname, regionAttribs);
    return partitionedregion;
  }


  /**
   * This method creates a local region with all the default values. The cache created is a loner,
   * so this is only suitable for single VM tests.
   */

  public static Region createLocalRegion(String regionName) throws RegionExistsException {

    AttributesFactory attr = new AttributesFactory();

    attr.setScope(Scope.LOCAL);
    Region localRegion = createCache().createRegion(regionName, attr.create());

    return localRegion;
  }

  /**
   * This method compares two selectResult Objects by 1. Size 2.
   * SelectResults#CollectionType#ElementType()
   */
  public static String compareResultSets(SelectResults sr1, SelectResults sr2) {


    ObjectType type1, type2;
    String failureString = null;
    type1 = sr1.getCollectionType().getElementType();
    Assert.assertNotNull("PartitionedRegionTestHelper#compareResultSets: Type 1 is NULL " + type1,
        type1);
    type2 = sr2.getCollectionType().getElementType();
    Assert.assertNotNull("PartitionedRegionTestHelper#compareResultSets: Type 2 is NULL " + type2,
        type2);
    if ((type1.getClass().getName()).equals(type2.getClass().getName())) {

      getLogger().info(
          "PartitionedRegionTestHelper#compareResultSets: Both Search Results are of the same Type i.e.--> "
              + type1);

    } else {
      getLogger().error("PartitionedRegionTestHelper#compareTwoQueryResults: Classes are : "
          + type1.getClass().getName() + " " + type2.getClass().getName());
      failureString =
          "PartitionedRegionTestHelper#compareResultSets: FAILED:Search result Type is different in both the cases"
              + type1.getClass().getName() + " " + type2.getClass().getName();

      Assert.fail(
          "PartitionedRegionTestHelper#compareResultSets: FAILED:Search result Type is different in both the cases");
      return failureString;
    }
    if ((sr1.size()) == (sr2.size())) {
      getLogger().info(
          "PartitionedRegionTestHelper#compareResultSets: Both Search Results are non-zero and are of Same Size i.e.  Size= "
              + sr1.size());

    } else {
      getLogger().error(
          "PartitionedRegionTestHelper#compareResultSets: FAILED:Search resultSet size are different in both the cases");
      failureString =
          "PartitionedRegionTestHelper#compareResultSets: FAILED:Search resultSet size are different in both the cases"
              + sr1.size() + " " + sr2.size();
      Assert.fail(
          "PartitionedRegionTestHelper#compareResultSets: FAILED:Search resultSet size are different in both the cases");

    }
    return failureString;
  }

  /**
   * This is a function to create partitioned region with following paramaters:
   * </p>
   * 1) name
   * </p>
   * 2) local max memory
   * </p>
   * 3) redundancy and scope.
   *
   * The cache created is a loner, so this is only suitable for single VM tests.
   */

  public static Region createPartitionedRegion(String regionName, String localMaxMemory,
      int redundancy) {
    Region pr = null;
    PartitionAttributes pa;
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    RegionAttributes ra;
    // setting property
    // setting partition attributes to partitionAttributesFactory
    int lmax;
    try {
      lmax = Integer.parseInt(localMaxMemory);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(
          "localMaxMemory must be an integer (" + localMaxMemory + ")");
    }
    pa = paf.setLocalMaxMemory(lmax).setRedundantCopies(redundancy).create();
    // setting attribute factor
    af.setPartitionAttributes(pa);
    // creating region attributes
    ra = af.create();
    cache = createCache();
    try {
      pr = cache.createRegion(regionName, ra);
    } catch (RegionExistsException rex) {
      pr = cache.getRegion(regionName);
    }
    return pr;
  }

  /**
   * This function is used to create serializable object for the partition region test.
   *
   */
  public static SerializableObject createPRSerializableObject(String name, int id) {
    Object obj = new SerializableObject(name, id);
    return (SerializableObject) obj;

  }

  /**
   * This method creates cache. The cache created is a loner, so this is only suitable for single VM
   * tests.
   *
   */
  public static synchronized InternalCache createCache() {
    if (cache == null) {
      Properties dsp = new Properties();
      dsp.setProperty(MCAST_PORT, "0");
      dsp.setProperty(LOCATORS, "");
      DistributedSystem sys = DistributedSystem.connect(dsp);
      try {
        cache = (InternalCache) CacheFactory.create(sys);
      } catch (CacheExistsException exp) {
        cache = (InternalCache) CacheFactory.getInstance(sys);
      } catch (RegionExistsException rex) {
        cache = (InternalCache) CacheFactory.getInstance(sys);
      }
    }
    return cache;
  }

  /**
   * This method closes the cache.
   */
  public static synchronized void closeCache() {
    if (cache != null) {
      cache.close();
      cache = null;
    }

  }


  /**
   * This method is used to return existing region.
   *
   */
  public static Region getExistingRegion(String PRName) {
    createCache();
    return cache.getRegion(PRName);
  }

  /**
   * Gets the log writer for the The cache created is a loner, so this is only suitable for single
   * VM tests.
   *
   */
  public static LogWriter getLogger() {
    return createCache().getLogger();
  }

  public static RegionAttributes createRegionAttrsForPR(int red, int localMaxMem) {
    return createRegionAttrsForPR(red, localMaxMem,
        PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT);
  }

  public static RegionAttributes createRegionAttrsForPR(int red, int localMaxMem,
      PartitionResolver resolver) {
    return createRegionAttrsForPR(red, localMaxMem,
        PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, null, resolver);
  }

  /**
   * This function creates Region attributes with provided scope,redundancy and localmaxMemory
   */
  public static RegionAttributes createRegionAttrsForPR(int red, int localMaxMem,
      long recoveryDelay) {
    return createRegionAttrsForPR(red, localMaxMem, recoveryDelay, null, null);
  }

  /**
   * This function creates Region attributes with provided scope,redundancy and localmaxMemory
   */
  public static RegionAttributes createRegionAttrsForPR(int red, int localMaxMem,
      long recoveryDelay, EvictionAttributes evictionAttrs, PartitionResolver resolver) {

    AttributesFactory attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PARTITION);
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(red).setLocalMaxMemory(localMaxMem).setRecoveryDelay(recoveryDelay);
    if (resolver != null) {
      paf.setPartitionResolver(resolver);
    }
    PartitionAttributes<?, ?> prAttr = paf.create();
    attr.setPartitionAttributes(prAttr);
    attr.setEvictionAttributes(evictionAttrs);
    return attr.create();
  }

}


/**
 * class for creating serializable object which is used for LocalMaxMemory verification.
 */

class SerializableObject implements Serializable {
  String str;

  int i;

  public SerializableObject(String str, int i) {
    this.str = str;
    this.i = i;
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof SerializableObject)) {
      return false;
    }
    if (obj == null)
      return false;
    if (this.str.equals(((SerializableObject) obj).str) && this.i == ((SerializableObject) obj).i)
      return true;
    return false;
  }
}
