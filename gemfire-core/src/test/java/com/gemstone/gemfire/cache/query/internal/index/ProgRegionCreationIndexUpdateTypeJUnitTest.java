/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Apr 21, 2005 *
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author asifs
 *
 * 
 */
@Category(IntegrationTest.class)
public class ProgRegionCreationIndexUpdateTypeJUnitTest{
  
  private Cache cache = null;
  
  @Before
  public void setUp() throws Exception {
    
  }
  
  @After
  public void tearDown() throws Exception {
    if( !cache.isClosed())
      cache.close();
   
  }
  
  @Test
  public void testProgrammaticIndexUpdateType() throws Exception  {
  	Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("log-level", "config");
    DistributedSystem  ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
    //Create a Region with index maintenance type as explicit synchronous
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setIndexMaintenanceSynchronous(true);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion("region1", regionAttributes);
    IndexManager im = IndexUtils.getIndexManager(region,true);
    
    if(!im.isIndexMaintenanceTypeSynchronous())
    	fail("IndexMaintenanceTest::testProgrammaticIndexUpdateType: Index Update Type found to be asynchronous when it was marked explicitly synchronous");
    
    //Create a Region with index mainteneace type as explicit asynchronous    
    attributesFactory = new AttributesFactory();
    attributesFactory.setIndexMaintenanceSynchronous(false);
    regionAttributes = attributesFactory.create();
    region = cache.createRegion("region2", regionAttributes);
    im = IndexUtils.getIndexManager(region,true);
    if(im.isIndexMaintenanceTypeSynchronous())
    	fail("IndexMaintenanceTest::testProgrammaticIndexUpdateType: Index Update Type found to be synchronous when it was marked explicitly asynchronous");
    
    //create a default region & check index maintenecae type .It should be 
    // synchronous    
    attributesFactory = new AttributesFactory();
    regionAttributes = attributesFactory.create();
    region = cache.createRegion("region3", regionAttributes);
    im = IndexUtils.getIndexManager(region,true);
    if(!im.isIndexMaintenanceTypeSynchronous())
    	fail("IndexMaintenanceTest::testProgrammaticIndexUpdateType: Index Update Type found to be asynchronous when it default RegionAttributes should have created synchronous update type");

  }
}
