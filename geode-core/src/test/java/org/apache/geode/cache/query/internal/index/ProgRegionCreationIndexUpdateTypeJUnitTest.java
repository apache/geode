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
/*
 * Created on Apr 21, 2005 *
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.fail;

/**
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
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "config");
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
