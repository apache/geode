/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *  * Tests if callbacks are getting invoked correctly 
 *   * for 'create', 'update' and 'destroy' of disk region entries
 *    * with concurrent 'clear' 
 *     * @author Pallavi Sontakke
 *      *
 */
@Category(IntegrationTest.class)
public class DiskRegCbkChkJUnitTest extends DiskRegionTestingBase 
{

  volatile static boolean intoCreateAfterCbk = false;
  volatile static boolean intoUpdateAfterCbk = false;
  volatile static boolean intoDestroyAfterCbk = false;
  
  @Before
  public void setUp() throws Exception
  {  
    super.setUp();
  }

  @After
  public void tearDown() throws Exception
  {
    super.tearDown();
  }
  
  private DiskRegionProperties getDiskRegionProperties(){
    DiskRegionProperties diskProperties = new DiskRegionProperties();
    diskProperties.setRegionName("DiskRegCbkChkJUnitTest_region");
    diskProperties.setMaxOplogSize(20480);
    diskProperties.setDiskDirs(dirs);
    return diskProperties;
  }
    
  @Test
  public void testAfterCallbacks()
  {
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
      getDiskRegionProperties(), Scope.LOCAL);

    //testing create callbacks
    region.getAttributesMutator().setCacheListener(new CacheListenerAdapter(){
      public void afterCreate(EntryEvent event) {
  	intoCreateAfterCbk = true;
      }
    });
    region.getAttributesMutator().setCacheWriter(new CacheWriterAdapter(){
      public void beforeCreate(EntryEvent event) {
      	region.clear();
      }
    });
    region.create("key1", "createValue");
    assertTrue("Create callback not called", intoCreateAfterCbk);
	
    //testing update callbacks
    region.getAttributesMutator().setCacheListener(new CacheListenerAdapter(){
      public void afterUpdate(EntryEvent event) {
    	intoUpdateAfterCbk = true;
      }
    });
    region.getAttributesMutator().setCacheWriter(new CacheWriterAdapter(){
      public void beforeUpdate(EntryEvent event) {
    	region.clear();
      }
    });
    region.create("key2", "createValue");
    region.put("key2", "updateValue");
    assertTrue("Update callback not called", intoUpdateAfterCbk);
	
    //testing destroy callbacks
    region.getAttributesMutator().setCacheListener(new CacheListenerAdapter(){
      public void afterDestroy(EntryEvent event) {
    	intoDestroyAfterCbk = true;
      }
    });
    region.getAttributesMutator().setCacheWriter(new CacheWriterAdapter(){
      public void beforeDestroy(EntryEvent event) {
    	region.clear();
      }
    });
    region.create("key3", "createValue");
    region.destroy("key3");
    assertTrue("Destroy callback not called", intoDestroyAfterCbk);
	
  }  
}
