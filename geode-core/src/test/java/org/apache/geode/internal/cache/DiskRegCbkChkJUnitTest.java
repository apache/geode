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
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Tests if callbacks are getting invoked correctly
 * for 'create', 'update' and 'destroy' of disk region entries
 * with concurrent 'clear'
 */
@Category(IntegrationTest.class)
public class DiskRegCbkChkJUnitTest extends DiskRegionTestingBase {

  private static volatile boolean intoCreateAfterCbk = false;
  private static volatile boolean intoUpdateAfterCbk = false;
  private static volatile boolean intoDestroyAfterCbk = false;
  
  private DiskRegionProperties getDiskRegionProperties(){
    DiskRegionProperties diskProperties = new DiskRegionProperties();
    diskProperties.setRegionName("DiskRegCbkChkJUnitTest_region");
    diskProperties.setMaxOplogSize(20480);
    diskProperties.setDiskDirs(dirs);
    return diskProperties;
  }
    
  @Test
  public void testAfterCallbacks() {
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
